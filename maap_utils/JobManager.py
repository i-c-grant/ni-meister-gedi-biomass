from typing import List, Dict
from maap.maap import MAAP
import datetime
import time
from tqdm import tqdm
from pathlib import Path
import logging
import json
import sys

from .RunConfig import RunConfig
from .Job import Job
from .JobLedger import JobLedger

maap = MAAP(maap_host="api.maap-project.org")


class JobManager:
    """Manages tracking and monitoring of submitted jobs"""

    def __init__(
        self,
        config: RunConfig,
        job_kwargs_list: List[Dict],
        check_interval: int = 120,
    ):
        self.config = config
        self.job_kwargs_list = job_kwargs_list
        self.check_interval = check_interval
        self.ledger = JobLedger()
        self.start_time = datetime.datetime.now()

    def submit(self, output_dir: Path) -> None:
        """Submit jobs in batches with error handling and delays"""
        job_batch_counter = 0
        job_batch_size = 50
        job_submit_delay = 2

        total_jobs = (
            min(len(self.job_kwargs_list), self.config.job_limit)
            if self.config.job_limit
            else len(self.job_kwargs_list)
        )

        tqdm.write("Submitting jobs")
        try:
            for job_kwargs in tqdm(
                self.job_kwargs_list[: self.config.job_limit],
                total=total_jobs,
                desc="",
            ):
                try:
                    job = Job(job_kwargs)
                    job.submit()
                    self.ledger.add_job(job)
                    job_batch_counter += 1
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logging.error(f"Error submitting job: {e}")
                    continue

                if job_batch_counter == job_batch_size:
                    time.sleep(job_submit_delay)
                    job_batch_counter = 0
        except KeyboardInterrupt:
            logging.info("Submission interrupted by user")
            self.exit_gracefully()
            sys.exit(1)

        # Store output_dir and write job IDs
        self.output_dir = output_dir
        job_ids_file = self.output_dir / "job_ids.txt"
        with open(job_ids_file, "w") as f:
            for job_id in self.ledger.get_job_ids():
                f.write(f"{job_id}\n")
        logging.info(f"Submitted job IDs written to {job_ids_file}")

        # Wait 10 seconds after submitting jobs, with tqdm bar
        tqdm.write("Waiting for jobs to start")
        try:
            for i in tqdm(range(10), desc=""):
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Submission interrupted by user")
            self.exit_gracefully()
            sys.exit(1)

    def _update_states(self, batch_size: int = 50, delay: int = 10):
        """Internal method to update job states in batches"""
        # Select up to batch_size jobs that were least recently checked
        pending = self.ledger.get_pending_jobs()
        for job in pending[:batch_size]:
            new_state = job.get_status()
            self.ledger.update_status(job.job_id, new_state)

    def monitor(self) -> str:
        """
        Monitor job progress with live updates and handle interrupts

        Returns:
            'interrupted' if user interrupt and jobs still pending,
            'succeeded' if all jobs succeeded,
            'partial' if all jobs finished but some failed.
        """
        # scheduling parameters
        INNER_BATCH = 10
        INNER_DELAY = 5
        LONG_PAUSE_AFTER = 10
        LONG_DELAY = 10

        cycle_count = 0
        try:
            tqdm.write("Job Status")
            with tqdm(total=len(self.ledger.get_jobs()), desc="") as pbar:
                while not self.ledger.all_final():
                    # Update job states once per batch
                    self._update_states(batch_size=INNER_BATCH, delay=0)
                    counts = self.ledger.get_status_counts()
                    # Update progress bar based on completed jobs
                    completed = len(self.ledger.get_finished_jobs())
                    pbar.n = completed
                    pbar.refresh()
                    # Round elapsed time to nearest second
                    elapsed = datetime.datetime.now() - self.start_time
                    elapsed_rounded = datetime.timedelta(
                        seconds=round(elapsed.total_seconds())
                    )
                    pbar.set_postfix(
                        {**counts, "Elapsed": str(elapsed_rounded)}
                    )

                    cycle_count += 1
                    if cycle_count < LONG_PAUSE_AFTER:
                        time.sleep(INNER_DELAY)
                    else:
                        time.sleep(LONG_DELAY)
                        cycle_count = 0

        except KeyboardInterrupt:
            print("\nJob monitoring interrupted by user")
            if self.ledger.all_final():
                if self.ledger.all_succeeded():
                    return "succeeded"
                else:
                    return "partial"
            return "interrupted"

        if self.ledger.all_succeeded():
            return "succeeded"
        return "partial"

    def resubmit_unsuccessful_jobs(self) -> None:
        """Resubmit unsuccessful jobs to retry within this session"""

        logging.info("Resubmitting unsuccessful jobs...")

        unsuccessful_jobs = [
            job
            for job in self.ledger.get_finished_jobs()
            if job not in self.ledger.get_jobs_in_state("Succeeded")
        ]

        if unsuccessful_jobs:
            self.resubmit_jobs(unsuccessful_jobs)

    def resubmit_jobs(self, to_resubmit: List[Job]) -> None:
        # Create new Job objects for failed jobs using original kwargs
        new_jobs = [Job(job.kwargs) for job in to_resubmit]
        logging.info(f"{len(new_jobs)} jobs queued for resubmission.")

        # Clear ledger tracking for failed jobs
        cancelled = 0
        for job in to_resubmit:
            # Cancel jobs that are not in final states
            if job in self.ledger.get_pending_jobs():
                job.cancel()
                cancelled += 1
            self.ledger.remove_job(job.job_id)

        if cancelled > 0:
            logging.info(f"Cancelling {cancelled} jobs in non-final states.")

        # Submit new jobs
        resubmitted = 0
        for job in new_jobs:
            try:
                job.submit()
                resubmitted += 1
                # Append resubmitted job ID to file
                with open(self.output_dir / "job_ids.txt", "a") as f:
                    f.write(f"Resubmitted: {job.job_id}\n")
                self.ledger.add_job(job)
            except Exception as e:
                logging.error(f"Error resubmitting job: {e}")
                continue
        logging.info(f"Resubmitted {resubmitted} jobs.")

    def exit_gracefully(self) -> None:
        """Cancel pending jobs, print a report, and exit"""
        logging.info("\nExiting gracefully...")
        # Cancel all pending jobs
        pending_jobs = self.ledger.get_pending_jobs()

        for job in pending_jobs:
            try:
                job.cancel()
            except Exception as e:
                logging.error(f"Error cancelling job: {e}")
        logging.info("All pending jobs cancelled.")
        self.report()
        return self.ledger.all_succeeded()

    def report(self) -> Dict[str, int]:
        """Logging.Info final job statistics report"""
        duration = datetime.datetime.now() - self.start_time
        counts = self.ledger.get_status_counts()
        total_jobs = len(self.ledger.get_jobs())
        success_pct = (
            (counts["Succeeded"] / total_jobs * 100) if total_jobs else 0
        )

        logging.info(f"\n{' Job Summary '.center(40, '=')}")
        logging.info(f"Total runtime:    {duration}")
        logging.info(f"Jobs submitted:  {total_jobs}")
        logging.info(f"Success rate:     {success_pct:.1f}%")
        logging.info("\nStatus counts:")
        for state, count in counts.items():
            logging.info(f"  {state}: {count}")

        successful_jobs = set(self.ledger.get_jobs_in_state("Succeeded"))
        unsuccessful_jobs = set(self.ledger.get_jobs()) - successful_jobs

        # Write unsuccessful jobs details to JSON in output directory
        unsuccessful_jobs_details = [
            {"job_id": job.job_id,
             "state": self.ledger.get_status(job.job_id),
             "kwargs": job.kwargs}
            for job in unsuccessful_jobs
        ]
        json_file = self.output_dir / "unsuccessful_jobs.json"
        with open(json_file, "w") as f:
            json.dump(unsuccessful_jobs_details, f, indent=2)
        logging.info(f"Wrote unsuccessful jobs details to {json_file}")

        return counts
