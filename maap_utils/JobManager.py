from typing import List, Dict
from maap.maap import MAAP
import datetime
import time
from tqdm import tqdm
from pathlib import Path
import logging
import random

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
        redo_enabled: bool = True,
    ):
        self.config = config
        self.job_kwargs_list = job_kwargs_list
        self.check_interval = check_interval
        self.redo_enabled = redo_enabled
        self.original_job_kwargs = job_kwargs_list  # for potential resubmit
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
            except Exception as e:
                logging.error(f"Error submitting job: {e}")
                continue

            if job_batch_counter == job_batch_size:
                time.sleep(job_submit_delay)
                job_batch_counter = 0

        # Store output_dir and write job IDs
        self.output_dir = output_dir
        job_ids_file = self.output_dir / "job_ids.txt"
        with open(job_ids_file, "w") as f:
            for job_id in self.ledger.get_job_ids():
                f.write(f"{job_id}\n")
        logging.info(f"Submitted job IDs written to {job_ids_file}")

        # Wait 10 seconds after submitting jobs, with tqdm bar
        tqdm.write("Waiting for jobs to start")
        for i in tqdm(range(10), desc=""):
            time.sleep(1)

    def _update_states(self, batch_size: int = 50, delay: int = 10):
        """Internal method to update job states in batches"""
        # Select up to batch_size jobs that were least recently checked
        pending = self.ledger.get_pending_jobs()

        # Sort by oldest last_checked timestamp
        pending.sort(key=lambda job: self.last_checked[job.job_id])

        # Randomly sample up to batch_size jobs from the least recently updated
        selected = random.sample(
            pending[:batch_size], k=min(batch_size, len(pending))
        )

        for job in selected:
            new_state = job.get_status()
            self.ledger.update_status(job.job_id, new_state)

    def monitor(self) -> bool:
        """Monitor jobs and return True if all completed successfully"""
        """Monitor job progress with live updates and handle interrupts"""
        # scheduling parameters
        INNER_BATCH = 10
        INNER_DELAY = 5
        LONG_PAUSE_AFTER = 10
        LONG_DELAY = 10

        cycle_count = 0
        try:
            tqdm.write("Job Status")
            with tqdm(total=len(self.jobs), desc="") as pbar:
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
            print("\nJob monitoring interrupted")
            self._handle_interrupt()

        # Return whether all jobs completed successfully
        all_succeeded = self.ledger.all_succeeded()
        if not all_succeeded and self.redo_enabled and self.prompt_for_redo():
            self.resubmit_jobs()
            self.monitor()

        return all_succeeded

    def resubmit_jobs(self) -> None:
        """Resubmit unsuccessful jobs to retry within this session"""

        # Get IDs of jobs that finished unsuccessfully
        bad_jobs = [
            job
            for job in self.ledger.get_finished_jobs()
            if job not in self.ledger.get_jobs_in_state("Succeeded")
        ]

        # Create new Job objects for failed jobs using original kwargs
        new_jobs = [Job(job.kwargs) for job in bad_jobs]

        # Clear ledger tracking for failed jobs
        # Note: no cancellation necessary since they are already finished
        for job in bad_jobs:
            self.ledger.remove_job(job.job_id)

        # Submit new jobs
        logging.info(f"Resubmitting {len(new_jobs)} failed jobs")
        for job in new_jobs:
            try:
                job.submit()
                # Append resubmitted job ID to file
                with open(self.output_dir / "job_ids.txt", "a") as f:
                    f.write(f"{job.job_id}\n")
                self.ledger.add_job(job)
            except Exception as e:
                logging.error(f"Error resubmitting job: {e}")
                continue

    def _handle_interrupt(self) -> None:
        """Handle keyboard interrupt and cleanup"""
        print(
            "Press Ctrl+C again to confirm cancellation,"
            "or wait to continue..."
        )
        try:
            time.sleep(3)
            print("Resuming monitoring...")
            self.monitor()
        except KeyboardInterrupt:
            if self.redo_enabled and self.prompt_for_redo():
                print("Resubmitting failed jobs...")
                self.resubmit_jobs()
                self.monitor()
            else:
                self.exit_gracefully()

    def prompt_for_redo(self) -> bool:
        """Prompt user for interactive resubmission of failed jobs"""
        redo_answer = input(
            "\nResubmit failed jobs? [y/N] "
            "This will resubmit all jobs that do not have 'Succeeded' status "
            "and continue monitoring.\n"
        ).strip()
        if redo_answer == "y":
            redo = True
        elif redo_answer == "N":
            redo = False
        else:
            print("Invalid input. Please enter 'y' or 'N'.")
            redo = self.prompt_for_redo()
        return redo

    def exit_gracefully(self) -> None:
        """Cancel pending jobs, print a report, and exit"""
        print("\nExiting...")
        # Cancel all pending jobs
        pending_jobs = self.ledger.get_pending_jobs()

        for job in pending_jobs:
            try:
                job.cancel()
            except Exception as e:
                logging.error(f"Error cancelling job: {e}")
        print("All jobs cancelled.")
        self.report()
        exit(0)

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

        if counts["Failed"] > 0:
            failed_ids = [job.job_id for job in self.ledger.get_failed_jobs()]
            logging.info(f"\nFailed job IDs:\n  {', '.join(failed_ids)}")

        return counts
