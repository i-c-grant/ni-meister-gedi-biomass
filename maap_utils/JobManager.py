from typing import List, Dict
from maap import MAAP
import datetime
import time
from tqdm import tqdm
from pathlib import Path
import logging

from .RunConfig import RunConfig
from .Job import Job

maap = MAAP(maap_host="api.maap-project.org")


# Job monitoring utilities
class JobManager:
    """Manages tracking and monitoring of submitted jobs"""
    FINAL_STATES = ["Succeeded", "Failed", "Deleted"]

    def __init__(self,
                 config: RunConfig,
                 job_kwargs_list: List[Dict],
                 check_interval: int = 120):
        self.config = config
        self.job_kwargs_list = job_kwargs_list
        self.check_interval = check_interval
        self.jobs = []
        self.states = {}
        self.progress = 0
        self.start_time = datetime.datetime.now()

    def submit(self, output_dir: Path) -> None:
        """Submit jobs in batches with error handling and delays"""
        jobs = []
        job_batch_counter = 0
        job_batch_size = 50
        job_submit_delay = 2

        total_jobs = (
            min(len(self.job_kwargs_list), self.config.job_limit)
            if self.config.job_limit
            else len(self.job_kwargs_list))

        for job_kwargs in tqdm(
            self.job_kwargs_list[:self.config.job_limit],
            total=total_jobs,
            desc="Submitting jobs"
        ):
            try:
                job = Job(job_kwargs)
                job.submit()
                jobs.append(job)
                job_batch_counter += 1
            except Exception as e:
                logging.error(f"Error submitting job: {e}")
                continue

            if job_batch_counter == job_batch_size:
                time.sleep(job_submit_delay)
                job_batch_counter = 0

        self.jobs = jobs
        self.states = {job.job_id: "" for job in self.jobs}

        # Write job IDs to file
        job_ids_file = output_dir / "job_ids.txt"
        with open(job_ids_file, "w") as f:
            for job in self.jobs:
                f.write(f"{job.job_id}\n")
        logging.info(f"Submitted job IDs written to {job_ids_file}")

    def _update_states(self,
                       batch_size: int = 50,
                       delay: int = 10) -> int:
        """Internal method to update job states in batches"""
        batch_count = 0
        updated = 0

        for job in self.jobs:
            job_id = job.job_id
            if self.states[job_id] not in self.FINAL_STATES:
                new_state = job.get_status()
                if new_state != self.states[job_id]:
                    self.states[job_id] = new_state
                    if new_state in self.FINAL_STATES:
                        updated += 1
                    batch_count += 1

                if batch_count >= batch_size:
                    time.sleep(delay)
                    batch_count = 0

        return updated

    def _status_counts(self) -> Dict[str, int]:
        """Get current counts of each job state"""
        counts = {state: 0 for state in self.FINAL_STATES + ["Other"]}
        for state in self.states.values():
            if state in counts:
                counts[state] += 1
            else:
                counts["Other"] += 1
        return counts

    def monitor(self) -> None:
        """Monitor job progress with live updates and handle interrupts"""
        try:
            with tqdm(total=len(self.jobs), desc="Jobs Completed") as pbar:
                while self.progress < len(self.jobs):
                    updated = self._update_states()
                    self.progress += updated

                    pbar.update(updated)
                    pbar.set_postfix({
                        **self._status_counts(),
                        "Elapsed": str(datetime.datetime.now() -
                                       self.start_time)
                    })

                    time.sleep(self.check_interval)

        except KeyboardInterrupt:
            print("\nJob monitoring interrupted")
            self._handle_interrupt()

    def _handle_interrupt(self) -> None:
        """Handle keyboard interrupt and cleanup"""
        print("Press Ctrl+C again to confirm cancellation,"
              "or wait to continue...")
        try:
            time.sleep(3)
            print("Resuming monitoring...")
            self.monitor()
        except KeyboardInterrupt:
            print("\nCancelling pending jobs...")
            pending = [job for job in self.jobs 
                      if self.states[job.job_id] not in self.FINAL_STATES]
            for job in pending:
                job.cancel()
            print(f"Cancelled {len(pending)} jobs")
