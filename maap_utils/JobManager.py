from typing import List, Dict
from maap.maap import MAAP
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
    PROGRESS_STATES = ["Accepted", "Running", "Offline"]

    def __init__(self,
                 config: RunConfig,
                 job_kwargs_list: List[Dict],
                 check_interval: int = 120):
        self.config = config
        self.job_kwargs_list = job_kwargs_list
        self.check_interval = check_interval
        self.jobs = []
        self.job_states: Dict[str, str] = {}
        self.last_checked: Dict[str, datetime.datetime] = {}
        self.attempts: Dict[str, int] = {}  # num. of job status checks
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
        self.job_states = {job.job_id: "Submitted" for job in self.jobs}
        self.last_checked = {job.job_id: datetime.datetime.min for job in self.jobs}
        self.attempts = {job.job_id: 0 for job in self.jobs}

        # Write job IDs to file
        job_ids_file = output_dir / "job_ids.txt"
        with open(job_ids_file, "w") as f:
            for job in self.jobs:
                f.write(f"{job.job_id}\n")
        logging.info(f"Submitted job IDs written to {job_ids_file}")

        # Wait 10 seconds after submitting jobs, with tqdm bar
        for i in tqdm(range(10), desc="Waiting for jobs to start"):
            time.sleep(1)

    def _update_states(self,
                       batch_size: int = 50,
                       delay: int = 10) -> int:
        """Internal method to update job states in batches"""
        batch_count = 0
        updated = 0

        for job in self.jobs:
            job_id = job.job_id
            if self.job_states[job_id] not in self.FINAL_STATES:
                new_state = job.get_status()
                if new_state != self.job_states[job_id]:
                    self.job_states[job_id] = new_state
                    self.attempts[job_id] += 1
                    self.last_checked[job_id] = datetime.datetime.now()

                    if new_state in self.FINAL_STATES:
                        updated += 1
                    batch_count += 1

                if batch_count >= batch_size:
                    time.sleep(delay)
                    batch_count = 0

        return updated

    def _status_counts(self) -> Dict[str, int]:
        """Get current counts of each job state"""
        counts = {state: 0
                  for state in (self.FINAL_STATES +
                                self.PROGRESS_STATES +
                                ["Other"])}
        for state in self.job_states.values():
            if state in counts:
                counts[state] += 1
            else:
                counts["Other"] += 1
        return counts

    def monitor(self) -> None:
        """Monitor job progress with live updates and handle interrupts"""
        # scheduling parameters
        INNER_BATCH = 10
        INNER_DELAY = 2
        LONG_PAUSE_AFTER = 10
        LONG_DELAY = 30

        cycle_count = 0
        try:
            with tqdm(total=len(self.jobs), desc="Job Status") as pbar:
                while not all(state in self.FINAL_STATES for state in self.job_states.values()):
                    updated = self._update_states(batch_size=INNER_BATCH,
                                                  delay=0)
                    self.progress += updated

                    pbar.update(updated)
                    pbar.set_postfix({
                        **self._status_counts(),
                        "Elapsed": str(datetime.datetime.now() -
                                       self.start_time)
                    })

                    cycle_count += 1
                    if cycle_count < LONG_PAUSE_AFTER:
                        time.sleep(INNER_DELAY)
                    else:
                        time.sleep(LONG_DELAY)
                        cycle_count = 0

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
                       if self.job_states[job.job_id] not in self.FINAL_STATES]
            for job in pending:
                job.cancel()
            print(f"Cancelled {len(pending)} jobs")

    def report(self) -> None:
        """Print final job statistics report"""
        duration = datetime.datetime.now() - self.start_time
        counts = self._status_counts()
        total_jobs = len(self.jobs)
        success_pct = (
            (counts["Succeeded"] / total_jobs * 100)
            if total_jobs else 0
        )

        print(f"\n{' Job Summary '.center(40, '=')}")
        print(f"Total runtime:    {duration}")
        print(f"Jobs submitted:  {total_jobs}")
        print(f"Success rate:     {success_pct:.1f}%")
        print("\nStatus counts:")
        for state, count in counts.items():
            print(f"  {state}: {count}")

        if counts["Failed"] > 0:
            failed_ids = [job.job_id for job in self.jobs
                          if self.job_states[job.job_id] == "Failed"]
            print(f"\nFailed job IDs:\n  {', '.join(failed_ids)}")
