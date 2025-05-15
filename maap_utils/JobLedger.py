from datetime import datetime
from typing import Dict, List
from .Job import Job


class JobLedger:
    """Tracks all job state in a single coordinated structure"""

    PENDING_STATES = ["Accepted", "Running", "Offline", "Submitted"]
    FINAL_STATES = ["Succeeded", "Failed", "Deleted"]

    def __init__(self):
        self.jobs: Dict[str, Job] = {}  # job_id -> Job
        self.status: Dict[str, str] = {}
        self.last_checked: Dict[str, datetime] = {}
        self.attempts: Dict[str, int] = {}

    def add_job(self, job: Job) -> None:
        job_id = job.job_id
        self.jobs[job_id] = job
        self.status[job_id] = "Submitted"
        self.last_checked[job_id] = datetime.now()
        self.attempts[job_id] = 0

    def remove_job(self, job_id: str) -> None:
        """Remove a job completely from tracking"""
        del self.jobs[job_id]
        del self.status[job_id]
        del self.last_checked[job_id]
        del self.attempts[job_id]

    def update_status(self, job_id: str, status: str) -> None:
        """Update status and tracking timestamps"""
        self.status[job_id] = status
        self.last_checked[job_id] = datetime.now()
        self.attempts[job_id] += 1

    def get_pending_jobs(self) -> List[Job]:
        """Get jobs not in final states"""

        # Get all unfinished jobs
        pending = [
            job
            for job_id, job in self.jobs.items()
            if self.status[job_id] not in self.FINAL_STATES
        ]

        # Sort by last checked time, most recently checked last
        pending.sort(key=lambda x: self.last_checked[x.job_id])

        return pending

    def get_finished_jobs(self) -> List[Job]:
        """Get only finished jobs"""
        return [
            job
            for job_id, job in self.jobs.items()
            if self.status[job_id] in self.FINAL_STATES
        ]

    def get_jobs_in_state(self, state: str) -> List[Job]:
        """Get jobs in a specific state"""
        return [
            job
            for job_id, job in self.jobs.items()
            if self.status[job_id] == state
        ]

    def all_final(self) -> bool:
        """Check if all jobs are in terminal states"""
        return all(
            status in self.FINAL_STATES
            for status in self.status.values()
        )

    def all_succeeded(self) -> bool:
        """Check if all jobs succeeded"""
        return all(
            status == "Succeeded"
            for status in self.status.values()
        )

    def get_job_ids(self) -> List[str]:
        """Get all job IDs"""
        return list(self.jobs.keys())

    def get_jobs(self) -> List[Job]:
        """Get all tracked jobs"""
        return list(self.jobs.values())

    def get_status_counts(self) -> Dict[str, int]:
        """Get counts of each job status"""
        counts = {
            state: 0
            for state in (
                self.PENDING_STATES
                + self.FINAL_STATES
                + ["Other"]
            )
        }
        for state in self.status.values():
            if state in counts:
                counts[state] += 1
            else:
                counts["Other"] += 1
        return counts
