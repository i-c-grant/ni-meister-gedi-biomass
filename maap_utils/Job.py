from maap.maap import MAAP
from typing import Optional, Dict

maap = MAAP(maap_host="api.maap-project.org")


class Job:
    """Handles API interactions for a single job on MAAP"""

    def __init__(self, kwargs: Dict):
        self._kwargs = kwargs
        self._job_id: Optional[str] = None

    @property
    def kwargs(self) -> Dict:
        """Get the job arguments (read-only)"""
        return self._kwargs

    @property
    def job_id(self) -> Optional[str]:
        """Get the job ID (read-only)"""
        return self._job_id

    def __repr__(self) -> str:
        return f"Job(job_id={self._job_id})"
        self._status: Optional[str] = None
        self._result_url: Optional[str] = None

    def submit(self) -> None:
        """Submit this job to MAAP"""
        job = maap.submitJob(**self.kwargs)
        self._job_id = job.id

    def get_status(self) -> str:
        """Get current job status from MAAP API"""
        self._status = maap.getJobStatus(self.job_id)
        return self._status

    def get_result(self) -> str:
        """Get job result URL from MAAP API"""
        self._result_url = maap.getJobResult(self.job_id)[0]
        return self._result_url

    def cancel(self) -> None:
        """Cancel this job"""
        maap.cancelJob(self.job_id)

    def __hash__(self) -> int:
        """Hash function for the job object"""
        return hash(self.job_id) if self.job_id else 0
