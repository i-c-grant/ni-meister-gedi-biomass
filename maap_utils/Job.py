import logging
from maap.maap import MAAP
from typing import Optional, Dict, Any, Callable

maap = MAAP(maap_host="api.maap-project.org")


class Job:
    """Handles API interactions for a single job on MAAP"""

    def __init__(self, kwargs: Dict):
        self._kwargs = kwargs
        self._job_id: Optional[str] = None
        self._status: Optional[str] = None
        self._result_url: Optional[str] = None

    @property
    def kwargs(self) -> Dict:
        """Get the job arguments (read-only)"""
        return self._kwargs

    @property
    def job_id(self) -> Optional[str]:
        """Get the job ID (read-only)"""
        return self._job_id

    def __repr__(self) -> str:
        return f"Job(job_id={self._job_id}, status={self._status})"

    def _safe_request(self, fn: Callable[..., Any], *args, **kwargs) -> Any:
        """Call a MAAP function, catch and log exceptions."""
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            logging.error(f"MAAP API error in {fn.__name__}: {e}")
            return None

    def submit(self) -> None:
        """Submit this job to MAAP"""
        job_obj = self._safe_request(maap.submitJob, **self.kwargs)
        if job_obj and hasattr(job_obj, "id"):
            self._job_id = job_obj.id
        else:
            raise RuntimeError("Failed to submit job—no job ID returned")

    def get_status(self) -> Optional[str]:
        """Get current job status from MAAP API"""
        status = self._safe_request(maap.getJobStatus, self.job_id)
        if status is not None:
            self._status = status
        return self._status

    def get_result(self) -> Optional[str]:
        """Get job result URL from MAAP API"""
        result = self._safe_request(maap.getJobResult, self.job_id)
        if result:
            self._result_url = result[0]
        return self._result_url

    def cancel(self) -> None:
        """Cancel this job"""
        self._safe_request(maap.cancelJob, self.job_id)

    def __hash__(self) -> int:
        """Hash function for the job object"""
        return hash(self.job_id) if self.job_id else 0
