from maap import MAAP
from typing import Optional

maap = MAAP(maap_host="api.maap-project.org")


class Job:
    """Represents an individual processing job"""

    def __init__(self, job_id: str):
        self.job_id = job_id
        self._status: Optional[str] = None
        self._result_url: Optional[str] = None

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
