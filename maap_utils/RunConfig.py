"""RunConfig.py
This module defines the RunConfig class, which is used to store and manage
configuration parameters for running NMBIM jobs on MAAP.
"""

from dataclasses import dataclass


@dataclass
class RunConfig:
    """Container for all runtime configuration parameters"""
    username: str
    tag: str
    algo_id: str
    algo_version: str
    model_config: str
    hse: str
    k_allom: str
    boundary: str = None
    date_range: str = None
    job_limit: int = None
    check_interval: int = 120
    redo_tag: str = None
    force_redo: bool = False
