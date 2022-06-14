from typing import TYPE_CHECKING, List

from parsl.executors import ThreadPoolExecutor
from parsl.executors.base import ParslExecutor

from ..configuration import get_bps_config_value
from ..site import SiteConfig

if TYPE_CHECKING:
    from ..job import ParslJob

__all__ = ("Local",)


class Local(SiteConfig):
    """Configuration for running jobs on a local ThreadPool

    (The python GIL does not impact the threads, because the jobs are running in
    bash, outside the GIL.)

    The number of cores to use is specified in the site configuration, under
    ``site.<computeSite>.cores`` (`int`).
    """

    def get_executors(self) -> List[ParslExecutor]:
        """Get a list of executors to be used in processing

        Each executor should have a unique ``label``.
        """
        cores = get_bps_config_value(self.site, "cores", int, required=True)
        return [ThreadPoolExecutor("local", max_threads=cores)]

    def select_executor(self, job: "ParslJob") -> str:
        """Get the ``label`` of the executor to use to execute a job

        Parameters
        ----------
        job : `ParslJob`
            Job to be executed.

        Returns
        -------
        label : `str`
            Label of executor to use to execute ``job``.
        """
        return "local"
