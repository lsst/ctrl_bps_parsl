from typing import TYPE_CHECKING, Any, Dict, List, Optional

from parsl.executors import HighThroughputExecutor
from parsl.executors.base import ParslExecutor
from parsl.providers import SlurmProvider

from ..configuration import get_bps_config_value, get_workflow_name
from ..site import SiteConfig

if TYPE_CHECKING:
    from ..job import ParslJob

__all__ = ("Slurm", "TripleSlurm")


Kwargs = Dict[str, Any]


class Slurm(SiteConfig):
    """Configuration for generic Slurm cluster

    This can be used directly as the site configuration for a Slurm cluster by
    setting the BPS config, e.g.:

    .. code-block:: yaml

        computeSite: slurm
        site:
          slurm:
            class: lsst.ctrl.bps.parsl.sites.Slurm
            nodes: 3
            cores_per_node: 20
            walltime: "00:59:00"  # Note: always quote walltime in YAML

    Alternatively, it can be used as a base class for Slurm cluster
    configurations.

    The following BPS configuration parameters are recognised (and required
    unless there is a default mentioned here, or provided by a subclass):

    - ``nodes`` (`int`): number of nodes for each Slurm job.
    - ``cores_per_node`` (`int`): number of cores per node for each Slurm job;
      by default we use all cores on the node.
    - ``walltime`` (`str`): time limit for each Slurm job.
    - ``mem_per_node`` (`int`): memory per node (GB) for each Slurm job; by
      default we use whatever Slurm gives us.
    - ``qos`` (`str`): quality of service to request for each Slurm job; by
      default we use whatever Slurm gives us.
    - ``singleton`` (`bool`): allow only one job to run at a time; by default
      ``False``.
    """

    def make_executor(
        self,
        label: str,
        *,
        nodes: Optional[int] = None,
        cores_per_node: Optional[int] = None,
        walltime: Optional[str] = None,
        mem_per_node: Optional[int] = None,
        mem_per_worker: Optional[float] = None,
        qos: Optional[str] = None,
        constraint: Optional[str] = None,
        singleton: bool = False,
        scheduler_options: Optional[str] = None,
        provider_options: Optional[Kwargs] = None,
        executor_options: Optional[Kwargs] = None,
    ) -> ParslExecutor:
        """Return an executor for running on a Slurm cluster

        Parameters
        ----------
        label : `str`
            Label for executor.
        nodes : `int`, optional
            Default number of nodes for each Slurm job.
        cores_per_node : `int`, optional
            Default number of cores per node for each Slurm job.
        walltime : `str`, optional
            Default time limit for each Slurm job.
        mem_per_node : `float`, optional
            Memory per node (GB) to request for each Slurm job.
        mem_per_worker : `float`, optional
            Minimum memory per worker (GB), limited by the executor.
        qos : `str`, optional
            Quality of service for each Slurm job.
        constraint : `str`, optional
            Node feature(s) to require for each Slurm job.
        singleton : `bool`, optional
            Allow only a single Slurm job to run at a time?
        scheduler_options : `str`, optional
            ``#SBATCH`` directives to prepend to the Slurm submission script.
        provider_options : `dict`, optional
            Additional arguments for `SlurmProvider` constructor.
        executor_options : `dict`, optional
            Additional arguments for `HighThroughputExecutor` constructor.

        Returns
        -------
        executor : `HighThroughputExecutor`
            Executor for Slurm jobs.
        """
        nodes = get_bps_config_value(self.site, "nodes", int, nodes, required=True)
        cores_per_node = get_bps_config_value(self.site, "cores_per_node", int, cores_per_node)
        walltime = get_bps_config_value(self.site, "walltime", str, walltime, required=True)
        mem_per_node = get_bps_config_value(self.site, "mem_per_node", int, mem_per_node)
        qos = get_bps_config_value(self.site, "qos", str, qos)
        singleton = get_bps_config_value(self.site, "singleton", bool, singleton)

        job_name = get_workflow_name(self.config)
        if scheduler_options is None:
            scheduler_options = ""
        scheduler_options += "\n"
        scheduler_options += f"#SBATCH --job-name={job_name}\n"
        if qos:
            scheduler_options += f"#SBATCH --qos={qos}\n"
        if constraint:
            scheduler_options += f"#SBATCH --constraint={constraint}\n"
        if singleton:
            # The following SBATCH directives allow only a single slurm job
            # (parsl block) with our job_name to run at once. This means we can
            # have one job running, and one already in the queue when the first
            # exceeds the walltime limit. More backups could be achieved with a
            # larger value of max_blocks. This only allows one job to be
            # actively running at once, so that needs to be sized appropriately
            # by the user.
            scheduler_options += "#SBATCH --dependency=singleton\n"
        return HighThroughputExecutor(
            label,
            provider=SlurmProvider(
                nodes_per_block=nodes,
                cores_per_node=cores_per_node,
                mem_per_node=mem_per_node,
                walltime=walltime,
                scheduler_options=scheduler_options,
                **(provider_options or {}),
            ),
            mem_per_worker=mem_per_worker,
            address=self.get_address(),
            **(executor_options or {}),
        )

    def get_executors(self) -> List[ParslExecutor]:
        """Get a list of executors to be used in processing

        Each executor should have a unique ``label``.
        """
        return [self.make_executor("slurm")]

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
        return "slurm"


class TripleSlurm(Slurm):
    """Configuration for running jobs on a Slurm cluster with three levels

    The three levels are useful for having workers with different amount of
    available memory (and this is how executors are selected, by default),
    though other uses are possible.

    The following BPS configuration parameters are recognised, overriding the
    defaults:

    - ``nodes`` (`int`): number of nodes for each Slurm job.
    - ``cores_per_node`` (`int`): number of cores per node for each Slurm job;
      by default we use all cores on the node.
    - ``walltime`` (`str`): time limit for each Slurm job; setting this would
      override each of the ``small_walltime``, ``medium_walltime`` and
      ``large_walltime`` values.
    - ``mem_per_node`` (`float`): memory per node for each Slurm job; by
      default we use whatever Slurm gives us.
    - ``qos`` (`str`): quality of service to request for each Slurm job; by
      default we use whatever Slurm gives us.

    - ``small_memory`` (`float`): memory per worker (GB) for each 'small' Slurm
      job.
    - ``medium_memory`` (`float`): memory per worker (GB) for each 'medium'
      Slurm job.
    - ``large_memory`` (`float`): memory per worker (GB) for each 'large' Slurm
      job.
    - ``small_walltime`` (`str`): time limit for each 'small' Slurm job.
    - ``medium_walltime`` (`str`): time limit for each 'medium' Slurm job.
    - ``large_walltime`` (`str`): time limit for each 'large' Slurm job.

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.small_memory = get_bps_config_value(self.site, "small_memory", float, 2.0)
        self.medium_memory = get_bps_config_value(self.site, "medium_memory", float, 4.0)
        self.large_memory = get_bps_config_value(self.site, "large_memory", float, 8.0)
        self.small_walltime = get_bps_config_value(self.site, "small_walltime", str, "10:00:00")
        self.medium_walltime = get_bps_config_value(self.site, "medium_walltime", str, "10:00:00")
        self.large_walltime = get_bps_config_value(self.site, "large_walltime", str, "40:00:00")

    def get_executors(
        self,
        small_options: Optional[Kwargs] = None,
        medium_options: Optional[Kwargs] = None,
        large_options: Optional[Kwargs] = None,
        **common_options,
    ) -> List[ParslExecutor]:
        """Get a list of executors to be used in processing

        We create three executors, with different walltime and memory per
        worker.

        Parameters
        ----------
        small_options : kwargs
            Options for ``make_executor`` for small executor.
        medium_options : kwargs
            Options for ``make_executor`` for medium executor.
        large_options : kwargs
            Options for ``make_executor`` for large executor.
        **common_options
            Common options for ``make_executor`` for each of the executors.
        """
        if small_options is None:
            small_options = {}
        if medium_options is None:
            medium_options = {}
        if large_options is None:
            large_options = {}

        small_options["walltime"] = small_options.get("walltime", self.small_walltime)
        medium_options["walltime"] = medium_options.get("walltime", self.medium_walltime)
        large_options["walltime"] = large_options.get("walltime", self.large_walltime)

        small_options["mem_per_worker"] = small_options.get("mem_per_worker", self.small_memory)
        medium_options["mem_per_worker"] = medium_options.get("mem_per_worker", self.medium_memory)
        large_options["mem_per_worker"] = large_options.get("mem_per_worker", self.large_memory)

        return [
            self.make_executor("small", **small_options, **(common_options or {})),
            self.make_executor("medium", **medium_options, **(common_options or {})),
            self.make_executor("large", **large_options, **(common_options or {})),
        ]

    def select_executor(self, job: "ParslJob") -> str:
        """Get the ``label`` of the executor to use to execute a job

        This implementation only looks at the requested memory.

        Parameters
        ----------
        job : `ParslJob`
            Job to be executed.

        Returns
        -------
        label : `str`
            Label of executor to use to execute ``job``.
        """
        memory = job.generic.request_memory / 1024  # GB
        if memory <= self.small_memory:
            return "small"
        if memory <= self.medium_memory:
            return "medium"
        return "large"
