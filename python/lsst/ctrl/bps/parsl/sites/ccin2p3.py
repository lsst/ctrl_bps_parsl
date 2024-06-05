from typing import TYPE_CHECKING, Any

import parsl.config
from parsl.executors import HighThroughputExecutor
from parsl.executors.base import ParslExecutor
from parsl.providers import SlurmProvider

from ..configuration import get_bps_config_value
from ..site import SiteConfig

if TYPE_CHECKING:
    from .job import ParslJob

__all__ = ("Ccin2p3",)

Kwargs = dict[str, Any]


class Ccin2p3(SiteConfig):
    """Configuration for running Parsl jobs in CC-IN2P3 Slurm batch farm.

    This class provides 4 job slot sizes with different requirements, in
    particular in terms of memory. Those slot sizes are named "small",
    "medium", "large" and "xlarge".

    Sensible default values for those requirements are provided for each
    kind of job but you can specify different values either in the
    the BPS submission file or in a site configuration file that you
    include in your BPS submission file.

    This is an example of how to modify the specifications for those job
    slot sizes in the BPS submission file:

    .. code-block:: yaml

        wmsServiceClass: lsst.ctrl.bps.parsl.ParslService
        computeSite: ccin2p3

        site:
          ccin2p3:
            class: lsst.ctrl.bps.parsl.sites.Ccin2p3
            walltime: "72:00:00"
            qos: "normal"
            small:
                memory: 4
                partition: "flash"
            medium:
                memory: 10
                partition: "lsst,htc"
            large:
                memory: 50
            xlarge:
                memory: 150
                partition: "lsst"

    At the level of 'site:' entry in the BPS submission file, the following
    configuration parameters are accepted, which apply to all slot sizes:

    - `partition` (`str`): name of the one or more configured partitions. If
       more than one, separate them with comma (',').
       (Default: "lsst,htc")
    - `qos` (`str`): quality of service to use (Default: "normal")
    - `walltime` (`str`): walltime to require for the job (Default: "72:00:00")

    For each kind of job slot (i.e. "small", "medium", etc.) you can specify
    the parameters above as well as:

    - `max_blocks` (`int`): maximum number of Slurm jobs that your workflow can
       simultaneously use.
    - ``memory`` (`int`): required amount of memory in Gigabytes.

    as shown in the example above.

    If you don't need to modify those values and use the default configuration
    for all the job slot sizes use:

    .. code-block:: yaml

        wmsServiceClass: lsst.ctrl.bps.parsl.ParslService
        computeSite: ccin2p3

        site:
        ccin2p3:
            class: lsst.ctrl.bps.parsl.sites.Ccin2p3

    Parameters
    ----------
    *args : optional
        Arguments to initialize the super-class.
    **kwargs : optional
        Keyword arguments to initialize the super-class.

    Returns
    -------
    Ccin2p3 : `SiteConfig`
        Concrete instance of a `SiteConfig` specific for the CC-IN2P3 Slurm
        farm.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._account = get_bps_config_value(self.site, "account", str, "lsst")
        default_partition = get_bps_config_value(self.site, "partition", str, "lsst,htc")
        default_qos = get_bps_config_value(self.site, "qos", str, "normal")
        default_walltime = get_bps_config_value(self.site, "walltime", str, "72:00:00")

        self._slot_size = {
            "small": {
                "max_blocks": get_bps_config_value(self.site, "small.max_blocks", int, 3_000),
                "memory": get_bps_config_value(self.site, "small.memory", int, 4),
                "partition": get_bps_config_value(self.site, "small.partition", str, default_partition),
                "qos": get_bps_config_value(self.site, "small.qos", str, default_qos),
                "walltime": get_bps_config_value(self.site, "small.walltime", str, default_walltime),
            },
            "medium": {
                "max_blocks": get_bps_config_value(self.site, "medium.max_blocks", int, 1_000),
                "memory": get_bps_config_value(self.site, "medium.memory", int, 10),
                "partition": get_bps_config_value(self.site, "medium.partition", str, "lsst"),
                "qos": get_bps_config_value(self.site, "medium.qos", str, default_qos),
                "walltime": get_bps_config_value(self.site, "medium.walltime", str, default_walltime),
            },
            "large": {
                "max_blocks": get_bps_config_value(self.site, "large.max_blocks", int, 100),
                "memory": get_bps_config_value(self.site, "large.memory", int, 50),
                "partition": get_bps_config_value(self.site, "large.partition", str, "lsst"),
                "qos": get_bps_config_value(self.site, "large.qos", str, default_qos),
                "walltime": get_bps_config_value(self.site, "large.walltime", str, default_walltime),
            },
            "xlarge": {
                "max_blocks": get_bps_config_value(self.site, "xlarge.max_blocks", int, 10),
                "memory": get_bps_config_value(self.site, "xlarge.memory", int, 150),
                "partition": get_bps_config_value(self.site, "xlarge.partition", str, "lsst"),
                "qos": get_bps_config_value(self.site, "xlarge.qos", str, default_qos),
                "walltime": get_bps_config_value(self.site, "xlarge.walltime", str, default_walltime),
            },
        }

    def get_executors(self) -> list[ParslExecutor]:
        """Get a list of executors to be used for processing a workflow.
        Each executor must have a unique ``label``.
        """
        executors: list[ParslExecutor] = []
        for label, slot in self._slot_size.items():
            qos = slot["qos"]
            executor = HighThroughputExecutor(
                label,
                provider=SlurmProvider(
                    # Slurm partition to request blocks from.
                    partition=slot["partition"],
                    # Slurm account to which to charge resources used by the
                    # job.
                    account=self._account,
                    # Nodes to provision per block (1 block = 1 CPU core).
                    nodes_per_block=1,
                    # Number of CPU cores to provision per node.
                    cores_per_node=1,
                    # Memory per node (GB) for each Slurm job.
                    mem_per_node=slot["memory"],
                    # Initial number of blocks.
                    init_blocks=0,
                    # Minimum number of blocks to maintain.
                    min_blocks=0,
                    # Maximum number of blocks to maintain.
                    max_blocks=slot["max_blocks"],
                    # Time limit for each Slurm job.
                    walltime=slot["walltime"],
                    # '#SBATCH' directives to prepend to the Slurm submission
                    # script.
                    scheduler_options=f"#SBATCH --qos={qos} --licenses=sps",
                    # Set the number of file descriptors and processes to
                    # the maximum allowed.
                    worker_init="ulimit -n hard && ulimit -u hard",
                    # Requests nodes which are not shared with other running
                    # jobs.
                    exclusive=False,
                    # Should files be moved by Parsl?
                    move_files=False,
                ),
                # Address to connect to the main Parsl process.
                address=self.get_address(),
                # GB of memory required per worker. If specified the node
                # manager will check the available memory at startup and limit
                # the number of workers such that the there’s sufficient memory
                # for each worker.
                mem_per_worker=None,
                # Caps the number of workers launched per node.
                max_workers=1,
                # Timeout period (in milliseconds) to be used by the
                # executor components.
                poll_period=1_000,
                # Retry submitting to Slurm in case of submission error.
                block_error_handler=False,
            )
            executors.append(executor)

        return executors

    def select_executor(self, job: "ParslJob") -> str:
        """Get the ``label`` of the executor to use to execute ``job``.

        Parameters
        ----------
        job : `ParslJob`
            Job to be executed.

        Returns
        -------
        label : `str`
            Label of executor to use to execute ``job``.
        """
        # We choose the executor to use based only on the memory required
        # by the job.
        memory = job.generic.request_memory / 1024  # Convert to GB
        for label in ("small", "medium", "large"):
            if memory <= self._slot_size[label]["memory"]:
                return label

        return "xlarge"

    def get_parsl_config(self) -> parsl.config.Config:
        """Get Parsl configuration for using CC-IN2P3 Slurm farm as a
        Parsl execution site.

        Returns
        -------
        config : `parsl.config.Config`
            The configuration to be used to initialize Parsl for this site.
        """
        executors = self.get_executors()
        monitor = self.get_monitor()
        retries = get_bps_config_value(self.site, "retries", int, 1)
        run_dir = get_bps_config_value(self.site, "run_dir", str, "parsl_runinfo")
        # Strategy for scaling blocks according to workflow needs.
        # Use a strategy that allows for scaling in and out Parsl
        # workers.
        strategy = get_bps_config_value(self.site, "strategy", str, "htex_auto_scale")
        return parsl.config.Config(
            executors=executors,
            monitoring=monitor,
            retries=retries,
            checkpoint_mode="task_exit",
            run_dir=run_dir,
            strategy=strategy,
        )
