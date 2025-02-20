import copy
import platform
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
    """Configuration for executing Parsl jobs in CC-IN2P3 Slurm batch farm.

    This class provides four job slot sizes each with its specific
    requirements, in particular in terms of memory. Those slot sizes are named
    "small", "medium", "large" and "xlarge".

    Sensible default values for those requirements are provided for each
    job slot but you can overwrite those defaults either in the
    the BPS submission file or in a site configuration file that you
    include in your BPS submission file.

    If you don't need to modify the default requirements for the job slot
    sizes, use the site specification below in your BPS configuration
    file:

    .. code-block:: yaml

        wmsServiceClass: lsst.ctrl.bps.parsl.ParslService
        computeSite: ccin2p3

        site:
          ccin2p3:
            class: lsst.ctrl.bps.parsl.sites.ccin2p3.Ccin2p3

    If you do need to modify those defaults, you can overwrite them for
    all job slots or for specific each job slots. Requirements specified
    for a job slot take priority over those specified for all job slots
    at the level of entry '.site.ccin2p3:'.

    This is an example of how to overwrite selected requirements in your BPS
    submission file:

    .. code-block:: yaml

        wmsServiceClass: lsst.ctrl.bps.parsl.ParslService
        computeSite: ccin2p3

        site:
          ccin2p3:
            class: lsst.ctrl.bps.parsl.sites.ccin2p3.Ccin2p3
            walltime: "72:00:00"
            scheduler_options:
              - "--licenses=sps"
              - "--qos=normal"
            small:
              memory: 6
              partition: "flash"
            medium:
              memory: 10
              partition: "lsst,htc"
            large:
              memory: 80
            xlarge:
              memory: 180
              partition: "lsst"
              scheduler_options:
                - "--constraint=el7"
                - "--licenses=my_product"
                - "--reservation=my_reservation"

    At the level of entry 'site.ccin2p3:' in the BPS submission file, the
    following configuration parameters are accepted, which apply to all slot
    sizes:

    - `partition` (`str`): name of the one or more configured partitions. If
       more than one, separate them with comma (',').
       (Default: "lsst,htc")
    - `walltime` (`str`): walltime to require for the job (Default: "72:00:00")
    - `scheduler_options` (`list` [`str`] ): scheduler options to send to Slurm
       for scheduling purposes.
       (Default: "--licenses=sps")

    In addition, as shown in the previous example, for each job slot (i.e.
    "small", "medium", etc.) you can specify the requirements above as well as
    the following:

    - `max_blocks` (`int`): maximum number of Slurm jobs that your workflow can
       simultaneously use.
    - `memory` (`int`): required amount of memory for each job, in Gigabytes.
       (Defaults: 4 for "small", 10 for "medium", 50 fo "large" and
       150 for "xlarge").

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

    DEFAULT_ACCOUNT: str = "lsst"
    DEFAULT_WALLTIME: str = "72:00:00"
    DEFAULT_SCHEDULER_OPTIONS: list[str] = [
        "--licenses=sps",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._account = get_bps_config_value(self.site, ".account", str, self.DEFAULT_ACCOUNT)
        self._scheduler_options = get_bps_config_value(
            self.site, ".scheduler_options", list, self.DEFAULT_SCHEDULER_OPTIONS
        )
        self._slot_size = {
            "small": {
                "memory": get_bps_config_value(self.site, ".small.memory", int, 4),
                "walltime": self._get_walltime_for_slot("small"),
                "partition": self._get_partition_for_slot("small"),
                "max_blocks": get_bps_config_value(self.site, ".small.max_blocks", int, 3_000),
                "scheduler_options": get_bps_config_value(self.site, ".small.scheduler_options", list, []),
            },
            "medium": {
                "memory": get_bps_config_value(self.site, ".medium.memory", int, 10),
                "walltime": self._get_walltime_for_slot("medium"),
                "partition": self._get_partition_for_slot("medium"),
                "max_blocks": get_bps_config_value(self.site, ".medium.max_blocks", int, 1_000),
                "scheduler_options": get_bps_config_value(self.site, ".medium.scheduler_options", list, []),
            },
            "large": {
                "memory": get_bps_config_value(self.site, ".large.memory", int, 50),
                "walltime": self._get_walltime_for_slot("large"),
                "partition": self._get_partition_for_slot("large"),
                "max_blocks": get_bps_config_value(self.site, ".large.max_blocks", int, 100),
                "scheduler_options": get_bps_config_value(self.site, ".large.scheduler_options", list, []),
            },
            "xlarge": {
                "memory": get_bps_config_value(self.site, ".xlarge.memory", int, 150),
                "walltime": self._get_walltime_for_slot("xlarge"),
                "partition": self._get_partition_for_slot("xlarge"),
                "max_blocks": get_bps_config_value(self.site, ".xlarge.max_blocks", int, 10),
                "scheduler_options": get_bps_config_value(self.site, ".xlarge.scheduler_options", list, []),
            },
        }

    def _get_partition_for_slot(self, slot: str) -> str:
        """Return the Slurm partition Parsl must use to submit jobs for the
        job slot `slot`. Values of `slot` can be "small", "medium", "large"
        or "xlarge".
        """
        # The target Slurm partition must be selected according to the type of
        # the job slot but also according to the CPU architecture of the
        # compute node.
        #
        # Parsl requires that the CPU architecture of its orchestrator to
        # be identical to the architecture of its executors. Therefore,
        # we need to ensure that Slurm schedules our Parsl executors on
        # compute nodes with the same architecture as the host where this
        # orchestrator runs.

        # Default target Slurm partitions per CPU architecture
        default_partition = {
            "aarch64": {
                "small": "htc_arm",
                "medium": "htc_arm",
                "large": "htc_arm",
                "xlarge": "htc_arm",
            },
            "x86_64": {
                "small": "lsst,htc",
                "medium": "lsst",
                "large": "lsst",
                "xlarge": "lsst",
            },
        }
        architecture = platform.machine()
        if architecture not in default_partition:
            raise ValueError(f"architecture {architecture} is not supported")

        # If a partition was specified in the workflow description file
        # specifically for this job slot, use that partition. For instance:
        #
        # site:
        #   ccin2p3:
        #     class: lsst.ctrl.bps.parsl.sites.ccin2p3.Ccin2p3
        #     small:
        #       partition: htc
        slot_partition = get_bps_config_value(self.site, f".{slot}.partition", str, "")
        if slot_partition != "":
            return slot_partition

        # If a partition was specified in the workflow description file at
        # the site level, use that partition. For instance:
        #
        # site:
        #   ccin2p3:
        #     class: lsst.ctrl.bps.parsl.sites.ccin2p3.Ccin2p3
        #     partition: htc
        #
        # Otherwise, use the default for this slot on this architecture.
        return get_bps_config_value(self.site, ".partition", str, default_partition[architecture][slot])

    def _get_walltime_for_slot(self, slot: str) -> str:
        """Return the value for walltime Parsl must use to submit jobs for the
        job slot `slot`. Values of `slot` can be "small", "medium", "large"
        or "xlarge".
        """
        # If a specific walltime value was specified for this job slot in the
        # configuration use that value. For instance:
        #
        # site:
        #   ccin2p3:
        #     class: lsst.ctrl.bps.parsl.sites.ccin2p3.Ccin2p3
        #     small:
        #       walltime: "3:00:00"
        slot_walltime = get_bps_config_value(self.site, f".{slot}.walltime", str, "")
        if slot_walltime != "":
            return slot_walltime

        # If a walltime value was specified for the site use that value.
        # Otherwise, use the default walltime. For instance:
        #
        # site:
        #   ccin2p3:
        #     class: lsst.ctrl.bps.parsl.sites.ccin2p3.Ccin2p3
        #     walltime: "3:00:00"
        return get_bps_config_value(self.site, ".walltime", str, self.DEFAULT_WALLTIME)

    def get_executors(self) -> list[ParslExecutor]:
        """Get a list of Parsl executors that can be used for processing a
        workflow.

        Each executor must have a unique ``label``.
        """
        executors: list[ParslExecutor] = []
        for label, slot in self._slot_size.items():
            # Compute the scheduler options for this job slot. Options
            # specified at the slot level in the configuration file
            # overwrite those specified at the site level.
            scheduler_options = copy.deepcopy(self._scheduler_options)
            if slot_scheduler_options := slot.get("scheduler_options", []):
                scheduler_options = copy.deepcopy(slot_scheduler_options)

            options = f"#SBATCH {' '.join(opt for opt in scheduler_options)}" if scheduler_options else ""

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
                    scheduler_options=options,
                    # Set the number of file descriptors and processes to
                    # the maximum allowed.
                    worker_init="ulimit -n hard && ulimit -u hard",
                    # Requests nodes which are not shared with other running
                    # jobs.
                    exclusive=False,
                ),
                # Address to connect to the main Parsl process.
                address=self.get_address(),
                # GB of memory required per worker. If specified the node
                # manager will check the available memory at startup and limit
                # the number of workers such that the there’s sufficient memory
                # for each worker.
                mem_per_worker=None,
                # Caps the number of workers launched per node.
                max_workers_per_node=1,
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

        # Number of retries in case of job failure.
        retries = get_bps_config_value(self.site, ".retries", int, 0)

        # Path to run directory.
        run_dir = get_bps_config_value(self.site, ".run_dir", str, "parsl_runinfo")

        # Strategy for scaling blocks according to workflow needs.
        # Use a strategy that allows for scaling up and down Parsl workers.
        strategy = get_bps_config_value(self.site, ".strategy", str, "htex_auto_scale")

        return parsl.config.Config(
            executors=executors,
            monitoring=monitor,
            retries=retries,
            checkpoint_mode="task_exit",
            run_dir=run_dir,
            strategy=strategy,
        )
