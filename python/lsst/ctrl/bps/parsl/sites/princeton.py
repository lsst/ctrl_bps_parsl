from typing import TYPE_CHECKING, List

from parsl.addresses import address_by_interface
from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher

from ..configuration import get_bps_config_value
from ..environment import export_environment
from .slurm import Slurm

if TYPE_CHECKING:
    from ..job import ParslJob

__all__ = ("Tiger",)


class Tiger(Slurm):
    """Configuration for running jobs on Princeton's Tiger cluster

    The following BPS configuration parameters are recognised, overriding the
    defaults:

    - ``nodes`` (`int`): number of nodes for each Slurm job.
    - ``cores_per_node`` (`int`): number of cores per node for each Slurm job.
    - ``walltime`` (`str`): time limit for each Slurm job.
    - ``mem_per_node`` (`int`): memory per node (GB) for each Slurm job.
    - ``max_blocks`` (`int`): maximum number of blocks (Slurm jobs) to use.
    - ``singleton`` (`bool`): allow only one job to run at a time; by default
      ``True``.

    When running on the Tiger cluster, you should operate on the
    ``/scratch/gpfs`` filesystem, rather than ``/projects`` or ``/tigress``,
    as the latter are much slower on the cluster nodes than they are on the
    head nodes. Your BPS config should contain::

        includeConfigs:
          - ${CTRL_BPS_PARSL_DIR}/etc/execution_butler_copy_files.yaml

    This will cause the necessary files to be transferred from your repo
    (presumably on ``/projects`` or ``/tigress``) to the execution butler in
    your submission directory (presumably on ``/scratch/gpfs``). Failure to do
    so will result in about a 6x slowdown, and probably degrading performance
    for other users. The results will be copied back to the original repo when
    everything has completed.
    """

    def get_executors(self) -> List[ParslExecutor]:
        """Get a list of executors to be used in processing

        Each executor should have a unique ``label``.

        The walltime default here is set so we get into the tiger-vshort QoS,
        which will hopefully reduce the wait for us to get a node. Then, we
        have one Slurm job running at a time (singleton) while another saves a
        spot in line (max_blocks=2). We hope that this allow us to run almost
        continually until the workflow is done.
        """
        max_blocks = get_bps_config_value(self.site, "max_blocks", int, 2)
        return [
            self.make_executor(
                "tiger",
                nodes=4,
                cores_per_node=40,
                walltime="05:00:00",  # Ensures we get into qos=tiger-vshort, which cuts off at 5h
                mem_per_node=187,  # Ensures all nodes are queried, reserving 5GB for OS services
                singleton=True,
                provider_options=dict(
                    init_blocks=1,
                    min_blocks=1,
                    max_blocks=max_blocks,
                    parallelism=1.0,
                    worker_init=export_environment(),
                    launcher=SrunLauncher(overrides="-K0 -k --slurmd-debug=verbose"),
                ),
            )
        ]

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
        return "tiger"

    def get_address(self) -> str:
        """Return the IP address of the machine hosting the driver/submission

        This address should be accessible from the workers. This should
        generally by the return value of one of the functions in
        ``parsl.addresses``.

        This is used by the default implementation of ``get_monitor``, but will
        generally be used by ``get_executors`` too.

        This implementation gets the address from the Infiniband network
        interface, because the cluster nodes can't connect to the head node
        through the regular internet.
        """
        return address_by_interface("ib0")
