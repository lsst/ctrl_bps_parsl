from typing import TYPE_CHECKING, List

from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher

from ..configuration import get_bps_config_value
from ..environment import export_environment
from .slurm import Slurm

if TYPE_CHECKING:
    from ..job import ParslJob

__all__ = ("Sdf",)


class Sdf(Slurm):
    """Slurm-based configuration for running jobs on SLAC's Shared Data
    Facility cluster using the rubin partition.

    The following BPS configuration parameters are recognised, overriding the
    defaults:

    - ``nodes`` (`int`): number of nodes for each Slurm job.
    - ``cores_per_node`` (`int`): number of cores per node for each Slurm job.
    - ``walltime`` (`str`): time limit for each Slurm job.
    - ``mem_per_node`` (`int`): memory per node (GB) for each Slurm job.
    - ``max_blocks`` (`int`): maximum number of blocks (Slurm jobs) to use.
    - ``singleton`` (`bool`): allow only one job to run at a time; by default
      ``True``.
    """

    def get_executors(self) -> List[ParslExecutor]:
        """Get a list of executors to be used in processing

        Each executor should have a unique ``label``.

        We have one Slurm job running at a time (singleton) while
        another saves a spot in line (max_blocks=2). We hope that this
        allow us to run almost continually until the workflow is done.
        """
        max_blocks = get_bps_config_value(self.site, "max_blocks", int, 2)
        return [
            self.make_executor(
                "sdf",
                nodes=1,
                cores_per_node=100,
                walltime="02:00:00",
                mem_per_node=400,
                qos="normal",
                singleton=True,
                scheduler_options="#SBATCH --partition=rubin",
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
        return "sdf"
