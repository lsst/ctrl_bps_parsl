from typing import Any, Dict, List, Optional

from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher

from .slurm import TripleSlurm

Kwargs = Dict[str, Any]

__all__ = ("CoriKnl",)


class CoriKnl(TripleSlurm):
    """Configuration for running jobs on the NERSC Cori-KNL cluster

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

    - ``small_memory`` (`float`): memory per worker for each 'small' Slurm job.
    - ``medium_memory`` (`float`): memory per worker for each 'small' Slurm
      job.
    - ``large_memory`` (`float`): memory per worker for each 'large' Slurm job.
    - ``small_walltime`` (`str`): time limit for each 'small' Slurm job.
    - ``medium_walltime`` (`str`): time limit for each 'small' Slurm job.
    - ``large_walltime`` (`str`): time limit for each 'large' Slurm job.

    """

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
        scheduler_options = "\n".join(
            (
                "#SBATCH --module=cvmfs",
                "#SBATCH --licenses=cvmfs",
                "#SBATCH --time-min=2:00:00",
            )
        )
        provider_options = dict(
            exclusive=True,
            init_blocks=0,
            min_blocks=0,
            max_blocks=1,
            parallelism=0,
            launcher=SrunLauncher(overrides="-K0 -k --slurmd-debug=verbose"),
            cmd_timeout=300,
        )
        executor_options = dict(worker_debug=False, heartbeat_period=60, heartbeat_threshold=180)

        return super().get_executors(
            nodes=1,
            constraint="knl",
            scheduler_options=scheduler_options,
            provider_options=provider_options,
            executor_options=executor_options,
            **common_options,
        )
