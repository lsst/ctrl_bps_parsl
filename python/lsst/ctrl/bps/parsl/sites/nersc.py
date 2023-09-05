# This file is part of ctrl_bps_parsl.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org) and the LSST DESC (https://www.lsstdesc.org/).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from typing import Any

from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher

from .slurm import TripleSlurm

Kwargs = dict[str, Any]

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
        small_options: Kwargs | None = None,
        medium_options: Kwargs | None = None,
        large_options: Kwargs | None = None,
        **common_options,
    ) -> list[ParslExecutor]:
        """Get a list of executors to be used in processing.

        We create three executors, with different walltime and memory per
        worker.

        Parameters
        ----------
        small_options : `dict`
            Options for ``make_executor`` for small executor.
        medium_options : `dict`
            Options for ``make_executor`` for medium executor.
        large_options : `dict`
            Options for ``make_executor`` for large executor.
        **common_options : Any
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
