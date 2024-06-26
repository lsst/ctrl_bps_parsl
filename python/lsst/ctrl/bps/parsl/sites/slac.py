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

from typing import TYPE_CHECKING

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

    def get_executors(self) -> list[ParslExecutor]:
        """Get a list of executors to be used in processing.

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
                provider_options={
                    "init_blocks": 1,
                    "min_blocks": 1,
                    "max_blocks": max_blocks,
                    "parallelism": 1.0,
                    "worker_init": export_environment(),
                    "launcher": SrunLauncher(overrides="-K0 -k --slurmd-debug=verbose"),
                },
            )
        ]

    def select_executor(self, job: "ParslJob") -> str:
        """Get the ``label`` of the executor to use to execute a job.

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
