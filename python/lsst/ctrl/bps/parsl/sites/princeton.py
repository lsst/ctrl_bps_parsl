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

from parsl.addresses import address_by_interface
from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher
from psutil import net_if_addrs

from ..configuration import get_bps_config_value
from ..environment import export_environment
from .slurm import Slurm

if TYPE_CHECKING:
    from ..job import ParslJob

__all__ = ("Tiger",)


class Tiger(Slurm):
    """Configuration for running jobs on Princeton's Tiger cluster.

    The following BPS configuration parameters are recognised, overriding the
    defaults:

    - ``nodes`` (`int`): number of nodes for each Slurm job.
    - ``cores_per_node`` (`int`): number of cores per node for each Slurm job.
    - ``walltime`` (`str`): time limit for each Slurm job.
    - ``mem_per_node`` (`int`): memory per node (GB) for each Slurm job.
    - ``max_blocks`` (`int`): maximum number of blocks (Slurm jobs) to use.
    - ``cmd_timeout`` (`int`): timeout (seconds) to wait for a scheduler.
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

    def get_executors(self) -> list[ParslExecutor]:
        """Get a list of executors to be used in processing.

        Each executor should have a unique ``label``.

        The walltime default here is set so we get into the tiger-vshort QoS,
        which will hopefully reduce the wait for us to get a node. Then, we
        have one Slurm job running at a time (singleton) while another saves a
        spot in line (max_blocks=2). We hope that this will allow us to run
        almost continually until the workflow is done.

        We set the cmd_timeout value to 300 seconds to help avoid
        TimeoutExpired errors when commands are slow to return (often due to
        system contention).
        """
        max_blocks = get_bps_config_value(self.site, "max_blocks", int, 2)
        cmd_timeout = get_bps_config_value(self.site, "cmd_timeout", int, 300)
        return [
            self.make_executor(
                "tiger",
                nodes=4,
                cores_per_node=40,
                walltime="05:00:00",  # Ensures we get into qos=tiger-vshort, which cuts off at 5h
                mem_per_node=187,  # Ensures all nodes are queried, reserving 5GB for OS services
                singleton=True,
                provider_options={
                    "init_blocks": 1,
                    "min_blocks": 1,
                    "max_blocks": max_blocks,
                    "parallelism": 1.0,
                    "worker_init": export_environment(),
                    "launcher": SrunLauncher(overrides="-K0 -k"),
                    "cmd_timeout": cmd_timeout,
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
        return "tiger"

    def get_address(self) -> str:
        """Return the IP address of the machine hosting the driver/submission.

        This host machine address should be accessible from the workers and
        should generally be the return value of one of the functions in
        ``parsl.addresses``.

        This is used by the default implementation of ``get_monitor``, but will
        generally be used by ``get_executors`` too.

        This implementation gets the address from the Infiniband network
        interface, because the cluster nodes can't connect to the head node
        through the regular internet.
        """
        net_interfaces = [interface for interface in net_if_addrs().keys() if interface[:2] in ["ib", "op"]]
        if net_interfaces:
            return address_by_interface(net_interfaces[0])
        raise RuntimeError("No Infiniband network interface found.")
