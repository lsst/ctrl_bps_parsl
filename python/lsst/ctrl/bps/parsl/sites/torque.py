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

import logging
from typing import TYPE_CHECKING, Any

from parsl.executors import HighThroughputExecutor
from parsl.executors.base import ParslExecutor


from ..configuration import get_bps_config_value, get_workflow_name
from ..site import SiteConfig
from parsl.launchers import MpiRunLauncher
from parsl.providers import TorqueProvider
from parsl.channels import LocalChannel

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..job import ParslJob

__all__ = ("Torque",)


Kwargs = dict[str, Any]


class Torque(SiteConfig):
    """Configuration for generic Torque cluster.

    This can be used directly as the site configuration for a Torque cluster by
    setting the BPS config, e.g.:

    .. code-block:: yaml

        computeSite: torque
        site:
          torque:
            class: lsst.ctrl.bps.parsl.sites.Torque
            nodes: 4
            tasks_per_node: 20
            walltime: "00:59:00"  # Note: always quote walltime in YAML

    Alternatively, it can be used as a base class for Torque cluster
    configurations.

    The following BPS configuration parameters are recognised (and required
    unless there is a default mentioned here, or provided by a subclass):

    - ``queue`` (`int`): Queue for the Torque job.
    - ``nodes`` (`int`): number of nodes for each Torque job.
    - ``tasks_per_node`` (`int`): number of cores per node for each Torque job;
      by default we use all cores on the node.
    - ``walltime`` (`str`): time limit for each Torque job.
    - ``scheduler_options`` (`str`): text to prepend to the Torque submission
      script (each line usually starting with ``#PBS``).
    """

    def make_executor(
        self,
        label: str,
        *,
        queue: str | None = None,
        nodes: int | None = None,
        tasks_per_node: int | None = None,
        walltime: str | None = None,
        mem_per_worker: float | None = None,
        scheduler_options: str | None = None,
        worker_init: str | None = None,
        provider_options: Kwargs | None = None,
        executor_options: Kwargs | None = None,
    ) -> ParslExecutor:
        """Return an executor for running on a Torque cluster.

        Parameters
        ----------
        label : `str`
            Label for executor.
        queue : `str`, optional
            Queue for the Torque job.
        nodes : `int`, optional
            Default number of nodes for each Torque job.
        tasks_per_node : `int`, optional
            Default number of cores per node for each Torque job.
        walltime : `str`, optional
            Default time limit for each Torque job.
        mem_per_worker : `float`, optional
            Minimum memory per worker (GB), limited by the executor.
        worker_init : `str`, optional
            Environment initiation command
        scheduler_options : `str`, optional
            ``#SBATCH`` directives to prepend to the Torque submission script.
        provider_options : `dict`, optional
            Additional arguments for `TorqueProvider` constructor.
        executor_options : `dict`, optional
            Additional arguments for `HighThroughputExecutor` constructor.

        Returns
        -------
        executor : `HighThroughputExecutor`
            Executor for Torque jobs.
        """
        nodes = get_bps_config_value(
            self.site,
            "nodes",
            int,
            nodes,
            required=True,
        )
        queue = get_bps_config_value(
            self.site,
            "queue",
            str,
            queue,
        )
        walltime = get_bps_config_value(
            self.site,
            "walltime",
            str,
            walltime,
            required=True,
        )
        tasks_per_node = get_bps_config_value(
            self.site,
            "tasks_per_node",
            int,
            tasks_per_node,
        )
        worker_init = get_bps_config_value(
            self.site,
            "worker_init",
            str,
            walltime,
        )
        scheduler_options = get_bps_config_value(
            self.site,
            "scheduler_options",
            str,
            scheduler_options,
        )

        job_name = get_workflow_name(self.config)
        if scheduler_options is None:
            scheduler_options = ""
        else:
            scheduler_options += "\n"
        scheduler_options += f"#PBS -N {job_name}\n"
        if queue:
            scheduler_options += f"#PBS -q {queue}\n"
        if not isinstance(nodes, int) or nodes < 1:
            nodes = 1
        if not isinstance(tasks_per_node, int) or tasks_per_node < 1:
            tasks_per_node = 1
        if not isinstance(walltime, str) or len(walltime) < 1:
            walltime = "48:00:00"
        if worker_init is None:
            worker_init = ""

        launcher = MpiRunLauncherI(
            overrides=f"--map-by core:{tasks_per_node}"
        )

        return HighThroughputExecutor(
            label,
            provider=TorqueProviderI(
                nodes_per_block=nodes,
                tasks_per_node=tasks_per_node,
                queue=queue,
                walltime=walltime,
                scheduler_options=scheduler_options,
                worker_init=worker_init,
                launcher=launcher,
                **(provider_options or {}),
            ),
            # Caps the number of workers per pool.
            max_workers=1,
            mem_per_worker=mem_per_worker,
            address=self.get_address(),
            **(executor_options or {}),
        )

    def get_executors(self) -> list[ParslExecutor]:
        """Get a list of executors to be used in processing.

        Each executor should have a unique ``label``.
        """
        return [self.make_executor("torque")]

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
        return "torque"


class TorqueProviderI(TorqueProvider):
    """Torque Execution Provider

    This provider uses qsub to submit, qstat for status, and qdel to cancel
    jobs. The qsub script to be used is created from a template file in this
    same module.

    Parameters
    ----------
    channel : Channel
        Channel for accessing this provider. Possible channels include
        :class:`~parsl.channels.LocalChannel` (the default),
        :class:`~parsl.channels.SSHChannel`, or
        :class:`~parsl.channels.SSHInteractiveLoginChannel`.
    account : str
        Account the job will be charged against.
    queue : str
        Torque queue to request blocks from.
    nodes_per_block : int
        Nodes to provision per block.
    tasks_per_node : int
        Number of tasks per node
    init_blocks : int
        Number of blocks to provision at the start of the run. Default is 1.
    min_blocks : int
        Minimum number of blocks to maintain. Default is 0.
    max_blocks : int
        Maximum number of blocks to maintain.
    parallelism : float
        Ratio of provisioned task slots to active tasks. A parallelism value of
        1 represents aggressive
        scaling where as many resources as possible are used; parallelism close
        to 0 represents
        the opposite situation in which as few resources as possible
        (i.e., min_blocks) are used.
    walltime : str
        Walltime requested per block in HH:MM:SS.
    scheduler_options : str
        String to prepend to the #PBS blocks in the submit script to the
        scheduler.
        WARNING: scheduler_options should only be given #PBS strings, and
        should not have trailing newlines.
    worker_init : str
        Command to be run before starting a worker, such as
        'module load Anaconda; source activate env'.
    launcher : Launcher
        Launcher for this provider. Possible launchers include
        :class:`~parsl.launchers.AprunLauncher` (the default), or
        :class:`~parsl.launchers.SingleNodeLauncher`,

    """

    def __init__(
        self,
        channel=LocalChannel(),
        account=None,
        queue=None,
        scheduler_options="",
        worker_init="",
        nodes_per_block=1,
        tasks_per_node=1,
        init_blocks=1,
        min_blocks=0,
        max_blocks=1,
        parallelism=1,
        launcher=MpiRunLauncher(),
        walltime="00:20:00",
        cmd_timeout=120,
    ):
        super().__init__(
            channel=channel,
            account=account,
            queue=queue,
            scheduler_options=scheduler_options,
            worker_init=worker_init,
            nodes_per_block=nodes_per_block,
            init_blocks=init_blocks,
            min_blocks=min_blocks,
            max_blocks=max_blocks,
            parallelism=parallelism,
            launcher=launcher,
            walltime=walltime,
            cmd_timeout=cmd_timeout,
        )
        self.tasks_per_node = tasks_per_node

    def submit(self, command, tasks_per_node, job_name="parsl.torque"):
        """Submit the command onto an Local Resource Manager job.

        This function returns an ID that corresponds to the task that was just
        submitted.

        Parameters
        ----------
        command (string):
            Commandline invocation to be made on the remote side.
        job_name (String):
            Name for job, must be unique

        Returns
        -------
            None: At capacity, cannot provision more
            job_id (string): Identifier for the job

        """
        return super().submit(
            command=command,
            tasks_per_node=self.tasks_per_node,
            job_name=job_name,
        )


class MpiRunLauncherI(MpiRunLauncher):
    """ Worker launcher that wraps the user's command with the framework to
    launch multiple command invocations via mpirun.

    This wrapper sets the bash env variable CORES to the number of cores on the
    machine.

    This launcher makes the following assumptions:
    - mpirun is installed and can be located in $PATH
    - The provider makes available the $PBS_NODEFILE environment variable
    """
    def __init__(
        self,
        debug: bool = True,
        bash_location: str = '/bin/bash',
        overrides: str = ''
    ):
        super().__init__(
            debug=debug, bash_location=bash_location, overrides=overrides,
        )

    def __call__(self, command: str, tasks_per_node: int, nodes_per_block: int) -> str:
        """
        Args:
        - command (string): The command string to be launched

        """
        workercount = nodes_per_block * tasks_per_node
        debug_num = int(self.debug)

        x = '''set -e
export CORES=$(getconf _NPROCESSORS_ONLN)
[[ "{debug}" == "1" ]] && echo "Found cores : $CORES"
WORKERCOUNT={workercount}

cat << MPIRUN_EOF > cmd_$JOBNAME.sh
{command}
MPIRUN_EOF
chmod u+x cmd_$JOBNAME.sh

mpirun -np $WORKERCOUNT {overrides} {bash_location} cmd_$JOBNAME.sh

[[ "{debug}" == "1" ]] && echo "All workers done"
'''.format(command=command,
           workercount=workercount,
           overrides=self.overrides,
           bash_location=self.bash_location,
           debug=debug_num)
        return x
