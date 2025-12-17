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

import socket
from contextlib import closing
from typing import TYPE_CHECKING, Any

from parsl.executors import WorkQueueExecutor
from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import LocalProvider, SlurmProvider

try:
    from parsl.providers.base import ExecutionProvider
except ImportError:
    from parsl.providers.provider_base import ExecutionProvider  # type: ignore

from ..configuration import get_bps_config_value, get_workflow_name
from ..site import SiteConfig

if TYPE_CHECKING:
    from ..job import ParslJob

__all__ = ("LocalSrunWorkQueue", "SlurmWorkQueue", "WorkQueue")


def get_free_port():
    """Return a free port on the local host.
    See https://stackoverflow.com/questions/1365265/
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = s.getsockname()[1]
        return port


class WorkQueue(SiteConfig):
    """Base class configuraton for `WorkQueueExecutor`.

    Subclasses must provide implementations for ``.get_executors``
    and ``.select_executor``.  In ``.get_executors``, the site-specific
    `ExecutionProvider` must be defined.

    Parameters
    ----------
    *args : `~typing.Any`
        Parameters forwarded to base class constructor.
    **kwargs : `~typing.Any`
        Keyword arguments passed to base class constructor, augmented by
        the ``resource_list`` argument.

    Notes
    -----
    The following BPS configuration parameters are recognized, overriding the
    defaults:

    - ``port`` (`int`): The port used by work_queue. Default: ``None``.
      If ``None``, then find a free port.
    - ``worker_options (`str`): Extra options to pass to work_queue workers.
      A typical option specifies the memory available per worker, e.g.,
      ``"--memory=90000"``, which sets the available memory to 90 GB.
      Default: ``""``
    - ``wq_max_retries`` (`int`): The number of retries that work_queue
      will make in case of task failures.  Set to ``None`` to have work_queue
      retry forever; set to ``1`` to have retries managed by Parsl.
      Default: ``1``
    """

    def __init__(self, *args, **kwargs):
        # Have BPS-defined resource requests for each job passed to work_queue.
        kwargs["resource_list"] = ["memory", "cores", "disk", "running_time_min", "priority"]
        super().__init__(*args, **kwargs)

    def make_executor(
        self,
        label: str,
        provider: ExecutionProvider,
        *,
        port: int = None,
        worker_options: str = "",
        wq_max_retries: int = 1,
    ) -> ParslExecutor:
        """Return a `WorkQueueExecutor`.  The ``provider`` contains the
        site-specific configuration.

        Parameters
        ----------
        label : `str`
            Label for executor.
        provider : `ExecutionProvider`
            Parsl execution provider, e.g., `SlurmProvider`.
        port : `int`, optional
            Port used by work_queue.  Default: ``None``
        worker_options : `str`, optional
            Extra options to pass to work_queue workers, e.g.,
            ``"--memory=90000"``. Default: `""`.
        wq_max_retries : `int`, optional
            Number of retries for work_queue to attempt per job.  Set to
            ``None`` to have it try indefinitely; set to ``1`` to have Parsl
            control the number of retries.  Default: ``1``.
        """
        port = get_bps_config_value(self.site, "port", int, port)
        if port is None:
            port = get_free_port()
        worker_options = get_bps_config_value(self.site, "worker_options", str, worker_options)
        max_retries = get_bps_config_value(self.site, "wq_max_retries", int, wq_max_retries)
        return WorkQueueExecutor(
            label=label,
            provider=provider,
            port=port,
            worker_options=worker_options,
            max_retries=max_retries,
            shared_fs=True,
            autolabel=False,
        )

    def get_executors(self) -> list[ParslExecutor]:
        return [self.make_executor("work_queue", self.get_provider())]

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
        return "work_queue"


class LocalSrunWorkQueue(WorkQueue):
    """Configuration for a `WorkQueueExecutor` that uses a `LocalProvider`
    to manage resources.

    This can be used directly as the site configuration within a
    multi-node allocation when Slurm is available.  For running on a
    single node, e.g., a laptop, a `SingleNodeLauncher` is used, and
    Slurm need not be available.

    The following BPS configuration parameters are recognized, overriding the
    defaults:

    - ``port`` (`int`): The port used by work_queue. Default: ``None``.
      If ``None``, then find a free port.
    - ``worker_options (`str`): Extra options to pass to work_queue workers.
      A typical option specifies the memory available per worker, e.g.,
      ``"--memory=90000"``, which sets the available memory to 90 GB.
      Default: ``""``
    - ``wq_max_retries`` (`int`): The number of retries that work_queue
      will make in case of task failures.  Set to ``None`` to have work_queue
      retry forever; set to ``1`` to have retries managed by Parsl. Default:
      ``1``.
    - ``nodes_per_block`` (`int`): The number of allocated nodes.
      Default: ``1``.
    """

    def get_provider(self) -> ExecutionProvider:
        """Return a LocalProvider."""
        nodes = get_bps_config_value(self.site, "nodes_per_block", int, 1)
        provider_options = {
            "nodes_per_block": nodes,
            "init_blocks": 0,
            "min_blocks": 0,
            "max_blocks": 1,
            "parallelism": 0,
            "cmd_timeout": 300,
        }
        if nodes > 1:
            provider_options["launcher"] = SrunLauncher(overrides="-K0 -k --cpu-bind=none")
        return LocalProvider(**provider_options)


class SlurmWorkQueue(WorkQueue):
    """Configuration for a `WorkQueueExecutor` that uses a `SlurmProvider`
    to manage resources.

    The following BPS configuration parameters are recognized, overriding the
    defaults:

    - ``port`` (`int`): The port used by work_queue. Default: ``None``.
      If ``None``, then find a free port.
    - ``worker_options (`str`): Extra options to pass to work_queue workers.
      A typical option specifies the memory available per worker, e.g.,
      ``"--memory=90000"``, which sets the available memory to 90 GB.
      Default: ``""``
    - ``wq_max_retries`` (`int`): The number of retries that work_queue
      will make in case of task failures.  Set to ``None`` to have work_queue
      retry forever; set to ``1`` to have retries managed by Parsl.
      Default: ``1``
    - ``nodes_per_block`` (`int`): The number of allocated nodes.
      Default: ``1``
    """

    def get_provider(
        self,
        nodes: int | None = 1,
        cores_per_node: int | None = None,
        walltime: str | None = None,
        mem_per_node: int | None = None,
        qos: str | None = None,
        constraint: str | None = None,
        singleton: bool = False,
        exclusive: bool = False,
        scheduler_options: str | None = None,
        provider_options: dict[str, Any] | None = None,
    ) -> ExecutionProvider:
        """Return a SlurmProvider."""
        nodes = get_bps_config_value(self.site, "nodes_per_block", int, 1)
        cores_per_node = get_bps_config_value(self.site, "cores_per_node", int, cores_per_node)
        walltime = get_bps_config_value(self.site, "walltime", str, walltime, required=True)
        mem_per_node = get_bps_config_value(self.site, "mem_per_node", int, mem_per_node)
        qos = get_bps_config_value(self.site, "qos", str, qos)
        constraint = get_bps_config_value(self.site, "constraint", str, constraint)
        singleton = get_bps_config_value(self.site, "singleton", bool, singleton)
        exclusive = get_bps_config_value(self.site, "exclusive", bool, exclusive)
        scheduler_options = get_bps_config_value(self.site, "scheduler_options", str, scheduler_options)

        # Replace any filepath separators with underscores since Parsl
        # creates a shell script named f"cmd_{job_name}.sh" using the
        # --job-name value in the sbatch script.
        job_name = get_workflow_name(self.config).replace("/", "_")
        if scheduler_options is None:
            scheduler_options = ""
        scheduler_options += "\n"
        scheduler_options += f"#SBATCH --job-name={job_name}\n"
        if qos:
            scheduler_options += f"#SBATCH --qos={qos}\n"
        if constraint:
            scheduler_options += f"#SBATCH --constraint={constraint}\n"
        if singleton:
            # The following SBATCH directives allow only a single
            # slurm job (parsl block) with our job_name to run at
            # once. This means we can have one job running, and one
            # already in the queue when the first exceeds the walltime
            # limit. More backups could be achieved with a larger
            # value of max_blocks.  This only allows one job to be
            # actively running at once, so that needs to be sized
            # appropriately by the user.
            scheduler_options += "#SBATCH --dependency=singleton\n"
        provider = SlurmProvider(
            nodes_per_block=nodes,
            cores_per_node=cores_per_node,
            mem_per_node=mem_per_node,
            walltime=walltime,
            exclusive=exclusive,
            scheduler_options=scheduler_options,
            launcher=SrunLauncher(overrides="-K0 -k --cpu-bind=none"),
            **(provider_options or {}),
        )
        return provider
