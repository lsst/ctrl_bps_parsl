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

from typing import TYPE_CHECKING, Any

from parsl.executors import WorkQueueExecutor
from parsl.executors.base import ParslExecutor
from parsl.launchers import SrunLauncher
from parsl.providers import LocalProvider

try:
    from parsl.providers.base import ExecutionProvider
except ImportError:
    from parsl.providers.provider_base import ExecutionProvider  # type: ignore

from ..configuration import get_bps_config_value
from ..site import SiteConfig

if TYPE_CHECKING:
    from ..job import ParslJob

__all__ = ("LocalSrunWorkQueue", "WorkQueue")


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
        the ``add_resources`` argument.

    Notes
    -----
    The following BPS configuration parameters are recognized, overriding the
    defaults:

    - ``port`` (`int`): The port used by work_queue. Default: ``9000``.
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
        kwargs["add_resources"] = True
        super().__init__(*args, **kwargs)

    def make_executor(
        self,
        label: str,
        provider: ExecutionProvider,
        *,
        port: int = 9000,
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
            Port used by work_queue.  Default: ``9000``.
        worker_options : `str`, optional
            Extra options to pass to work_queue workers, e.g.,
            ``"--memory=90000"``. Default: `""`.
        wq_max_retries : `int`, optional
            Number of retries for work_queue to attempt per job.  Set to
            ``None`` to have it try indefinitely; set to ``1`` to have Parsl
            control the number of retries.  Default: ``1``.
        """
        port = get_bps_config_value(self.site, "port", int, port)
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


class LocalSrunWorkQueue(WorkQueue):
    """Configuration for a `WorkQueueExecutor` that uses a `LocalProvider`
    to manage resources.

    This can be used directly as the site configuration within a
    multi-node allocation when Slurm is available.  For running on a
    single node, e.g., a laptop, a `SingleNodeLauncher` is used, and
    Slurm need not be available.

    The following BPS configuration parameters are recognized, overriding the
    defaults:

    - ``port`` (`int`): The port used by work_queue. Default: ``9000``.
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

    def get_executors(self) -> list[ParslExecutor]:
        """Get a list of executors to be used in processing."""
        nodes_per_block = get_bps_config_value(self.site, "nodes_per_block", int, 1)
        provider_options: dict[str, Any] = {
            "nodes_per_block": nodes_per_block,
            "init_blocks": 0,
            "min_blocks": 0,
            "max_blocks": 1,
            "parallelism": 0,
            "cmd_timeout": 300,
        }
        if nodes_per_block > 1:
            provider_options["launcher"] = SrunLauncher(overrides="-K0 -k --slurmd-debug=verbose")
        provider = LocalProvider(**provider_options)
        return [self.make_executor("work_queue", provider)]

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
        return "work_queue"
