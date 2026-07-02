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

"""Execution provider that pools a LocalProvider and a SlurmProvider."""

from __future__ import annotations

import logging
from typing import Any

from parsl.jobs.states import JobStatus
from parsl.providers import LocalProvider, SlurmProvider
from parsl.providers.base import ExecutionProvider

__all__ = ("ImpatientSlurmProvider",)

logger = logging.getLogger(__name__)

_LOCAL_PREFIX = "local:"
_SLURM_PREFIX = "slurm:"


class ImpatientSlurmProvider(ExecutionProvider):
    """An `~parsl.providers.base.ExecutionProvider` that pools a
    `~parsl.providers.LocalProvider` and a `~parsl.providers.SlurmProvider`.

    Blocks are submitted to the ``LocalProvider`` first (up to its
    ``max_blocks``), then to the ``SlurmProvider`` for additional capacity.
    This lets tasks start immediately on the head node while Slurm nodes are
    being allocated, with the pool expanding as those nodes come online.

    The executor sees a single provider whose ``max_blocks`` is the sum of the
    two sub-providers' limits.  Job IDs returned by :meth:`submit` are prefixed
    with ``"local:"`` or ``"slurm:"`` so that :meth:`status` and
    :meth:`cancel` can route each call to the correct sub-provider.

    Parameters
    ----------
    slurm_provider : `parsl.providers.SlurmProvider`
        Provider for Slurm cluster execution.
    local_cores : `int`, optional
        Number of cores to use on the head node while Slurm nodes are being
        allocated.  When ``0`` (the default) no head-node block is started and
        the provider behaves identically to a plain
        `~parsl.providers.SlurmProvider`.

    Notes
    -----
    ``cores_per_node`` and ``mem_per_node`` are intentionally left as ``None``
    on the composite so that the executor discovers per-worker resources from
    the workers themselves rather than using a single value that would be wrong
    for one of the two sub-providers.

    The head-node `~parsl.providers.LocalProvider` is configured
    automatically: it inherits ``worker_init`` from *slurm_provider* (so the
    same environment is set up on both kinds of workers) and sets
    ``PARSL_CORES`` to *local_cores* so that the executor caps the number of
    tasks sent to that block.
    """

    def __init__(
        self,
        slurm_provider: SlurmProvider,
        local_cores: int = 0,
    ) -> None:
        self.slurm = slurm_provider
        # When local_cores == 0 we still create a LocalProvider but with
        # max_blocks=0, so it is never submitted to.  This keeps the rest of
        # the implementation uniform.
        self.local = LocalProvider(
            max_blocks=1 if local_cores > 0 else 0,
            min_blocks=0,
            init_blocks=1 if local_cores > 0 else 0,
            worker_init=slurm_provider.worker_init + f"export PARSL_CORES={local_cores}\n",
        )

        # Aggregate scaling limits seen by the executor's strategy controller.
        self.min_blocks: int = self.local.min_blocks + slurm_provider.min_blocks
        self.max_blocks: int = self.local.max_blocks + slurm_provider.max_blocks
        self.init_blocks: int = self.local.init_blocks + slurm_provider.init_blocks
        self.nodes_per_block: int = slurm_provider.nodes_per_block
        self.parallelism: float = slurm_provider.parallelism

        # Deliberately None — see class docstring.
        self.cores_per_node: int | None = None
        self.mem_per_node: float | None = None

        # Required by ExecutionProvider; sub-providers maintain their own.
        self.resources: dict[object, Any] = {}

        self._script_dir: str | None = None
        # Map composite_id -> (tag, original_sub_id)
        self._job_map: dict[str, tuple[str, str]] = {}
        # Cache of the most recent status for each composite job.
        self._statuses: dict[str, JobStatus] = {}

    # ------------------------------------------------------------------
    # script_dir — forwarded to both sub-providers so their submit()
    # methods can write their scripts to the right place.
    # ------------------------------------------------------------------

    @property
    def script_dir(self) -> str | None:
        return self._script_dir

    @script_dir.setter
    def script_dir(self, value: str | None) -> None:
        self._script_dir = value
        self.local.script_dir = value
        self.slurm.script_dir = value

    # ------------------------------------------------------------------
    # ExecutionProvider interface
    # ------------------------------------------------------------------

    @property
    def label(self) -> str:
        return "impatient_slurm"

    @property
    def status_polling_interval(self) -> int:
        # Slurm polling is the bottleneck; use the longer interval to avoid
        # hammering sacct/squeue.
        return max(self.local.status_polling_interval, self.slurm.status_polling_interval)

    def submit(self, command: str, tasks_per_node: int, job_name: str = "parsl.auto") -> str:
        """Submit a block to the local or Slurm sub-provider.

        Local blocks are filled first (up to ``local_provider.max_blocks``
        non-terminal jobs); additional blocks are submitted to Slurm.

        Parameters
        ----------
        command : `str`
            Shell command to run on the worker.
        tasks_per_node : `int`
            Number of task slots per node.
        job_name : `str`, optional
            Human-readable label for the job.

        Returns
        -------
        job_id : `str`
            Composite job identifier prefixed with ``"local:"`` or
            ``"slurm:"``.
        """
        if self._local_active_count() < self.local.max_blocks:
            sub_id = self.local.submit(command, tasks_per_node, job_name)
            tag = _LOCAL_PREFIX
        else:
            sub_id = self.slurm.submit(command, tasks_per_node, job_name)
            tag = _SLURM_PREFIX

        composite_id = f"{tag}{sub_id}"
        self._job_map[composite_id] = (tag, str(sub_id))
        logger.debug(
            "Submitted composite block %s (tag=%s, sub_id=%s)", composite_id, tag, sub_id
        )
        return composite_id

    def status(self, job_ids: list[str]) -> list[JobStatus]:
        """Return the status of the requested jobs.

        Parameters
        ----------
        job_ids : `list` [`str`]
            Composite job identifiers as returned by :meth:`submit`.

        Returns
        -------
        statuses : `list` [`~parsl.jobs.states.JobStatus`]
        """
        local_pairs: list[tuple[str, str]] = []
        slurm_pairs: list[tuple[str, str]] = []
        for cid in job_ids:
            tag, sub_id = self._job_map[cid]
            if tag == _LOCAL_PREFIX:
                local_pairs.append((cid, sub_id))
            else:
                slurm_pairs.append((cid, sub_id))

        if local_pairs:
            local_statuses = self.local.status([sub_id for _, sub_id in local_pairs])
            for (cid, _), st in zip(local_pairs, local_statuses, strict=True):
                self._statuses[cid] = st

        if slurm_pairs:
            slurm_statuses = self.slurm.status([sub_id for _, sub_id in slurm_pairs])
            for (cid, _), st in zip(slurm_pairs, slurm_statuses, strict=True):
                self._statuses[cid] = st

        return [self._statuses[cid] for cid in job_ids]

    def cancel(self, job_ids: list[str]) -> list[bool]:
        """Cancel the requested jobs.

        Parameters
        ----------
        job_ids : `list` [`str`]
            Composite job identifiers as returned by :meth:`submit`.

        Returns
        -------
        results : `list` [`bool`]
        """
        local_pairs: list[tuple[str, str]] = []
        slurm_pairs: list[tuple[str, str]] = []
        for cid in job_ids:
            tag, sub_id = self._job_map[cid]
            if tag == _LOCAL_PREFIX:
                local_pairs.append((cid, sub_id))
            else:
                slurm_pairs.append((cid, sub_id))

        results: dict[str, bool] = {}

        if local_pairs:
            local_rets = self.local.cancel([sub_id for _, sub_id in local_pairs])
            for (cid, _), ok in zip(local_pairs, local_rets, strict=True):
                results[cid] = ok

        if slurm_pairs:
            slurm_rets = self.slurm.cancel([sub_id for _, sub_id in slurm_pairs])
            for (cid, _), ok in zip(slurm_pairs, slurm_rets, strict=True):
                results[cid] = ok

        return [results[cid] for cid in job_ids]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _local_active_count(self) -> int:
        """Return the number of non-terminal local blocks."""
        count = 0
        for cid, (tag, _) in self._job_map.items():
            if tag == _LOCAL_PREFIX:
                st = self._statuses.get(cid)
                # Treat unseen (freshly submitted, not yet polled) as active.
                if st is None or not st.terminal:
                    count += 1
        return count
