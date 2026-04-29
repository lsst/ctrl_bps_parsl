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

"""Unit tests for ParslWorkflow.restart, load_dfk, and shutdown."""

from unittest.mock import MagicMock, patch

import pytest

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.parsl.workflow import ParslWorkflow


def make_workflow():
    """Return a minimal ParslWorkflow and its mock parsl_config.

    ``get_parsl_config`` and ``SiteConfig`` are patched so that no real Parsl
    executors are created.  The returned parsl_config mock has an empty
    ``executors`` list (so the ``bash_app`` loop in ``__init__`` is a no-op).
    """
    bps_config = BpsConfig(
        {
            "submitPath": ".",
            "operator": "operator",
            "computeSite": "local",
            "uniqProcName": "test_run",
            "project": "test_project",
            "campaign": "test_campaign",
        }
    )

    mock_parsl_config = MagicMock()
    mock_parsl_config.executors = []

    mock_site = MagicMock()
    mock_site.get_command_prefix.return_value = ""

    with (
        patch(
            "lsst.ctrl.bps.parsl.workflow.get_parsl_config",
            return_value=mock_parsl_config,
        ),
        patch(
            "lsst.ctrl.bps.parsl.workflow.SiteConfig",
        ) as mock_site_class,
    ):
        mock_site_class.from_config.return_value = mock_site
        workflow = ParslWorkflow(
            name="test_run",
            config=bps_config,
            path=".",
            jobs={},
            parents={},
            endpoints=[],
        )

    return workflow, mock_parsl_config


# ---------------------------------------------------------------------------
# restart()
# ---------------------------------------------------------------------------


def test_restart_calls_get_last_checkpoint():
    """restart() must call parsl.utils.get_last_checkpoint()."""
    workflow, _ = make_workflow()

    with (
        patch("parsl.utils.get_last_checkpoint", return_value="/runinfo/000/tasks.pkl") as mock_glc,
        patch("parsl.load"),
        patch("lsst.ctrl.bps.parsl.workflow.set_parsl_logging"),
    ):
        workflow.restart()

    mock_glc.assert_called_once_with()


def test_restart_checkpoint_files_set_when_checkpoint_exists():
    """When a checkpoint file exists, get_last_checkpoint() returns [path]."""
    workflow, mock_parsl_config = make_workflow()
    checkpoint = "/runinfo/000/tasks.pkl"

    with (
        patch("parsl.utils.get_last_checkpoint", return_value=[checkpoint]),
        patch("parsl.load"),
        patch("lsst.ctrl.bps.parsl.workflow.set_parsl_logging"),
    ):
        workflow.restart()

    assert mock_parsl_config.memoizer.checkpoint_files == [checkpoint]


def test_restart_checkpoint_files_empty_when_no_checkpoint():
    """When there are no checkpoint files, get_last_checkpoint() returns []."""
    workflow, mock_parsl_config = make_workflow()

    with (
        patch("parsl.utils.get_last_checkpoint", return_value=[]),
        patch("parsl.load"),
        patch("lsst.ctrl.bps.parsl.workflow.set_parsl_logging"),
    ):
        workflow.restart()

    assert mock_parsl_config.memoizer.checkpoint_files == []


def test_restart_calls_parsl_load():
    """restart() must call parsl.load with the workflow's parsl_config."""
    workflow, mock_parsl_config = make_workflow()

    with (
        patch("parsl.utils.get_last_checkpoint", return_value="/runinfo/000/tasks.pkl"),
        patch("parsl.load") as mock_load,
        patch("lsst.ctrl.bps.parsl.workflow.set_parsl_logging"),
    ):
        workflow.restart()

    mock_load.assert_called_once_with(mock_parsl_config)


def test_restart_does_not_call_initialize_jobs():
    """
    restart() must not run initialize_jobs (no pipetaskInit side-effects).
    """
    workflow, _ = make_workflow()
    mock_job = MagicMock()
    workflow.jobs["pipetaskInit"] = mock_job

    with (
        patch("parsl.utils.get_last_checkpoint", return_value=None),
        patch("parsl.load"),
        patch("lsst.ctrl.bps.parsl.workflow.set_parsl_logging"),
    ):
        workflow.restart()

    mock_job.run_local.assert_not_called()


# ---------------------------------------------------------------------------
# load_dfk()
# ---------------------------------------------------------------------------


def test_load_dfk_sets_dfk():
    """After load_dfk(), workflow.dfk is the object returned by parsl.load."""
    workflow, _ = make_workflow()
    fake_dfk = MagicMock()

    with (
        patch("parsl.load", return_value=fake_dfk),
        patch("lsst.ctrl.bps.parsl.workflow.set_parsl_logging"),
    ):
        workflow.load_dfk()

    assert workflow.dfk is fake_dfk


def test_load_dfk_raises_if_already_started():
    """load_dfk() raises RuntimeError when the workflow is already running."""
    workflow, _ = make_workflow()
    workflow.dfk = MagicMock()

    with pytest.raises(RuntimeError, match="already started"):
        workflow.load_dfk()


def test_load_dfk_calls_set_parsl_logging():
    """load_dfk() forwards the BPS config to set_parsl_logging."""
    workflow, _ = make_workflow()

    with (
        patch("parsl.load"),
        patch("lsst.ctrl.bps.parsl.workflow.set_parsl_logging") as mock_logging,
    ):
        workflow.load_dfk()

    mock_logging.assert_called_once_with(workflow.bps_config)


# ---------------------------------------------------------------------------
# shutdown()
# ---------------------------------------------------------------------------


def test_shutdown_calls_dfk_cleanup():
    """shutdown() must invoke cleanup() on the active DFK."""
    workflow, _ = make_workflow()
    fake_dfk = MagicMock()
    workflow.dfk = fake_dfk

    with patch("parsl.DataFlowKernelLoader.clear"):
        workflow.shutdown()

    fake_dfk.cleanup.assert_called_once_with()


def test_shutdown_clears_dfk():
    """After shutdown(), workflow.dfk is None."""
    workflow, _ = make_workflow()
    workflow.dfk = MagicMock()

    with patch("parsl.DataFlowKernelLoader.clear"):
        workflow.shutdown()

    assert workflow.dfk is None


def test_shutdown_calls_dfkl_clear():
    """shutdown() must call parsl.DataFlowKernelLoader.clear()."""
    workflow, _ = make_workflow()
    workflow.dfk = MagicMock()

    with patch("parsl.DataFlowKernelLoader.clear") as mock_clear:
        workflow.shutdown()

    mock_clear.assert_called_once_with()


def test_shutdown_raises_if_not_started():
    """
    shutdown() raises RuntimeError when the workflow has not been started.
    """
    workflow, _ = make_workflow()

    with pytest.raises(RuntimeError, match="not started"):
        workflow.shutdown()
