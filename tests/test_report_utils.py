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

import os
import sqlite3
import types
from unittest.mock import MagicMock

import pytest

from lsst.ctrl.bps import WmsStates
from lsst.ctrl.bps.parsl.report_utils import (
    _derive_overall_state,
    find_monitoring_db,
    get_run_reports,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_monitoring_db(path, workflow_name, tasks, run_id=1):
    """Create a minimal Parsl monitoring SQLite database for testing.

    Parameters
    ----------
    path : str or pathlib.Path
        File path for the SQLite database to create.
    workflow_name : str
        Name of the workflow to insert into the ``workflow`` table.
    tasks : list of dict
        Each dict describes one task and must contain:

        ``task_id`` : int
            Unique task identifier.
        ``task_func_name`` : str
            Label / function name for the task.
        ``task_stderr`` : str or None
            Path to the task's stderr file (``None`` for internal tasks).
        ``statuses`` : list of (str, int) tuples
            Sequence of ``(status_name, timestamp)`` pairs representing the
            status history of the task in chronological order.

    run_id : int, optional
        ``run_id`` shared by the workflow and all its tasks.
    """
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE workflow (
                run_id      TEXT PRIMARY KEY,
                workflow_name TEXT
            );
            CREATE TABLE task (
                task_id         INTEGER,
                run_id          TEXT,
                task_func_name  TEXT,
                task_stderr     TEXT
            );
            CREATE TABLE status (
                task_id          INTEGER,
                run_id           TEXT,
                task_status_name TEXT,
                timestamp        REAL
            );
        """)
        conn.execute(
            "INSERT INTO workflow VALUES (?, ?)",
            (run_id, workflow_name),
        )
        for task in tasks:
            conn.execute(
                "INSERT INTO task VALUES (?, ?, ?, ?)",
                (task["task_id"], run_id, task["task_func_name"], task["task_stderr"]),
            )
            for status_name, timestamp in task["statuses"]:
                conn.execute(
                    "INSERT INTO status VALUES (?, ?, ?, ?)",
                    (task["task_id"], run_id, status_name, timestamp),
                )


def make_workflow(name="test_workflow"):
    """Return a minimal workflow mock suitable for ``get_run_reports``."""
    workflow = MagicMock()
    workflow.name = name
    workflow.bps_config = {
        "operator": "test_operator",
        "project": "test_project",
        "campaign": "test_campaign",
        "payloadName": "test_payload",
    }
    return workflow


# ---------------------------------------------------------------------------
# _derive_overall_state
# ---------------------------------------------------------------------------

def test_derive_overall_state_empty():
    assert _derive_overall_state({}) == WmsStates.UNKNOWN


def test_derive_overall_state_all_zero_counts():
    counts = dict.fromkeys(WmsStates, 0)
    assert _derive_overall_state(counts) == WmsStates.UNKNOWN


def test_derive_overall_state_running():
    counts = dict.fromkeys(WmsStates, 0)
    counts[WmsStates.RUNNING] = 2
    counts[WmsStates.SUCCEEDED] = 3
    assert _derive_overall_state(counts) == WmsStates.RUNNING


def test_derive_overall_state_pending_counts_as_running():
    counts = dict.fromkeys(WmsStates, 0)
    counts[WmsStates.PENDING] = 1
    assert _derive_overall_state(counts) == WmsStates.RUNNING


def test_derive_overall_state_failed():
    counts = dict.fromkeys(WmsStates, 0)
    counts[WmsStates.FAILED] = 1
    counts[WmsStates.SUCCEEDED] = 5
    assert _derive_overall_state(counts) == WmsStates.FAILED


def test_derive_overall_state_succeeded():
    counts = dict.fromkeys(WmsStates, 0)
    counts[WmsStates.SUCCEEDED] = 4
    assert _derive_overall_state(counts) == WmsStates.SUCCEEDED


def test_derive_overall_state_succeeded_plus_pruned_is_failed():
    """A mix of only SUCCEEDED and PRUNED means some jobs were skipped."""
    counts = dict.fromkeys(WmsStates, 0)
    counts[WmsStates.SUCCEEDED] = 3
    counts[WmsStates.PRUNED] = 2
    assert _derive_overall_state(counts) == WmsStates.FAILED


# ---------------------------------------------------------------------------
# find_monitoring_db
# ---------------------------------------------------------------------------

def _make_parsl_config(monitoring_endpoint=None, run_dir="runinfo"):
    """Build a minimal parsl.Config-like object for find_monitoring_db."""
    cfg = types.SimpleNamespace(run_dir=run_dir)
    if monitoring_endpoint is not None:
        cfg.monitoring = types.SimpleNamespace(logging_endpoint=monitoring_endpoint)
    else:
        cfg.monitoring = None
    return cfg


def test_find_monitoring_db_default_location(tmp_path):
    """DB found at the default runinfo/monitoring.db path."""
    working_dir = tmp_path / "run"
    working_dir.mkdir()
    db_path = working_dir / "runinfo" / "monitoring.db"
    db_path.parent.mkdir()
    db_path.touch()

    out_prefix = str(working_dir / "submit" / "something")
    parsl_config = _make_parsl_config()

    result = find_monitoring_db(out_prefix, parsl_config)
    assert result == str(db_path)


def test_find_monitoring_db_missing_returns_none(tmp_path):
    """Returns None when the DB file does not exist."""
    out_prefix = str(tmp_path / "submit" / "something")
    parsl_config = _make_parsl_config()

    assert find_monitoring_db(out_prefix, parsl_config) is None


def test_find_monitoring_db_absolute_endpoint(tmp_path):
    """DB found via absolute path in monitoring.logging_endpoint."""
    db_path = tmp_path / "custom.db"
    db_path.touch()

    out_prefix = str(tmp_path / "submit" / "something")
    parsl_config = _make_parsl_config(monitoring_endpoint=f"sqlite:///{db_path}")

    result = find_monitoring_db(out_prefix, parsl_config)
    assert result == str(db_path)


def test_find_monitoring_db_relative_endpoint(tmp_path):
    """DB found via relative path in monitoring.logging_endpoint."""
    working_dir = tmp_path / "run"
    working_dir.mkdir()
    db_path = working_dir / "custom.db"
    db_path.touch()

    out_prefix = str(working_dir / "submit" / "something")
    parsl_config = _make_parsl_config(monitoring_endpoint="sqlite:///custom.db")

    result = find_monitoring_db(out_prefix, parsl_config)
    assert result == str(db_path)


def test_find_monitoring_db_custom_run_dir(tmp_path):
    """DB found when parsl_config.run_dir is set to a non-default value."""
    working_dir = tmp_path / "run"
    working_dir.mkdir()
    db_path = working_dir / "parsl_runinfo" / "monitoring.db"
    db_path.parent.mkdir()
    db_path.touch()

    out_prefix = str(working_dir / "submit" / "something")
    parsl_config = _make_parsl_config(run_dir="parsl_runinfo")

    result = find_monitoring_db(out_prefix, parsl_config)
    assert result == str(db_path)


# ---------------------------------------------------------------------------
# get_run_reports
# ---------------------------------------------------------------------------

def test_get_run_reports_single_job(tmp_path):
    """Single succeeded job produces a correct run report."""
    db_path = tmp_path / "monitoring.db"
    make_monitoring_db(
        db_path,
        workflow_name="test_workflow",
        tasks=[
            {
                "task_id": 1,
                "task_func_name": "labelA",
                "task_stderr": "/logs/job1.stderr",
                "statuses": [("exec_done", 1.0)],
            }
        ],
    )

    workflow = make_workflow("test_workflow")
    reports = get_run_reports(str(db_path), workflow, "/submit/out")

    assert len(reports) == 1
    report = reports[0]
    assert report.total_number_jobs == 1
    assert report.state == WmsStates.SUCCEEDED
    assert report.job_state_counts[WmsStates.SUCCEEDED] == 1
    assert len(report.jobs) == 1
    assert report.jobs[0].label == "labelA"
    assert report.jobs[0].name == "job1"
    assert report.jobs[0].state == WmsStates.SUCCEEDED


def test_get_run_reports_multiple_labels(tmp_path):
    """Jobs from multiple labels are counted and summarised per label."""
    db_path = tmp_path / "monitoring.db"
    make_monitoring_db(
        db_path,
        workflow_name="test_workflow",
        tasks=[
            {
                "task_id": 1,
                "task_func_name": "labelA",
                "task_stderr": "/logs/jobA1.stderr",
                "statuses": [("exec_done", 1.0)],
            },
            {
                "task_id": 2,
                "task_func_name": "labelA",
                "task_stderr": "/logs/jobA2.stderr",
                "statuses": [("failed", 2.0)],
            },
            {
                "task_id": 3,
                "task_func_name": "labelB",
                "task_stderr": "/logs/jobB1.stderr",
                "statuses": [("exec_done", 3.0)],
            },
        ],
    )

    workflow = make_workflow("test_workflow")
    reports = get_run_reports(str(db_path), workflow, "/submit/out")

    report = reports[0]
    assert report.total_number_jobs == 3
    assert report.state == WmsStates.FAILED
    assert report.job_summary["labelA"][WmsStates.SUCCEEDED] == 1
    assert report.job_summary["labelA"][WmsStates.FAILED] == 1
    assert report.job_summary["labelB"][WmsStates.SUCCEEDED] == 1


def test_get_run_reports_run_summary_format(tmp_path):
    """run_summary lists each label with its total job count."""
    db_path = tmp_path / "monitoring.db"
    make_monitoring_db(
        db_path,
        workflow_name="test_workflow",
        tasks=[
            {
                "task_id": 1,
                "task_func_name": "labelA",
                "task_stderr": "/logs/jobA1.stderr",
                "statuses": [("exec_done", 1.0)],
            },
            {
                "task_id": 2,
                "task_func_name": "labelB",
                "task_stderr": "/logs/jobB1.stderr",
                "statuses": [("exec_done", 2.0)],
            },
            {
                "task_id": 3,
                "task_func_name": "labelB",
                "task_stderr": "/logs/jobB2.stderr",
                "statuses": [("exec_done", 3.0)],
            },
        ],
    )

    workflow = make_workflow("test_workflow")
    report = get_run_reports(str(db_path), workflow, "/submit/out")[0]

    # Each label entry is "label:count"; split and check as a set so order
    # does not matter.
    entries = set(report.run_summary.split(";"))
    assert entries == {"labelA:1", "labelB:2"}


def test_get_run_reports_restart_preserves_succeeded(tmp_path):
    """After a restart, a previously SUCCEEDED job is not overwritten."""
    db_path = tmp_path / "monitoring.db"
    # Simulate a job that completed, then on restart was re-queued and
    # emitted an 'unknown' status before Parsl recognised the cached result.
    make_monitoring_db(
        db_path,
        workflow_name="test_workflow",
        tasks=[
            {
                "task_id": 1,
                "task_func_name": "labelA",
                "task_stderr": "/logs/job1.stderr",
                "statuses": [("exec_done", 1.0), ("unknown", 2.0)],
            }
        ],
    )

    workflow = make_workflow("test_workflow")
    report = get_run_reports(str(db_path), workflow, "/submit/out")[0]

    assert report.jobs[0].state == WmsStates.SUCCEEDED


def test_get_run_reports_ignores_null_stderr(tmp_path):
    """Tasks with NULL task_stderr (internal Parsl tasks) are excluded."""
    db_path = tmp_path / "monitoring.db"
    make_monitoring_db(
        db_path,
        workflow_name="test_workflow",
        tasks=[
            {
                "task_id": 1,
                "task_func_name": "internal",
                "task_stderr": None,
                "statuses": [("exec_done", 1.0)],
            },
            {
                "task_id": 2,
                "task_func_name": "labelA",
                "task_stderr": "/logs/job2.stderr",
                "statuses": [("exec_done", 2.0)],
            },
        ],
    )

    workflow = make_workflow("test_workflow")
    report = get_run_reports(str(db_path), workflow, "/submit/out")[0]

    assert report.total_number_jobs == 1
    assert report.jobs[0].label == "labelA"


def test_get_run_reports_bps_config_fields(tmp_path):
    """WmsRunReport is populated with values from workflow.bps_config."""
    db_path = tmp_path / "monitoring.db"
    make_monitoring_db(db_path, workflow_name="test_workflow", tasks=[])

    workflow = make_workflow("test_workflow")
    report = get_run_reports(str(db_path), workflow, "/my/submit/path")[0]

    assert report.operator == "test_operator"
    assert report.project == "test_project"
    assert report.campaign == "test_campaign"
    assert report.payload == "test_payload"
    assert report.path == "/my/submit/path"
    assert report.run == "test_workflow"


def test_get_run_reports_empty_workflow(tmp_path):
    """A workflow with no tasks yields an UNKNOWN overall state."""
    db_path = tmp_path / "monitoring.db"
    make_monitoring_db(db_path, workflow_name="test_workflow", tasks=[])

    workflow = make_workflow("test_workflow")
    report = get_run_reports(str(db_path), workflow, "/submit/out")[0]

    assert report.total_number_jobs == 0
    assert report.state == WmsStates.UNKNOWN
    assert report.run_summary is None
