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

"""Query the Parsl monitoring database for BPS run reports."""

__all__ = ("find_monitoring_db", "get_run_reports")

import os
import sqlite3
from collections import defaultdict

import parsl

from lsst.ctrl.bps import WmsJobReport, WmsRunReport, WmsStates

from .workflow import ParslWorkflow

# Map Parsl task status names to WmsStates.
_PARSL_STATE_MAP: dict[str, WmsStates] = {
    "pending": WmsStates.READY,
    "launched": WmsStates.PENDING,
    "running": WmsStates.RUNNING,
    "exec_done": WmsStates.SUCCEEDED,
    "failed": WmsStates.FAILED,
    "dep_fail": WmsStates.PRUNED,
}


def _derive_overall_state(job_state_counts: dict[WmsStates, int]) -> WmsStates:
    """Determine overall workflow state from per-state job counts.

    Parameters
    ----------
    job_state_counts : `dict` [`WmsStates`, `int`]
        Number of jobs in each state.

    Returns
    -------
    state : `WmsStates`
        Overall workflow state.
    """
    if not job_state_counts:
        return WmsStates.UNKNOWN
    states = {s for s, n in job_state_counts.items() if n > 0}
    if WmsStates.RUNNING in states or WmsStates.PENDING in states:
        return WmsStates.RUNNING
    if WmsStates.FAILED in states:
        return WmsStates.FAILED
    if states == {WmsStates.SUCCEEDED}:
        return WmsStates.SUCCEEDED
    if states >= {WmsStates.SUCCEEDED, WmsStates.PRUNED}:
        return WmsStates.FAILED
    return WmsStates.UNKNOWN


def find_monitoring_db(submit_path: str, parsl_config: parsl.Config) -> str | None:
    """Locate the Parsl monitoring SQLite database.

    Parameters
    ----------
    submit_path : `str`
        The workflow submit path.
    parsl_config : `parsl.Config`
        Parsl configuration for the workflow.

    Returns
    -------
    path : `str` or None
        Absolute path to the monitoring database, or `None` if not found.
    """
    working_dir = os.path.abspath(submit_path).split("/submit")[0]
    if parsl_config.monitoring is not None:
        endpoint = parsl_config.monitoring.logging_endpoint or ""
        db_path = endpoint.removeprefix("sqlite:///")
        if not os.path.isabs(db_path):
            db_path = os.path.join(working_dir, db_path)
    else:
        run_dir = getattr(parsl_config, "run_dir", "runinfo")
        db_path = os.path.join(working_dir, run_dir, "monitoring.db")

    if os.path.isfile(db_path):
        return db_path

    return None


def get_run_reports(
    db_file: str,
    workflow: ParslWorkflow,
    submit_path: str,
) -> list[WmsRunReport]:
    """Build ``WmsRunReport`` objects from a Parsl monitoring database.

    Parameters
    ----------
    db_file : `str`
        Path to the Parsl monitoring SQLite database.
    workflow : `ParslWorkflow`
        Workflow object with ``name`` and ``bps_config`` attributes.
    submit_path : `str`
        Workflow submit path; stored as ``WmsRunReport.path``.

    Returns
    -------
    reports : `list` [`lsst.ctrl.bps.WmsRunReport`]
        List of one report for the workflow_name.
    """
    # Retrieve the status for every task in time-order so that the
    # current state is captured by iterating over the rows in order.
    # Constrain the query on workflow.workflow_name to ensure we get
    # all the runs, including restarts.
    query = """
        SELECT task.task_id, task.task_stderr, task.task_func_name,
        status.task_status_name
        FROM task
        JOIN status
          ON task.task_id = status.task_id AND task.run_id = status.run_id
        JOIN workflow
          ON task.run_id = workflow.run_id
        WHERE workflow.workflow_name = ? AND task.task_stderr IS NOT NULL
        ORDER BY task.task_stderr, status.timestamp ASC
    """
    with sqlite3.connect(db_file) as conn:
        conn.row_factory = sqlite3.Row
        task_rows = conn.execute(query, (workflow.name,)).fetchall()

    job_reports: dict[str, WmsJobReport] = {}

    for row in task_rows:
        wms_id = row["task_id"]
        stderr = row["task_stderr"]
        label = row["task_func_name"]
        status = row["task_status_name"]
        if stderr in job_reports and job_reports[stderr].state == WmsStates.SUCCEEDED:
            # On restart, SUCCEEDED jobs can be marked UNKNOWN, so
            # don't overwrite these entries.
            continue
        job_reports[stderr] = WmsJobReport(
            wms_id=wms_id,
            name=os.path.basename(stderr).split(".")[0],
            label=label,
            state=_PARSL_STATE_MAP.get(status, WmsStates.UNKNOWN)
        )

    # Build per-label state counts.
    job_summary: dict[str, dict[WmsStates, int]] = defaultdict(lambda: dict.fromkeys(WmsStates, 0))
    job_state_counts: dict[WmsStates, int] = dict.fromkeys(WmsStates, 0)

    jobs = []
    # Loop in reversed order to preserve expected job order by label.
    for stderr, job_report in reversed(job_reports.items()):
        job_summary[job_report.label][job_report.state] += 1
        job_state_counts[job_report.state] += 1
        jobs.append(job_report)

    total = sum(job_state_counts.values())
    overall_state = _derive_overall_state(job_state_counts)

    run_summary = ";".join(
        f"{label}:{sum(counts.values())}" for label, counts in job_summary.items()
    )

    bps_config = workflow.bps_config

    run_report = WmsRunReport(
        wms_id="",
        path=submit_path,
        run=workflow.name,
        state=overall_state,
        total_number_jobs=total,
        job_state_counts=dict(job_state_counts),
        job_summary={label: dict(counts) for label, counts in job_summary.items()},
        run_summary=run_summary or None,
        jobs=jobs,
        operator=bps_config['operator'],
        project=bps_config['project'],
        campaign=bps_config['campaign'],
        payload=bps_config['payloadName'],
    )

    return [run_report]
