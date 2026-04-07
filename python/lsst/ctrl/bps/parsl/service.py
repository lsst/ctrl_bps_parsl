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
import os
from typing import Any

from lsst.ctrl.bps import BaseWmsService, BaseWmsWorkflow, BpsConfig, GenericWorkflow, WmsRunReport

from .report_utils import find_monitoring_db, get_run_reports
from .workflow import ParslWorkflow

_LOG = logging.getLogger(__name__)

__all__ = ("ParslService",)


class ParslService(BaseWmsService):
    """Parsl-based implementation for the WMS interface."""

    def prepare(
        self, config: BpsConfig, generic_workflow: GenericWorkflow, out_prefix: str | None = None
    ) -> BaseWmsWorkflow:
        """Convert a generic workflow to a Parsl pipeline.

        Parameters
        ----------
        config : `lsst.ctrl.bps.BpsConfig`
            Configuration of the workflow.
        generic_workflow : `lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic representation of a single workflow.
        out_prefix : `str` or `None`
            Prefix for WMS output files.

        Returns
        -------
        workflow : `ParslWorkflow`
            Workflow that will execute the jobs.
        """
        service_class = self.__class__.__module__ + "." + self.__class__.__name__
        if out_prefix is None:
            out_prefix = config["submitPath"]
        workflow = ParslWorkflow.from_generic_workflow(config, generic_workflow, out_prefix, service_class)
        workflow.write(out_prefix)
        return workflow

    def submit(self, workflow: BaseWmsWorkflow, **kwargs: Any):
        """Submit a single WMS workflow.

        Parameters
        ----------
        workflow : `lsst.ctrl.bps.BaseWmsWorkflow`
            Prepared WMS Workflow to submit for execution.
        **kwargs : `~typing.Any`
            Additional modifiers to the configuration.
        """
        workflow.start()
        workflow.run()

    def report(
        self,
        wms_workflow_id: str,
        user: str | None = None,
        hist: float = 0,
        pass_thru: str | None = None,
        is_global: bool = False,
        return_exit_codes: bool = False,
    ) -> tuple[list[WmsRunReport], str]:
        """Query the Parsl monitoring database for the status of a run.

        Parameters
        ----------
        wms_workflow_id : `str`
            The submit path (``out_prefix``) of the workflow to report on.
        user : `str`, optional
            Ignored; Parsl monitoring does not record the submitting user.
        hist : `float`, optional
            Ignored; all records in the monitoring database are always
            considered.
        pass_thru : `str`, optional
            Ignored.
        is_global : `bool`, optional
            Ignored; Parsl uses a single local monitoring database.
        return_exit_codes : `bool`, optional
            Ignored; Parsl monitoring does not expose per-job exit codes.

        Returns
        -------
        run_reports : `list` [`lsst.ctrl.bps.WmsRunReport`]
            Status information for the requested run(s).
        message : `str`
            Informational message, or an empty string when no issues arose.
        """
        # Use the full submit path for finding various artifacts.
        submit_path = os.path.realpath(wms_workflow_id)

        # Load the pickled workflow to recover the Parsl configuration (needed
        # to locate the monitoring database).
        workflow = ParslWorkflow.read(submit_path)
        parsl_config = workflow.parsl_config
        workflow_name = workflow.name

        working_dir = submit_path.split("/submit")[0]
        db_file = find_monitoring_db(working_dir, parsl_config)
        if db_file is None:
            return [], (
                f"No Parsl monitoring database found for submit path '{submit_path}'. "
                "Ensure that monitoring is enabled in the BPS configuration "
                "(site.<computeSite>.monitorEnable: true)."
            )

        _LOG.debug("Found monitoring database at %s", db_file)
        reports = get_run_reports(db_file, workflow, submit_path)

        if not reports:
            return [], f"No runs matching workflow '{workflow_name}' found in {db_file}."

        return reports, ""

    def restart(self, out_prefix: str) -> tuple[str, str, str]:
        """Restart a workflow from the point of failure.

        Parameters
        ----------
        out_prefix : `str`
            Id for workflow to be restarted. For this service, it is the prefix
            for WMS files, also known as the ``submitPath``.

        Returns
        -------
        wms_id : `str`
            Id of the restarted workflow. If restart failed, it will be set
            to None.
        run_name : `str`
            Name of the restarted workflow. If restart failed, it will be set
            to None.
        message : `str`
            A message describing any issues encountered during the restart.
            If there were no issue, an empty string is returned.
        """
        workflow = ParslWorkflow.read(out_prefix)
        workflow.restart()
        workflow.run()
        return workflow.name, workflow.name, ""
