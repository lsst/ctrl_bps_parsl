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

from lsst.ctrl.bps import BaseWmsService, BaseWmsWorkflow, BpsConfig, GenericWorkflow

from .workflow import ParslWorkflow

__all__ = ("ParslService",)


class ParslService(BaseWmsService):
    """Parsl-based implementation for the WMS interface."""

    def prepare(
        self, config: BpsConfig, generic_workflow: GenericWorkflow, out_prefix: str | None = None
    ) -> BaseWmsWorkflow:
        """Convert a generic workflow to a Parsl pipeline.

        Parameters
        ----------
        config: `lss.ctrl.bps.BpsConfig`
            Configuration of the workflow.
        generic_workflow: `lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic representation of a single workflow.
        out_prefix : `str` [None]
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

    def submit(self, workflow: BaseWmsWorkflow):
        """Submit a single WMS workflow

        Parameters
        ----------
        workflow : `lsst.ctrl.bps.BaseWmsWorkflow`
            Prepared WMS Workflow to submit for execution
        """
        workflow.start()
        workflow.run()

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
