from typing import Tuple

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

    def restart(self, out_prefix: str) -> Tuple[str, str, str]:
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
