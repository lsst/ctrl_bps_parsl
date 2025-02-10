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
import pickle
from collections.abc import Iterable, Mapping

import parsl
import parsl.config
from parsl.app.app import bash_app
from parsl.app.bash import BashApp
from parsl.app.futures import Future

from lsst.ctrl.bps import BaseWmsWorkflow, BpsConfig, GenericWorkflow, GenericWorkflowJob

from .configuration import get_workflow_filename, set_parsl_logging
from .job import ParslJob, get_file_paths
from .site import SiteConfig

__all__ = ("ParslWorkflow", "get_parsl_config")

_log = logging.getLogger("lsst.ctrl.bps.parsl")


def get_parsl_config(config: BpsConfig) -> parsl.config.Config:
    """Construct parsl configuration from BPS configuration.

    For details on the site configuration, see `SiteConfig`. For details on the
    monitor configuration, see ``get_parsl_monitor``.

    `SiteConfig` provides an implementation of the method ``get_parsl_config``
    which returns a Parsl configuration with sensible defaults. Subclasses
    of `SiteConfig` can overwrite that method to configure Parsl in a
    way specific to the site's configuration.

    Parameters
    ----------
    config : `BpsConfig`
        BPS configuration.

    Returns
    -------
    parsl_config : `parsl.config.Config`
        Parsl configuration.
    """
    site = SiteConfig.from_config(config)
    return site.get_parsl_config()


class ParslWorkflow(BaseWmsWorkflow):
    """Parsl-based workflow object to manage execution of workflow.

    Parameters
    ----------
    name : `str`
        Unique name of workflow.
    config : `lsst.ctrl.bps.BpsConfig`
        Generic workflow config.
    path : `str`
        Path prefix for workflow output files.
    jobs : `dict` mapping `str` to `ParslJob`
        Jobs to be executed.
    parents : `dict` mapping `str` to iterable of `str`
        Dependency tree. Keywords are job names, and values are a list of job
        names that must be executed before the keyword job name can be
        executed.
    endpoints : iterable of `str`
        Endpoints of the dependency tree. These jobs (specified by name) have
        no children.
    final : `ParslJob`, optional
        Final job to be done, e.g., to merge the execution butler. This is done
        locally.
    """

    def __init__(
        self,
        name: str,
        config: BpsConfig,
        path: str,
        jobs: dict[str, ParslJob],
        parents: Mapping[str, Iterable[str]],
        endpoints: Iterable[str],
        final: ParslJob | None = None,
    ):
        super().__init__(name, config)

        self.path = path
        self.bps_config = config
        self.parsl_config = get_parsl_config(config)
        self.site = SiteConfig.from_config(config)
        self.dfk: parsl.DataFlowKernel | None = None  # type: ignore
        self.command_prefix = self.site.get_command_prefix()

        # these are function decorators
        self.apps: dict[str, BashApp] = {
            ex.label: bash_app(  # type: ignore
                executors=[ex.label], cache=True, ignore_for_cache=["stderr", "stdout"]
            )
            for ex in self.parsl_config.executors
        }

        self.jobs = jobs
        self.parents = parents
        self.endpoints = endpoints
        self.final = final

    def __reduce__(self):
        """Recipe for pickle"""
        return type(self), (
            self.name,
            self.bps_config,
            self.path,
            self.jobs,
            self.parents,
            self.endpoints,
            self.final,
        )

    @classmethod
    def from_generic_workflow(
        cls, config: BpsConfig, generic_workflow: GenericWorkflow, out_prefix: str, service_class: str
    ) -> BaseWmsWorkflow:
        """Create a ParslWorkflow object from a BPS GenericWorkflow.

        Parameters
        ----------
        config : `BpsConfig`
            Configuration of the workflow.
        generic_workflow : `lsst.ctrl.bps.generic_workflow.GenericWorkflow`
            Generic representation of a single workflow.
        out_prefix : `str`
            Prefix for workflow output files.
        service_class : `str`
            Full module name of WMS service class that created this workflow.

        Returns
        -------
        self : `ParslWorkflow`
            Constructed workflow.
        """
        # Generate list of jobs
        jobs: dict[str, ParslJob] = {}
        for job_name in generic_workflow:
            generic_job = generic_workflow.get_job(job_name)
            assert generic_job.name not in jobs
            jobs[job_name] = ParslJob(generic_job, config, get_file_paths(generic_workflow, job_name))

        parents = {name: set(generic_workflow.predecessors(name)) for name in jobs}
        endpoints = [name for name in jobs if generic_workflow.out_degree(name) == 0]

        # Add final job: execution butler merge
        job = generic_workflow.get_final()
        final: ParslJob | None = None
        if job is not None:
            assert isinstance(job, GenericWorkflowJob)
            final = ParslJob(job, config, get_file_paths(generic_workflow, job.name))

        return cls(generic_workflow.name, config, out_prefix, jobs, parents, endpoints, final)

    def write(self, out_prefix: str):
        """Write workflow state.

        This, in combination with the parsl checkpoint files, can be used to
        restart a workflow that was interrupted.

        Parameters
        ----------
        out_prefix : `str`
            Root directory to be used for WMS workflow inputs and outputs
            as well as internal WMS files.
        """
        filename = get_workflow_filename(out_prefix)
        _log.info("Writing workflow with ID=%s", out_prefix)
        with open(filename, "wb") as fd:
            pickle.dump(self, fd)

    @classmethod
    def read(cls, out_prefix: str) -> "ParslWorkflow":
        """Construct from the saved workflow state.

        Parameters
        ----------
        out_prefix : `str`
            Root directory to be used for WMS workflow inputs and outputs
            as well as internal WMS files.

        Returns
        -------
        self : `ParslWorkflow`
            Constructed workflow.
        """
        filename = get_workflow_filename(out_prefix)
        with open(filename, "rb") as fd:
            self = pickle.load(fd)
        assert isinstance(self, cls)
        return self

    def run(self, block: bool = True) -> list[Future | None]:
        """Run the workflow.

        Parameters
        ----------
        block : `bool`, optional
            Block returning from this method until the workflow is complete? If
            `False`, jobs may still be running when this returns, and it is the
            user's responsibility to call the ``finalize_jobs`` and
            ``shutdown`` methods when they are complete.

        Returns
        -------
        futures : `list` of `Future`
            `Future` objects linked to the execution of the endpoint jobs.
        """
        futures = [self.execute(name) for name in self.endpoints]
        if block:
            # Calling .exception() for each future blocks returning
            # from this method until all the jobs have executed or
            # raised an error.  This is needed for running in a
            # non-interactive python process that would otherwise end
            # before the futures resolve.
            for ff in futures:
                if ff is not None:
                    ff.exception()
            self.shutdown()
            self.finalize_jobs()
        return futures

    def execute(self, name: str) -> Future | None:
        """Execute a job.

        Parameters
        ----------
        name : `str`
            Name of job to execute.

        Returns
        -------
        future : `Future` or `None`
            A `Future` object linked to the execution of the job, or `None` if
            the job is being reserved to run locally.
        """
        if name in ("pipetaskInit", "mergeExecutionButler"):
            # These get done outside of parsl
            return None
        job = self.jobs[name]
        inputs = [self.execute(parent) for parent in self.parents[name]]
        executors = self.parsl_config.executors
        if len(executors) > 1:
            label = self.site.select_executor(job)
        else:
            label = executors[0].label
        return job.get_future(
            self.apps[label],
            [ff for ff in inputs if ff is not None],
            self.command_prefix,
            self.site.add_resources,
        )

    def load_dfk(self):
        """Load data frame kernel.

        This starts parsl.
        """
        if self.dfk is not None:
            raise RuntimeError("Workflow has already started.")
        set_parsl_logging(self.bps_config)
        self.dfk = parsl.load(self.parsl_config)

    def start(self):
        """Start the workflow."""
        self.initialize_jobs()
        self.load_dfk()

    def restart(self):
        """Restart the workflow after interruption."""
        self.parsl_config.checkpoint_files = parsl.utils.get_last_checkpoint()
        self.load_dfk()

    def shutdown(self):
        """Shut down the workflow.

        This stops parsl.
        """
        if self.dfk is None:
            raise RuntimeError("Workflow not started.")
        self.dfk.cleanup()
        self.dfk = None
        parsl.DataFlowKernelLoader.clear()

    def initialize_jobs(self):
        """Run initial jobs.

        These jobs are run locally before any other jobs are submitted to
        parsl.

        This is used to set up the butler.
        """
        job = self.jobs.get("pipetaskInit", None)
        if job is not None:
            os.makedirs(os.path.join(self.path, "logs"))
            job.run_local()

    def finalize_jobs(self):
        """Run final jobs.

        These jobs are run locally after all other jobs are complete.

        This is used to merge the execution butler.
        """
        if self.final is not None and not self.final.done:
            self.final.run_local()
