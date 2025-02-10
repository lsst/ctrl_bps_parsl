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
import re
import subprocess
from collections import defaultdict
from collections.abc import Sequence
from functools import partial
from textwrap import dedent
from typing import Any

from parsl.app.bash import BashApp
from parsl.app.futures import Future

from lsst.ctrl.bps import BpsConfig, GenericWorkflow, GenericWorkflowJob

from .configuration import get_bps_config_value

__all__ = ("ParslJob", "get_file_paths")

_env_regex = re.compile(r"<ENV:(\S+)>")  # Regex for replacing <ENV:WHATEVER> in BPS job command-lines
_file_regex = re.compile(r"<FILE:(\S+)>")  # Regex for replacing <FILE:WHATEVER> in BPS job command-lines


def run_command(
    command_line: str,
    inputs: Sequence[Future] = (),
    stdout: str | None = None,
    stderr: str | None = None,
    parsl_resource_specification: dict[str, Any] | None = None,
) -> str:
    """Run a command.

    This function exists to get information into parsl, through the ``inputs``,
    ``stdout`` and ``stderr`` parameters. It needs to be wrapped by a parsl
    ``bash_app`` decorator before use, after which it will return a `Future`.

    Parameters
    ----------
    command_line : `str`
        Command-line to have parsl run.
    inputs : list of `Future`
        Other commands that must have run before this.
    stdout, stderr : `str`, optional
        Filenames for stdout and stderr.
    parsl_resource_specification : `dict`, optional
        Resources required for job.

    Returns
    -------
    command_line : `str`
        Command-line to have parsl run.
    """
    return command_line


def get_file_paths(workflow: GenericWorkflow, name: str) -> dict[str, str]:
    """Extract file paths for a job.

    Parameters
    ----------
    workflow : `GenericWorkflow`
        BPS workflow that knows the file paths.
    name : `str`
        Job name.

    Returns
    -------
    paths : `dict` mapping `str` to `str`
        File paths for job, indexed by symbolic name.
    """
    return {ff.name: ff.src_uri for ff in workflow.get_job_inputs(name)}


class ParslJob:
    """Job to execute with parsl.

    Parameters
    ----------
    generic : `GenericWorkflowJob`
        BPS job information.
    config : `BpsConfig`
        BPS configuration.
    file_paths : `dict` mapping `str` to `str`
        File paths for job, indexed by symbolic name.
    """

    def __init__(
        self,
        generic: GenericWorkflowJob,
        config: BpsConfig,
        file_paths: dict[str, str],
    ):
        self.generic = generic
        self.name = generic.name
        self.config = config
        self.file_paths = file_paths
        self.future = None
        self.done = False

        # Determine directory for job stdout and stderr
        log_dir = os.path.join(get_bps_config_value(self.config, "submitPath", str, required=True), "logs")
        _, template = self.config.search(
            "subDirTemplate",
            opt={
                "curvals": {"curr_site": self.config["computeSite"], "curr_cluster": self.generic.label},
                "replaceVars": False,
                "default": "",
            },
        )
        job_vals = defaultdict(str)
        job_vals["label"] = self.generic.label
        if self.generic.tags:
            job_vals.update(self.generic.tags)
        subdir = template.format_map(job_vals)
        # Call normpath just to make paths easier to read as templates tend
        # to have variables that aren't used by every job.  Avoid calling on
        # empty string because changes it to dot.
        same_part = os.path.normpath(os.path.join(log_dir, subdir, self.name))
        self.stdout = same_part + ".stdout"
        self.stderr = same_part + ".stderr"

    def __reduce__(self):
        """Recipe for pickling."""
        return type(self), (self.generic, self.config, self.file_paths)

    def get_command_line(self, allow_stage=True) -> str:
        """Get the bash command-line to run to execute this job.

        Parameters
        ----------
        allow_stage : `bool`
            Allow staging of execution butler? This is not appropriate for the
            initial or final jobs that run on the local nodes.

        Returns
        -------
        command : `str`
            Command-line to execute for job.
        """
        command: str = self.generic.executable.src_uri + " " + self.generic.arguments
        if not allow_stage:
            return command
        exec_butler_dir = get_bps_config_value(self.config, "executionButlerDir", str)
        if not exec_butler_dir or not os.path.isdir(exec_butler_dir):
            # We're not using the execution butler
            return command

        # Add commands to copy the execution butler.
        # This keeps workers from overloading the sqlite database.
        # The copy can be deleted once we're done, because the original
        # execution butler contains everything that's required.
        job_dir = os.path.join(os.path.dirname(exec_butler_dir), self.name)
        # Set the butlerConfig field to the location of the job-specific copy.
        command = command.replace("<FILE:butlerConfig>", job_dir)
        return dedent(
            f"""
            if [[ ! -d {job_dir} ]]; then mkdir -p {job_dir}; fi
            cp {exec_butler_dir}/* {job_dir}
            {command}
            retcode=$?
            rm -rf {job_dir}
            exit $retcode
            """
        )

    def evaluate_command_line(self, command: str) -> str:
        """Evaluate the bash command-line.

        BPS provides a command-line with symbolic names for BPS variables,
        environment variables and files. Here, we replace those symbolic names
        with the actual values, to provide a concrete command that can be
        executed.

        In replacing file paths, we are implicitly assuming that we are working
        on a shared file system, i.e., that workers can see the butler
        directory, and that files do not need to be staged to the worker.

        Parameters
        ----------
        command : `str`
            Command-line to execute, from BPS.

        Returns
        -------
        command : `str`
            Command ready for execution on a worker.
        """
        command = command.format(**self.generic.cmdvals)  # BPS variables

        # Make sure *all* symbolic names are resolved.
        #
        # In general, actual values for some symbolic names may contain other
        # symbolic names. As a result, more than one iteration may be required
        # to resolve all symbolic names.  For example, an actual value for
        # a filename may contain a symbolic name for an environment variable.
        prev_command = command
        while True:
            command = re.sub(_env_regex, r"${\g<1>}", command)  # Environment variables
            command = re.sub(_file_regex, lambda match: self.file_paths[match.group(1)], command)  # Files
            if prev_command == command:
                break
            prev_command = command

        return command

    def get_resources(self) -> dict[str, Any]:
        """Return what resources are required for executing this job."""
        resources = {}
        for bps_name, parsl_name, scale in (
            ("request_memory", "memory", None),  # Both BPS and WorkQueueExecutor use MB
            ("request_cpus", "cores", None),
            ("request_disk", "disk", None),  # Both are MB
            ("request_walltime", "running_time_min", None),  # Both are minutes
        ):
            value = getattr(self.generic, bps_name)
            if scale is not None:
                value *= scale
            resources[parsl_name] = value
        return resources

    def get_future(
        self,
        app: BashApp,
        inputs: list[Future],
        command_prefix: str | None = None,
        add_resources: bool = False,
    ) -> Future | None:
        """Get the parsl app future for the job.

        This effectively queues the job for execution by a worker, subject to
        dependencies.

        Parameters
        ----------
        app : callable
            A parsl bash_app decorator to use.
        inputs : list of `Future`
            Dependencies to be satisfied before executing this job.
        command_prefix : `str`, optional
            Bash commands to execute before the job command, e.g., for setting
            the environment.
        add_resources : `bool`
            Add resource specification when submitting the job? This is only
            appropriate for the ``WorkQueue`` executor; other executors will
            raise an exception.

        Returns
        -------
        future : `Future` or `None`
            A `Future` object linked to the execution of the job, or `None` if
            the job has already been done (e.g., by ``run_local``).
        """
        if self.done:
            return None  # Nothing to do
        if not self.future:
            command = self.get_command_line()
            command = self.evaluate_command_line(command)
            if command_prefix:
                command = command_prefix + "\n" + command
            resources = self.get_resources() if add_resources else {}

            # Add a layer of indirection to which we can add a useful name.
            # This name is used by parsl for tracking workflow status.
            func = partial(run_command)
            func.__name__ = self.generic.label  # type: ignore

            self.future = app(func)(
                command,
                inputs=inputs,
                stdout=self.stdout,
                stderr=self.stderr,
                parsl_resource_specification=resources,
            )
        return self.future

    def run_local(self):
        """Run the command locally.

        This is intended to support jobs that should not be done by a
        worker.
        """
        if self.done:  # Nothing to do
            return
        command = self.get_command_line(False)
        command = self.evaluate_command_line(command)
        os.makedirs(os.path.dirname(self.stdout), exist_ok=True)
        os.makedirs(os.path.dirname(self.stderr), exist_ok=True)
        with open(self.stdout, "w") as stdout, open(self.stderr, "w") as stderr:
            subprocess.check_call(command, shell=True, executable="/bin/bash", stdout=stdout, stderr=stderr)
        self.done = True
