import os
import re
import subprocess
from functools import partial
from textwrap import dedent
from typing import Any, Dict, List, Optional, Sequence

from lsst.ctrl.bps import BpsConfig, GenericWorkflow, GenericWorkflowJob
from parsl.app.bash import BashApp
from parsl.app.futures import Future

from .configuration import get_bps_config_value

__all__ = ("get_file_paths", "ParslJob")

_env_regex = re.compile(r"<ENV:(\S+)>")  # Regex for replacing <ENV:WHATEVER> in BPS job command-lines
_file_regex = re.compile(r"<FILE:(\S+)>")  # Regex for replacing <FILE:WHATEVER> in BPS job command-lines


def run_command(
    command_line: str,
    inputs: Sequence[Future] = (),
    stdout: Optional[str] = None,
    stderr: Optional[str] = None,
    parsl_resource_specification: Optional[Dict[str, Any]] = None,
) -> str:
    """Run a command

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


def get_file_paths(workflow: GenericWorkflow, name: str) -> Dict[str, str]:
    """Extract file paths for a job

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
    """Job to execute with parsl

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
        file_paths: Dict[str, str],
    ):
        self.generic = generic
        self.name = generic.name
        self.config = config
        self.file_paths = file_paths
        self.future = None
        self.done = False
        log_dir = os.path.join(get_bps_config_value(self.config, "submitPath", str, required=True), "logs")
        self.stdout = os.path.join(log_dir, self.name + ".stdout")
        self.stderr = os.path.join(log_dir, self.name + ".stderr")

    def __reduce__(self):
        """Recipe for pickling"""
        return type(self), (self.generic, self.config, self.file_paths)

    def get_command_line(self, allow_stage=True) -> str:
        """Get the bash command-line to run to execute this job

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
        """Evaluate the bash command-line

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
        command = re.sub(_env_regex, r"${\g<1>}", command)  # Environment variables
        command = re.sub(_file_regex, lambda match: self.file_paths[match.group(1)], command)  # Files
        return command

    def get_resources(self) -> Dict[str, Any]:
        """Return what resources are required for executing this job"""
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
        inputs: List[Future],
        command_prefix: Optional[str] = None,
        add_resources: bool = False,
    ) -> Optional[Future]:
        """Get the parsl app future for the job

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
            resources = self.get_resources() if add_resources else None

            # Add a layer of indirection to which we can add a useful name.
            # This name is used by parsl for tracking workflow status.
            func = partial(run_command)
            setattr(func, "__name__", self.generic.label)

            self.future = app(func)(
                command,
                inputs=inputs,
                stdout=self.stdout,
                stderr=self.stderr,
                parsl_resource_specification=resources,
            )
        return self.future

    def run_local(self):
        """Run the command locally

        This is intended to support jobs that should not be done by a
        worker.
        """
        if self.done:  # Nothing to do
            return
        command = self.get_command_line(False)
        command = self.evaluate_command_line(command)
        with open(self.stdout, "w") as stdout, open(self.stderr, "w") as stderr:
            subprocess.check_call(command, shell=True, executable="/bin/bash", stdout=stdout, stderr=stderr)
        self.done = True
