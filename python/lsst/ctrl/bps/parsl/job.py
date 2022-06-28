import os
import re
import subprocess
from functools import partial
from typing import Callable, Dict, List, Optional, Sequence

from lsst.ctrl.bps import BpsConfig, GenericWorkflow, GenericWorkflowJob
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
        log_dir = os.path.join(get_bps_config_value(self.config, "submitPath"), "logs")
        self.stdout = os.path.join(log_dir, self.name + ".stdout")
        self.stderr = os.path.join(log_dir, self.name + ".stderr")

    def __reduce__(self):
        """Recipe for pickling"""
        return type(self), (self.generic, self.config, self.file_paths)

    def get_command_line(self) -> str:
        """Get the bash command-line to run to execute this job"""
        command = [
            self.generic.executable.src_uri,
            self.generic.arguments,
        ]
        return " ".join(command)

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

    def get_future(
        self,
        app: Callable[[Callable[[str, Sequence[Future], Optional[str], Optional[str]], str]], Future],
        inputs: List[Future],
        command_prefix: Optional[str] = None,
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

            # Add a layer of indirection to which we can add a useful name.
            # This name is used by parsl for tracking workflow status.
            func = partial(run_command)
            setattr(func, "__name__", self.generic.label)

            self.future = app(func)(command, inputs=inputs, stdout=self.stdout, stderr=self.stderr)
        return self.future

    def run_local(self):
        """Run the command locally

        This is intended to support jobs that should not be done by a
        worker.
        """
        if self.done:  # Nothing to do
            return
        command = self.get_command_line()
        command = self.evaluate_command_line(command)
        with open(self.stdout, "w") as stdout, open(self.stderr, "w") as stderr:
            subprocess.check_call(command, shell=True, executable="/bin/bash", stdout=stdout, stderr=stderr)
        self.done = True
