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

from abc import ABC, abstractmethod
from types import ModuleType
from typing import TYPE_CHECKING

from lsst.ctrl.bps import BpsConfig
from lsst.utils import doImport
from parsl.addresses import address_by_hostname
from parsl.executors.base import ParslExecutor
from parsl.monitoring import MonitoringHub

from .configuration import get_bps_config_value, get_workflow_name
from .environment import export_environment

if TYPE_CHECKING:
    from .job import ParslJob

__all__ = ("SiteConfig",)


class SiteConfig(ABC):
    """Base class for site configuration

    Subclasses need to override at least the ``get_executors`` and
    ``select_executor`` methods.

    Parameters
    ----------
    config : `BpsConfig`
        BPS configuration.
    add_resources : `bool`
        Add resource specification when submitting the job? This is only
        appropriate for the ``WorkQueue`` executor; other executors will
        raise an exception.
    """

    def __init__(self, config: BpsConfig, add_resources: bool = False):
        self.config = config
        self.site = self.get_site_subconfig(config)
        self.add_resources = add_resources

    @staticmethod
    def get_site_subconfig(config: BpsConfig) -> BpsConfig:
        """Get BPS configuration for the site of interest

        We return the BPS sub-configuration for the site indicated by the
        ``computeSite`` value, which is ``site.<computeSite>``.

        Parameters
        ----------
        config : `BpsConfig`
            BPS configuration.

        Returns
        -------
        site : `BpsConfig`
            Site sub-configuration.
        """
        computeSite = get_bps_config_value(config, "computeSite", str, required=True)
        return get_bps_config_value(config, f".site.{computeSite}", BpsConfig, required=True)

    @classmethod
    def from_config(cls, config: BpsConfig) -> "SiteConfig":
        """Get the site configuration nominated in the BPS config

        The ``computeSite`` (`str`) value in the BPS configuration is used to
        select a site configuration. The site configuration class to use is
        specified by the BPS configuration as ``site.<computeSite>.class``
        (`str`), which should be the fully-qualified name of a python class
        that inherits from `SiteConfig`.

        Parameters
        ----------
        config : `BpsConfig`
            BPS configuration.

        Returns
        -------
        site_config : subclass of `SiteConfig`
            Site configuration.
        """
        site = cls.get_site_subconfig(config)
        name = get_bps_config_value(site, "class", str, required=True)
        site_config = doImport(name)
        if isinstance(site_config, ModuleType) or not issubclass(site_config, SiteConfig):
            raise RuntimeError(f"Site class={name} is not a SiteConfig subclass")
        return site_config(config)

    @abstractmethod
    def get_executors(self) -> list[ParslExecutor]:
        """Get a list of executors to be used in processing

        Each executor should have a unique ``label``.
        """
        raise NotImplementedError("Subclasses must define")

    @abstractmethod
    def select_executor(self, job: "ParslJob") -> str:
        """Get the ``label`` of the executor to use to execute a job

        Parameters
        ----------
        job : `ParslJob`
            Job to be executed.

        Returns
        -------
        label : `str`
            Label of executor to use to execute ``job``.
        """
        raise NotImplementedError("Subclasses must define")

    def get_address(self) -> str:
        """Return the IP address of the machine hosting the driver/submission

        This address should be accessible from the workers. This should
        generally by the return value of one of the functions in
        ``parsl.addresses``.

        This is used by the default implementation of ``get_monitor``, but will
        generally be used by ``get_executors`` too.

        This default implementation gets the address from the hostname, but
        that will not work if the workers don't access the driver/submission
        node by that address.
        """
        return address_by_hostname()

    def get_command_prefix(self) -> str:
        """Return command(s) to add before each job command

        These may be used to configure the environment for the job.

        This default implementation respects the BPS configuration elements:

        - ``site.<computeSite>.commandPrefix`` (`str`): command(s) to use as a
          prefix to executing a job command on a worker.
        - ``site.<computeSite>.environment`` (`bool`): add bash commands that
          replicate the environment on the driver/submit machine?
        """
        prefix = get_bps_config_value(self.site, "commandPrefix", str, "")
        if get_bps_config_value(self.site, "environment", bool, False):
            prefix += "\n" + export_environment()
        return prefix

    def get_monitor(self) -> MonitoringHub | None:
        """Get parsl monitor

        The parsl monitor provides a database that tracks the progress of the
        workflow and the use of resources on the workers.

        This implementation respects the BPS configuration elements:

        - ``site.<computeSite>.monitorEnable`` (`bool`): enable monitor?
        - ``site.<computeSite>.monitorInterval`` (`float`): time interval (sec)
          between logging of resource usage.
        - ``site.<computeSite>.monitorFilename`` (`str`): name of file to use
          for the monitor sqlite database.

        Returns
        -------
        monitor : `MonitoringHub` or `None`
            Parsl monitor, or `None` for no monitor.
        """
        if not get_bps_config_value(self.site, "monitorEnable", bool, False):
            return None
        return MonitoringHub(
            workflow_name=get_workflow_name(self.config),
            hub_address=self.get_address(),
            resource_monitoring_interval=get_bps_config_value(self.site, "monitorInterval", float, 30.0),
            logging_endpoint="sqlite:///"
            + get_bps_config_value(self.site, "monitorFilename", str, "monitor.sqlite"),
        )
