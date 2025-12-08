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


import platform

from parsl.executors import HighThroughputExecutor, WorkQueueExecutor

from lsst.ctrl.bps import BpsConfig
from lsst.ctrl.bps.parsl.sites import Ccin2p3, Local, LocalSrunWorkQueue, Slurm, Torque


def testSiteResourceLists():
    """Test compute site resource lists."""
    # Provide a minimal config that allows compute site classes to be created.
    config = BpsConfig(
        {
            "submitPath": ".",
            "operator": "operator",
            "computeSite": "local",
            "site": {
                "local": {
                    "class": "lsst.ctrl.bps.parsl.sites.Local",
                    "cores": 1,
                    "nodes": 1,
                    "walltime": "00:01:00",
                }
            },
        }
    )

    expected_resources = {
        HighThroughputExecutor: {"priority"},
        WorkQueueExecutor: {"memory", "cores", "disk", "running_time_min", "priority"},
    }

    site_classes = [Local, Slurm, Torque]
    if platform.machine() in ("aarch64", "x86_64"):
        # If running on a supported architecture, then add Ccin2p3 site class.
        site_classes.append(Ccin2p3)

    try:
        # If the ndcctools module is available, then add WorkQueue
        # subclasses to the site_classes list.
        import ndcctools  # noqa F401

        site_classes.append(LocalSrunWorkQueue)
    except ImportError:
        pass

    for site_class in site_classes:
        compute_site = site_class(config)
        executor_type = type(compute_site.get_executors()[0])
        assert expected_resources[executor_type] == set(compute_site.resource_list)
