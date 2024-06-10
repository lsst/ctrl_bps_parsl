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

from lsst.ctrl.bps import BpsConfig, GenericWorkflowJob
from lsst.ctrl.bps.parsl.job import ParslJob

TESTDIR = os.path.abspath(os.path.dirname(__file__))


def testInitWithTemplate():
    """Test ParslJob stdout/stderr init. with template."""
    # Test doesn't actually use directory
    submit_path = os.path.join(TESTDIR, "with_template")
    config = BpsConfig(
        {
            "subDirTemplate": "{label}/{tract}/{patch}/{band}/{visit}/{exposure}",
            "submitPath": submit_path,
        },
        search_order=[],
    )
    gwjob = GenericWorkflowJob("job1", label="label1")
    gwjob.tags = {"exposure": 903344}

    parsl_job = ParslJob(gwjob, config, {})
    assert parsl_job.stdout == os.path.join(submit_path, "logs/label1/903344/job1.stdout")
    assert parsl_job.stderr == os.path.join(submit_path, "logs/label1/903344/job1.stderr")


def testInitWithTemplateNoTags():
    """Test ParslJob stdout/stderr init. with template but without tags."""
    # Test doesn't actually use directory
    submit_path = os.path.join(TESTDIR, "with_template_no_tags")
    config = BpsConfig(
        {
            "subDirTemplate": "{label}/{tract}/{patch}/{band}/{visit}/{exposure}",
            "submitPath": submit_path,
        },
        search_order=[],
    )
    gwjob = GenericWorkflowJob("job2", label="label2")
    gwjob.tags = None

    parsl_job = ParslJob(gwjob, config, {})
    assert parsl_job.stdout == os.path.join(submit_path, "logs/label2/job2.stdout")
    assert parsl_job.stderr == os.path.join(submit_path, "logs/label2/job2.stderr")


def testInitWithoutTemplate():
    """Test ParslJob stdout/stderr init. with template."""
    # Test doesn't actually use directory
    submit_path = os.path.join(TESTDIR, "without_template")
    config = BpsConfig({"subDirTemplate": "", "submitPath": submit_path}, search_order=[])
    gwjob = GenericWorkflowJob("job3", label="label3")
    gwjob.tags = {"exposure": 903344}

    parsl_job = ParslJob(gwjob, config, {})
    assert parsl_job.stdout == os.path.join(submit_path, "logs/job3.stdout")
    assert parsl_job.stderr == os.path.join(submit_path, "logs/job3.stderr")
