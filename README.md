# ctrl_bps_parsl

[![pypi](https://img.shields.io/pypi/v/lsst-ctrl-bps-parsl.svg)](https://pypi.org/project/lsst-ctrl-bps-parsl/)
[![codecov](https://codecov.io/gh/lsst/ctrl_bps_parsl/branch/main/graph/badge.svg?token=YoPKBx96gw)](https://codecov.io/gh/lsst/ctrl_bps_parsl)

This package is a [Parsl](https://parsl-project.org)-based plugin for the [LSST](https://www.lsst.org) Batch Production Service (BPS) [execution framework](https://github.com/lsst/ctrl_bps).
It is intended to support running LSST `PipelineTask` jobs on high-performance computing (HPC) clusters.
Parsl includes [execution providers](https://parsl.readthedocs.io/en/stable/userguide/execution.html#execution-providers) that allow operation on batch systems typically used by HPC clusters, e.g., [Slurm](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html#parsl.providers.SlurmProvider), [PBS/Torque](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.TorqueProvider.html#parsl.providers.TorqueProvider) and [LSF](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.LSFProvider.html#parsl.providers.LSFProvider).
Parsl can also be configured to run on a single node using a [thread pool](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.ThreadPoolExecutor.html#parsl.executors.ThreadPoolExecutor), which is useful for testing and development.

This is a **Python 3 only** package (we assume Python 3.8 or higher).

Documentation will be available [here](https://pipelines.lsst.io/modules/lsst.ctrl.bps.parsl/index.html).

This software is dual licensed under the GNU General Public License (version 3 of the License, or (at your option) any later version, and also under a 3-clause BSD license.
Recipients may choose which of these licenses to use; please see the files gpl-3.0.txt and/or bsd_license.txt, respectively.
