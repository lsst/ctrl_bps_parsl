.. py:currentmodule:: lsst.ctrl.bps.parsl

.. _lsst.ctrl.bps.parsl:

###################
lsst.ctrl.bps.parsl
###################

.. _lsst.ctrl.bps.parsl-changes:

Changes
=======

.. toctree::
   :maxdepth: 1

   CHANGES.rst


.. _lsst.ctrl.bps.parsl-intro:

Introduction
============

This package is a `Parsl`_-based plugin for the `LSST`_ Batch Production Service (BPS) execution framework (:ref:`lsst.ctrl.bps`).
It is intended to support running LSST `~lsst.pipe.base.PipelineTask` jobs on high-performance computing (HPC) clusters.
Parsl includes `execution providers`_ that allow operation on batch systems typically used by HPC clusters, e.g., `Slurm`_, `PBS/Torque`_ and `LSF`_.
Parsl can also be configured to run on a single node using a `thread pool`_, which is useful for testing and development.

.. _Parsl: https://parsl-project.org
.. _LSST: https://www.lsst.org
.. _execution framework: https://github.com/lsst/ctrl_bps
.. _execution providers: https://parsl.readthedocs.io/en/stable/userguide/execution.html#execution-providers
.. _Slurm: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html#parsl.providers.SlurmProvider
.. _PBS/Torque: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.TorqueProvider.html#parsl.providers.TorqueProvider
.. _LSF: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.LSFProvider.html#parsl.providers.LSFProvider
.. _thread pool: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.ThreadPoolExecutor.html#parsl.executors.ThreadPoolExecutor


Note that while Parsl does provide means for `staging files for workers`_, these are not currently used by the ctrl_bps_parsl plugin.
Use of this plugin, therefore, is currently limited to environments where workers will have direct access to the `butler`_ files being processed.
If there is sufficient interest, this might change in the future, opening up the possibility of using this plugin to process data with cloud nodes, e.g., `AWS`_ and `GoogleCloud`_.


.. _staging files for workers: https://parsl.readthedocs.io/en/stable/userguide/data.html#staging-data-files
.. _butler: https://github.com/lsst/daf_butler
.. _AWS: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.AWSProvider.html#parsl.providers.AWSProvider
.. _GoogleCloud: _https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.GoogleCloudProvider.html#parsl.providers.GoogleCloudProvider


.. _lsst.ctrl.bps.parsl-software:

Software
========

This package is based on `the Parsl plugin`_ developed for the `LSST-DESC`_ `Data Challenge 2`_ by `Jim Chiang`_.
The plugin is now suitable for use in a variety of cluster environments and will be used by other projects, including the `Subaru Prime Focus Spectrograph`_ and the `Merian survey`_.

.. _the Parsl plugin: https://github.com/LSSTDESC/gen3_workflow
.. _LSST-DESC: https://lsstdesc.org
.. _Data Challenge 2: https://ui.adsabs.harvard.edu/abs/2021ApJS..253...31L/abstract
.. _Jim Chiang: https://kipac.stanford.edu/people/james-chiang
.. _Subaru Prime Focus Spectrograph: https://pfs.ipmu.jp
.. _Merian survey: https://merian.sites.ucsc.edu

This package is open source software, released under a `BSD license`_.
Contributions, including bug reports and feature requests, are welcome: please open an issue `on GitHub`_.

.. _BSD license: https://github.com/lsst/ctrl_bps_parsl/blob/main/LICENSE
.. _on GitHub: https://github.com/lsst/ctrl_bps_parsl/issues


.. _lsst.ctrl.bps.parsl-installation:

Installation
============

.. toctree::
  :maxdepth: 1

  installation.rst


.. _lsst.ctrl.bps.parsl-use:

Use
===

.. toctree::
  :maxdepth: 1

  use.rst


.. _lsst.ctr.bps.parsl-pyapi:

Python API reference
====================

.. automodapi:: lsst.ctrl.bps.parsl
   :no-main-docstr:
   :no-heading:
   :no-inheritance-diagram:
   :headings: ^"


Base and general-purpose sites
------------------------------

The following are base classes that can also be used as general-purpose site configurations.

.. automodapi:: lsst.ctrl.bps.parsl.sites
   :no-main-docstr:
   :no-heading:
   :headings: ^"


Specific sites
--------------

The following are intended to support operations at specific sites or by specific groups.

.. automodapi:: lsst.ctrl.bps.parsl.sites.nersc
   :no-main-docstr:
   :no-heading:
   :headings: ^"
   :no-inheritance-diagram:

.. automodapi:: lsst.ctrl.bps.parsl.sites.princeton
   :no-main-docstr:
   :no-heading:
   :headings: ^"
   :no-inheritance-diagram:

.. automodapi:: lsst.ctrl.bps.parsl.sites.slac
   :no-main-docstr:
   :no-heading:
   :headings: ^"
   :no-inheritance-diagram:


Environment utility functions
-----------------------------

.. automodapi:: lsst.ctrl.bps.parsl.environment
   :no-heading:
   :headings: ^"
   :no-main-docstr:

