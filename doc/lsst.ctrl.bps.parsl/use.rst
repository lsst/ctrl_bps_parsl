.. py:currentmodule:: lsst.ctrl.bps.parsl

###
Use
###

Use of this plugin by BPS is triggered through the `BPS configuration`_ file's ``wmsServiceClass`` entry, which you should set to ``lsst.ctrl.bps.parsl.ParslService``.

.. _BPS configuration: https://pipelines.lsst.io/modules/lsst.ctrl.bps/quickstart.html#defining-a-submission

The ``computeSite`` entry should be set to a value of your choice, representative of the computing site in use.
For example, I use ``local`` for running on a single machine, and ``tiger`` for running on the `Princeton Tiger cluster`_.
The site is then configured by settings under ``site.<computeSite>`` (this scheme allows simple switching between different sites, and different configurations of the same site).
A site should have a ``class`` entry, which is the fully-qualified python name of a subclass of `lsst.ctrl.bps.parsl.SiteConfig`.
Beyond that, the configuration of a site depends on the particular site configuration ``class`` chosen.
See the section on :ref:`lsst.ctrl.bps.parsl-sites` for details on available site configuration classes, and what configuration entries are available.

.. _Princeton Tiger cluster: https://researchcomputing.princeton.edu/systems/tiger

Here's an example BPS configuration file for running the `ci_hsc dataset`_:

.. _ci_hsc dataset: https://github.com/lsst/testdata_ci_hsc

.. code-block:: yaml

    pipelineYaml: "${DRP_PIPE_DIR}/pipelines/HSC/DRP-ci_hsc.yaml"
    wmsServiceClass: lsst.ctrl.bps.parsl.ParslService
    computeSite: tiger
    parsl:
      log_level: DEBUG
    site:
      local:
        class: lsst.ctrl.bps.parsl.sites.Local
        cores: 10
      tiger:
        class: lsst.ctrl.bps.parsl.sites.princeton.Tiger
        nodes: 1
        walltime: "0:59:00"  # Under the 1 hour limit for qos=tiger-test

Note that there are two sites configured:

1. ``local``, which uses the :ref:`lsst.ctrl.bps.parsl-local` site configuration with 10 cores; and
2. ``tiger``, which uses the :ref:`lsst.ctrl.bps.parsl-tiger` site configuration with a single node and almost 1 hour walltime.

It's currently configured (through ``computeSite``) to use the ``tiger`` site, but switching between these two is simply a matter of changing the ``computeSite`` value.

Configuration
=============

The following configuration settings can be used in configuring the plugin:

* ``parsl.log_level`` (`str`): logging level for Parsl; may be one of ``CRITICAL``, ``DEBUG``, ``ERROR``, ``FATAL``, ``INFO``, ``WARN``.
* ``project`` (`str`): project name; defaults to ``bps``.
* ``campaign`` (`str`): campaign name; defaults to the user name (which can also be set via the ``username`` setting).

The workflow job name is taken to be ``<project>.<campaign>``.

.. _lsst.ctrl.bps.parsl-sites:

Sites
=====

All sites respect the following settings (under ``site.<computeSite>``):

* ``commandPrefix`` (`str`): command(s) to use as a prefix to executing a job command on a worker.
* ``environment``` (`bool`): add bash commands that replicate the environment on the driver/submit machine?
* ``retries`` (`int`): number of times to retry a job that fails; defaults to 1.

The following sites are provided by the ctrl_bps_parsl package.


.. _lsst.ctrl.bps.parsl-local:

Local
-----

`lsst.ctrl.bps.parsl.sites.Local` uses a |ThreadPoolExecutor|_ to execute the workflow on the local machine.
Required settings are:

* ``cores`` (`int`): number of cores to use.

.. |ThreadPoolExecutor| replace:: ``ThreadPoolExecutor``
.. _ThreadPoolExecutor: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.ThreadPoolExecutor.html#parsl.executors.ThreadPoolExecutor


.. _lsst.ctrl.bps.parsl-slurm:

Slurm
-----

`lsst.ctrl.bps.parsl.sites.Slurm` uses a |HighThroughputExecutor|_ and |SlurmProvider|_ to execute the workflow on a `Slurm`_ cluster.
This class can be used directly by providing the necessary values in the BPS configuration, or by subclasssing and setting values in the subclass.
When used directly, required settings are:

* ``nodes`` (`int`): number of nodes for each Slurm job.
* ``walltime`` (`str`): time limit for each Slurm job.

.. caution::
   ``walltime`` colon-delimited values should always be enclosed in double-quotes, to avoid YAML parsing them differently than you intend.

Optional settings are:

* ``cores_per_node`` (`int`): number of cores per node for each Slurm job; by default we use all cores on the node.
* ``mem_per_node`` (`int`): memory per node (GB) for each Slurm job; by default we use whatever Slurm gives us.
* ``qos`` (`str`): quality of service to request for each Slurm job; by default we use whatever Slurm gives us.
* ``scheduler_options`` (`str`): text to prepend to the Slurm submission script (each line usually starting with ``#SBATCH``); empty string by default.

.. |HighThroughputExecutor| replace:: ``HighThroughputExecutor``
.. _HighThroughputExecutor: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html#parsl.executors.HighThroughputExecutor
.. |SlurmProvider| replace:: ``SlurmProvider``
.. _SlurmProvider: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html#parsl.providers.SlurmProvider
.. _Slurm: https://www.schedmd.com


.. _lsst.ctrl.bps.parsl-tripleslurm:

TripleSlurm
-----------

`lsst.ctrl.bps.parsl.sites.TripleSlurm` uses three |HighThroughputExecutors|_ and |SlurmProviders|_ to execute the workflow on a `Slurm`_ cluster.
The ``small``, ``medium`` and ``large`` executors may have different memory limits, allowing jobs to be sent to different allocations depending upon their requirements.
This class can be used directly by providing the necessary values in the BPS configuration, or by subclasssing and setting values in the subclass.
The ``TripleSlurm`` site respects the same settings as for :ref:`lsst.ctrl.bps.parsl-slurm` (except for ``walltime``), plus the following optional settings:

* ``small_memory`` (`float`): memory per worker (GB) for each 'small' Slurm job (default: 2.0).
* ``medium_memory`` (`float`): memory per worker (GB) for each 'medium' Slurm job (default: 4.0).
* ``large_memory`` (`float`): memory per worker (GB) for each 'large' Slurm job (default: 8.0).
* ``small_walltime`` (`str`): time limit for each 'small' Slurm job (default: 10 hours).
* ``medium_walltime`` (`str`): time limit for each 'medium' Slurm job (default: 10 hours).
* ``large_walltime`` (`str`): time limit for each 'large' Slurm job (default: 40 hours).

Specifying ``walltime`` (as for the ``Slurm`` site) would override the individual ``small_walltime``, ``medium_walltime`` and ``large_walltime`` values.

.. warning::
   All the ``*_walltime`` colon-delimited values should always be enclosed in double-quotes, to avoid YAML parsing them differently than you intend.

.. |HighThroughputExecutors| replace:: ``HighThroughputExecutor``\ s
.. _HighThroughputExecutors: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html#parsl.executors.HighThroughputExecutor
.. |SlurmProviders| replace:: ``SlurmProvider``\ s
.. _SlurmProviders: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html#parsl.providers.SlurmProvider
.. _Slurm: https://www.schedmd.com


.. _lsst.ctrl.bps.parsl-tiger:

Tiger
-----

`lsst.ctrl.bps.parsl.sites.princeton.Tiger` is intended for use with the `Princeton Tiger cluster`_.
It subclasses :ref:`lsst.ctrl.bps.parsl-slurm` and adds some suitable customisation.
By default, a block of 4 nodes of 40 cores each run while another block waits in the queue.
Optional settings are:

* ``nodes`` (`int`): number of nodes for each Slurm job.
* ``cores_per_node`` (`int`): number of cores per node for each Slurm job.
* ``walltime`` (`str`): time limit for each Slurm job.
* ``mem_per_node`` (`int`): memory per node (GB) for each Slurm job.
* ``max_blocks`` (`int`): number of blocks (Slurm jobs) to use; one will execute while the others wait.

.. _Princeton Tiger cluster: https://researchcomputing.princeton.edu/systems/tiger


CoriKnl
-------

`lsst.ctrl.bps.parsl.sites.nersc.CoriKnl` is intended for use with the `NERSC Cori-KNL cluster`_.
It subclasses :ref:`lsst.ctrl.bps.parsl-tripleslurm` and adds some customisation.
Required and optional settings are the same as for :ref:`lsst.ctrl.bps.parsl-tripleslurm`.

.. _NERSC Cori-KNL cluster: https://docs.nersc.gov/performance/io/knl/


Sdf
---

`lsst.ctrl.bps.parsl.sites.slac.Sdf` is intended to be used with the Rubin partition at the `SLAC Shared Scientific Data Facility (SDF)`_.  It subclasses :ref:`lsst.ctrl.bps.parsl-slurm` and adds some suitable customisation.  By default, a block of 1 node of 100 cores runs while another block waits in the queue.
Optional settings are:

* ``nodes`` (`int`): number of nodes for each Slurm job.
* ``cores_per_node``` (`int`): number of cores per node for each Slurm job.
* ``walltime`` (`str`): time limit for each Slurm job.
* ``mem_per_node`` (`int`): memory per node (GB) for each Slurm job.
* ``max_blocks`` (`int`): number of blocks (Slurm jobs) to use; one will execute while the others wait.

.. _SLAC Shared Scientific Data Facility (SDF): https://sdf.slac.stanford.edu/public/doc/


LocalSrunWorkQueue
------------------

`lsst.ctrl.bps.parsl.sites.work_queue.LocalSrunWorkQueue` uses a |LocalProvider|_ with a |WorkQueueExecutor|_ to manage resources on single- or multi-node allocations.  For multi-node allocations, Slurm's |srun|_ command is used to launch jobs via an |SrunLauncher|_.  This implementation uses the |work_queue|_ module to schedule jobs with specific resource requests (e.g., memory, cpus, wall time, disk space), taking into account the available resources on the nodes.

.. |LocalProvider| replace:: ``LocalProvider``
.. _LocalProvider: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.LocalProvider.html
.. |WorkQueueExecutor| replace:: ``WorkQueueExecutor``
.. _WorkQueueExecutor: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.WorkQueueExecutor.html
.. |srun| replace:: ``srun``
.. _srun: https://slurm.schedmd.com/srun.html
.. |SrunLauncher| replace:: ``SrunLauncher``
.. _SrunLauncher: https://parsl.readthedocs.io/en/stable/stubs/parsl.launchers.SrunLauncher.html
.. |work_queue| replace:: ``work_queue``
.. _work_queue: https://cctools.readthedocs.io/en/stable/work_queue


Adding a site
=============

You don't need to use a site configuration that's already contained in the ctrl_bps_parsl package.
You can write a subclass of `lsst.ctrl.bps.parsl.SiteConfig`, define the two abstract methods (:py:meth:`~lsst.ctrl.bps.parsl.SiteConfig.get_executors` and :py:meth:`~lsst.ctrl.bps.parsl.SiteConfig.select_executor`), and override any other methods that need to be customised.
You should place your ``SiteConfig`` subclass somewhere on your ``PYTHONPATH``, and then set the ``site.<computeSite>.class`` to the fully-qualified name of your ``SiteConfig`` subclass.

If you think your site configuration might be of use to others, we can incorporate it into ctrl_bps_parsl; please file an `issue on GitHub`_.

.. _issue on GitHub: https://github.com/lsst/ctrl_bps_parsl/issues


Monitoring
==========

Turning on `Parsl's monitoring`_ feature allows tracking the progress of the workflow using a web browser.
The site settings that support monitoring are:

* ``monitorEnable`` (`bool`): enable monitor? Defaults to ``false``.
* ``monitorInterval`` (`float`): time interval (sec) between logging of resource usage. Defaults to 30.
* ``monitorFilename`` (`str`): name of file to use for the monitor sqlite database. Defaults to ``monitor.sqlite``.

.. _Parsl's monitoring: https://parsl.readthedocs.io/en/stable/userguide/monitoring.html

Once the workflow is running, point the ``parsl-visualize`` executable to the monitoring database, e.g.:

.. code-block:: bash

    parsl-visualize sqlite:////path/to/monitor.sqlite

.. note::
   Yes, that's four slashes!

Then you can point your web browser to the machine serving the visualisation, on the default port of 8080.
You will likely have to use an ``ssh`` tunnel to expose that port, e.g.:

.. code-block:: bash

    ssh -L 8080:localhost:8080 username@headnode

Then you can point your browser to ``localhost:8080``.
