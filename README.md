# ctrl_bps_parsl

This package is a [Parsl](https://parsl-project.org)-based plugin for the [LSST](https://www.lsst.org) Batch Production Service (BPS) [execution framework](https://github.com/lsst/ctrl_bps).
It is intended to support running LSST `PipelineTask` jobs on high-performance computing (HPC) clusters.
Parsl includes [execution providers](https://parsl.readthedocs.io/en/stable/userguide/execution.html#execution-providers) that allow operation on batch systems typically used by HPC clusters, e.g., [Slurm](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html#parsl.providers.SlurmProvider), [PBS/Torque](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.TorqueProvider.html#parsl.providers.TorqueProvider) and [LSF](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.LSFProvider.html#parsl.providers.LSFProvider).
Parsl can also be configured to run on a single node using a [thread pool](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.ThreadPoolExecutor.html#parsl.executors.ThreadPoolExecutor), which is useful for testing and development.

Note that while Parsl does provide means for [staging files for workers](https://parsl.readthedocs.io/en/stable/userguide/data.html#staging-data-files), these are not currently used by the ctrl_bps_parsl plugin.
Use of this plugin, therefore, is currently limited to environments where workers will have direct access to the [butler](https://github.com/lsst/daf_butler) files being processed.
If there is sufficient interest, this might change in the future, opening up the possibility of using this plugin to process data with cloud nodes, e.g., [AWS](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.AWSProvider.html#parsl.providers.AWSProvider) and [GoogleCloud](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.GoogleCloudProvider.html#parsl.providers.GoogleCloudProvider).

## History

This package is based on [the Parsl plugin](https://github.com/LSSTDESC/gen3_workflow) developed for the [LSST-DESC](https://lsstdesc.org) [Data Challenge 2](https://ui.adsabs.harvard.edu/abs/2021ApJS..253...31L/abstract) by [Jim Chiang](https://kipac.stanford.edu/people/james-chiang).
The plugin is now suitable for use in a variety of cluster environments and will be used by other projects, including the [Subaru Prime Focus Spectrograph](https://pfs.ipmu.jp) and the [Merian survey](https://merian.sites.ucsc.edu).

## Open source software

This package is open source software, released under the BSD license.
Contributions, including bug reports and feature requests, are welcome: please open an issue on GitHub.

## Installation

### Parsl

This plugin requires [Parsl](https://parsl-project.org), which may be installed into the LSST stack:

    mamba install --no-update-deps parsl

### Parsl with monitoring support

The [`parsl-visualize` executable](https://parsl.readthedocs.io/en/stable/userguide/monitoring.html#visualization) is a web app for monitoring the progress of the Parsl workflow.
It has extra dependencies that are (as of the time of writing), [formally (but not practically) incompatible](https://github.com/Parsl/parsl/issues/2296) with the LSST butler (which requires sqlalchemy>=1.4).
Attempting to install the `parsl-with-monitoring` conda package with `--no-update-deps` results in a failure, and without `-no-update-deps` results in a broken butler.
The solution is a little complicated.

First, install the necessary dependencies:

    mamba install --only-deps parsl-with-monitoring

This will downgrade sqlalchemy (e.g., to 1.3.24).
We need to undo this in order for the LSST butler to work:

    mamba upgrade sqlalchemy=1.4.36

Now we need to uninstall `parsl` (installed as a dependency of `parsl-with-monitoring`), and install its dependencies:

    mamba uninstall parsl
    mamba install --only-deps parsl

Now we have all the necessary dependencies but not `parsl` itself.
To avoid `conda` or `pip` dependency problems in parsl, we install it directly from source:

    git clone https://github.com/parsl/parsl.git
    cd parsl
    python setup.py install --user

Hopefully the next parsl release won't have this problem.

### Plugin

Clone this package from [GitHub](https://github.com/lsst/ctrl_bps_parsl), set it up and build:

    git clone https://github.com/lsst/ctrl_bps_parsl
    cd ctrl_bps_parsl
    setup -kr .
    scons

To use this plugin, you'll need to set it up in your environment, as you do for the rest of the LSST pipeline code:

    setup -kr /path/to/ctrl_bps_parsl

## Use

Use of this plugin by BPS is triggered through the [BPS configuration](https://pipelines.lsst.io/modules/lsst.ctrl.bps/quickstart.html#defining-a-submission) file's `wmsServiceClass` entry, which you should set to `lsst.ctrl.bps.parsl.ParslService`.

The `computeSite` entry should be set to a value of your choice, representative of the computing site in use.
For example, I use `local` for running on a single machine, and `tiger` for running on the [Princeton Tiger cluster](https://researchcomputing.princeton.edu/systems/tiger).
The site is then configured by settings under `site.<computeSite>` (this scheme allows simple switching between different sites, and different configurations of the same site).
A site should have a `class` entry, which is the fully-qualified python name of a subclass of `lsst.ctrl.bps.parsl.SiteConfig`.
Beyond that, the configuration of a site depends on the particular site configuration `class` chosen.
See the section on [Sites](#sites) for details on available site configuration classes, and what configuration entries are available.

Here's an example BPS configuration file for running the [ci_hsc dataset](https://github.com/lsst/testdata_ci_hsc):

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

1. `local`, which uses the [`Local`](#local) site configuration with 10 cores; and
2. `tiger`, which uses the [`Tiger`](#tiger) site configuration with a single node and almost 1 hour walltime.

It's currently configured (through `computeSite`) to use the `tiger` site, but switching between these two is simply a matter of changing the `computeSite` value.

### Configuration

The following configuration settings can be used in configuring the plugin:

* `parsl.log_level` (`str`): logging level for Parsl; may be one of `CRITICAL`, `DEBUG`, `ERROR`, `FATAL`, `INFO`, `WARN`.
* `project` (`str`): project name; defaults to `bps`.
* `campaign` (`str`): campaign name; defaults to the user name (which can also be set via the `username` setting).

The workflow job name is taken to be `<project>.<campaign>`.

## Sites

All sites respect the following settings (under `site.<computeSite>`):

* `commandPrefix` (`str`): command(s) to use as a prefix to executing a job command on a worker.
* `environment` (`bool`): add bash commands that replicate the environment on the driver/submit machine?
* `retries` (`int`): number of times to retry a job that fails; defaults to 1.

The following sites are provided by the ctrl_bps_parsl package.
### Local

`lsst.ctrl.bps.parsl.sites.Local` uses a [`ThreadPoolExecutor`](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.ThreadPoolExecutor.html#parsl.executors.ThreadPoolExecutor) to execute the workflow on the local machine.
Required settings are:

* `cores` (`int`): number of cores to use.

### Slurm

`lsst.ctrl.bps.parsl.sites.Slurm` uses a [`HighThroughputExecutor`](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html#parsl.executors.HighThroughputExecutor) and [`SlurmProvider`](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html#parsl.providers.SlurmProvider) to execute the workflow on a [Slurm](https://www.schedmd.com) cluster.
This class can be used directly by providing the necessary values in the BPS configuration, or by subclasssing and setting values in the subclass.
When used directly, required settings are:

* `nodes` (`int`): number of nodes for each Slurm job.
* `walltime` (`str`): time limit for each Slurm job.
  Note that `walltime` colon-delimited values should always be enclosed in double-quotes, to avoid YAML parsing them differently than you intend.

Optional settings are:

* `cores_per_node` (`int`): number of cores per node for each Slurm job; by default we use all cores on the node.
* `mem_per_node` (`int`): memory per node (GB) for each Slurm job; by default we use whatever Slurm gives us.
* `qos` (`str`): quality of service to request for each Slurm job; by default we use whatever Slurm gives us.


### TripleSlurm

`lsst.ctrl.bps.parsl.sites.TripleSlurm` uses three [`HighThroughputExecutor`s](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html#parsl.executors.HighThroughputExecutor) and [`SlurmProvider`s](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.SlurmProvider.html#parsl.providers.SlurmProvider) to execute the workflow on a [Slurm](https://www.schedmd.com) cluster.
The `small`, `medium` and `large` executors may have different memory limits, allowing jobs to be sent to different allocations depending upon their requirements.
This class can be used directly by providing the necessary values in the BPS configuration, or by subclasssing and setting values in the subclass.
The `TripleSlurm` site respects the same settings as for [`Slurm`](#slurm) (except for `walltime`), plus the following optional settings:

* `small_memory` (`float`): memory per worker (GB) for each 'small' Slurm job (default: 2.0).
* `medium_memory` (`float`): memory per worker (GB) for each 'medium' Slurm job (default: 4.0).
* `large_memory` (`float`): memory per worker (GB) for each 'large' Slurm job (default: 8.0).
* `small_walltime` (`str`): time limit for each 'small' Slurm job (default: 10 hours).
* `medium_walltime` (`str`): time limit for each 'medium' Slurm job (default: 10 hours).
* `large_walltime` (`str`): time limit for each 'large' Slurm job (default: 40 hours).

Specifying `walltime` (as for the `Slurm` site) would override the individual `small_walltime`, `medium_walltime` and `large_walltime` values.
Note that all the `*_walltime` colon-delimited values should always be enclosed in double-quotes, to avoid YAML parsing them differently than you intend.

### Tiger

`lsst.ctrl.bps.parsl.sites.princeton.Tiger` is intended for use with the [Princeton Tiger cluster](https://researchcomputing.princeton.edu/systems/tiger).
It subclasses [`Slurm`](#slurm) and adds some suitable customisation.
By default, a block of 4 nodes of 40 cores each run while another block waits in the queue.
Optional settings are:

* `nodes` (`int`): number of nodes for each Slurm job.
* `cores_per_node` (`int`): number of cores per node for each Slurm job.
* `walltime` (`str`): time limit for each Slurm job.
* `mem_per_node` (`int`): memory per node (GB) for each Slurm job.
* `max_blocks` (`int`): number of blocks (Slurm jobs) to use; one will execute while the others wait.

### CoriKnl

`lsst.ctrl.bps.parsl.sites.nersc.CoriKnl` is intended for use with the [NERSC Cori-KNL cluster](https://docs.nersc.gov/performance/io/knl/).
It subclasses [`TripleSlurm`](#tripleslurm) and adds some customisation.
Required and optional settings are the same as for `TripleSlurm`.

### Sdf

`lsst.ctrl.bps.parsl.sites.slac.Sdf` is intended to be used with the Rubin partition at the [SLAC Shared Scientific Data Facility (SDF)](https://sdf.slac.stanford.edu/public/doc/#/).  It subclasses [`Slurm`](#slurm) and adds some suitable customisation.  By default, a block of 1 node of 100 cores runs while another block waits in the queue.
Optional settings are:

* `nodes` (`int`): number of nodes for each Slurm job.
* `cores_per_node` (`int`): number of cores per node for each Slurm job.
* `walltime` (`str`): time limit for each Slurm job.
* `mem_per_node` (`int`): memory per node (GB) for each Slurm job.
* `max_blocks` (`int`): number of blocks (Slurm jobs) to use; one will execute while the others wait.

### LocalSrunWorkQueue

`lsst.ctrl.bps.parsl.sites.work_queue.LocalSrunWorkQueue` uses a [`LocalProvider`](https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.LocalProvider.html) with a [`WorkQueueExecutor`](https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.WorkQueueExecutor.html) to manage resources on single- or multi-node allocations.  For multi-node allocations, Slurm's [`srun`](https://slurm.schedmd.com/srun.html) command is used to launch jobs via an [`SrunLauncher`](https://parsl.readthedocs.io/en/stable/stubs/parsl.launchers.SrunLauncher.html).  This implementation uses the [`work_queue`](https://cctools.readthedocs.io/en/stable/work_queue/) module to schedule jobs with specific resource requests (e.g., memory, # cpus, wall time, disk space), taking into account the available resources on the nodes.

## Adding a site

You don't need to use a site configuration that's already contained in the ctrl_bps_parsl package.
You can write a subclass of `lsst.ctrl.bps.parsl.SiteConfig`, define the two abstract methods (`get_executors` and `select_executor`), and override any other methods that need to be customised.
You should place your `SiteConfig` subclass somewhere on your `PYTHONPATH`, and then set the `site.<computeSite>.class` to the fully-qualified name of your `SiteConfig` subclass.

If you think your site configuration might be of use to others, we can incorporate it into ctrl_bps_parsl; please file an issue on GitHub.

## Monitoring

Turning on Parsl's [monitoring](https://parsl.readthedocs.io/en/stable/userguide/monitoring.html) feature allows tracking the progress of the workflow using a web browser.
The site settings that support monitoring are:

* `monitorEnable` (`bool`): enable monitor? Defaults to `false`.
* `monitorInterval` (`float`): time interval (sec) between logging of resource usage. Defaults to 30.
* `monitorFilename` (`str`): name of file to use for the monitor sqlite database. Defaults to `monitor.sqlite`.

Once the workflow is running, point the `parsl-visualize` executable to the monitoring database, e.g.:

    parsl-visualize sqlite:////path/to/monitor.sqlite

(Yes, that's four slashes!)
Then you can point your web browser to the machine serving the visualisation, on the default port of 8080.
You will likely have to use an `ssh` tunnel to expose that port, e.g.:

    ssh -L 8080:localhost:8080 username@headnode

Then you can point your browser to `localhost:8080`.
