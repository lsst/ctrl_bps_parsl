Added `~lsst.ctrl.bps.parsl.providers.ImpatientSlurmProvider`, an execution provider that pools head-node cores with Slurm-allocated cores.
Tasks start immediately on the head node while Slurm nodes are being allocated, with the pool expanding as those nodes come online.
Added `~lsst.ctrl.bps.parsl.sites.ImpatientSlurm` site configuration that uses this provider, and updated `~lsst.ctrl.bps.parsl.sites.princeton.Tiger` to inherit from it (controlled by the new ``local_cores`` BPS configuration parameter, which defaults to ``0`` to preserve existing behaviour).
