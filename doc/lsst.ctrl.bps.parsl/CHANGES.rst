lsst-ctrl-bps-parsl v30.0.0 (2026-01-16)
========================================

New Features
------------

- Now pass job priority requests from BPS to Parsl executor classes (`DM-53483 <https://rubinobs.atlassian.net/browse/DM-53483>`_)


lsst-ctrl-bps-parsl v29.0.0 (2025-03-25)
========================================

New Features
------------

- Added support for PBS/Torque and updated Princeton site to support new Tiger3 cluster. (`dm-48539 <https://rubinobs.atlassian.net/browse/dm-48539>`_)


lsst-ctrl-bps-parsl v28.0.0 (2024-11-21)
========================================

Bug Fixes
---------

- Resolved issue relating to the high throughput executor performing validation via checking of a resource spec dictionary.
  Until now, ``ctrl_bps_parsl`` was passing `None`` instead of an empty `dict`` when there were no resource requests associated with a task. (`DM-45863 <https://rubinobs.atlassian.net/browse/DM-45863>`_)
- Fixed an error caused by the deprecated ``max_workers`` parameter, which was removed in Parsl version 2024.09.09 following its deprecation in version 2024.03.04. (`DM-47399 <https://rubinobs.atlassian.net/browse/DM-47399>`_)

Other Changes and Additions
---------------------------

- Updated selected unit tests to reflect the changes made in ``BpsConfig.__init__()``. (`DM-44110 <https://rubinobs.atlassian.net/browse/DM-44110>`_)


lsst-ctrl-bps-parsl v27.0.0 (2024-06-05)
========================================

New Features
------------

- Updated the open-source license to allow for the code to be distributed with either GPLv3 or BSD 3-clause license. (`DM-37231 <https://rubinobs.atlassian.net/browse/DM-37231>`_)
- Added log subdirectories to avoid having too many files in a single directory. (`DM-41229 <https://rubinobs.atlassian.net/browse/DM-41229>`_)


Bug Fixes
---------

- Fixed ``compute_site`` keyword error in submit introduced by `DM-38138  <https://rubinobs.atlassian.net/browse/DM-38138>`_. (`DM-43721 <https://rubinobs.atlassian.net/browse/DM-43721>`_)


lsst-ctrl-bps-parsl v26.0.0 (2023-09-25)
========================================

New Features
------------

- Added support for getting ``scheduler_options`` from BPS configuration for Slurm. (`DM-32764 <https://rubinobs.atlassian.net/browse/DM-32764>`_)
- Added ``cmd_timeout`` configurable option to the Princeton site settings. (`DM-38184 <https://rubinobs.atlassian.net/browse/DM-38184>`_)


Bug Fixes
---------

- Now resolve nested symbolic names correctly (`DM-39885 <https://rubinobs.atlassian.net/browse/DM-39885>`_)


Other Changes and Additions
---------------------------

- Brought package up to LSST middleware packaging standards. (`DM-36092 <https://rubinobs.atlassian.net/browse/DM-36092>`_)


lsst-ctrl-bps-parsl v25.0.0 (2023-07-01)
========================================

First release as part of the LSST Science Pipelines.
