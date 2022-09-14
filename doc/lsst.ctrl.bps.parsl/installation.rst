.. py:currentmodule:: lsst.ctrl.bps.parsl

############
Installation
############


Parsl
=====

This plugin requires `Parsl`_, which may be installed into the LSST stack:

.. code-block:: bash

    mamba install --no-update-deps parsl

.. _Parsl: https://parsl-project.org


Parsl with monitoring support
=============================

The |parsl-visualize executable|_ is a web app for monitoring the progress of the Parsl workflow.
It has extra dependencies that are (as of the time of writing), `formally (but not practically) incompatible`_ with the LSST butler (which requires sqlalchemy>=1.4).
Attempting to install the ``parsl-with-monitoring`` conda package with ``--no-update-deps`` results in a failure, and without ``-no-update-deps`` results in a broken butler.
The solution is a little complicated.

.. |parsl-visualize executable| replace:: ``parsl-visualize`` executable
.. _parsl-visualize executable: https://parsl.readthedocs.io/en/stable/userguide/monitoring.html#visualization
.. _formally (but not practically) incompatible: https://github.com/Parsl/parsl/issues/2296

First, install the necessary dependencies:

.. code-block:: bash

    mamba install --only-deps parsl-with-monitoring

This will downgrade sqlalchemy (e.g., to 1.3.24).
We need to undo this in order for the LSST butler to work:

.. code-block:: bash

    mamba upgrade sqlalchemy=1.4.36

Now we need to uninstall ``parsl`` (installed as a dependency of ``parsl-with-monitoring``), and install its dependencies:

.. code-block:: bash

    mamba uninstall parsl
    mamba install --only-deps parsl

Now we have all the necessary dependencies but not ``parsl`` itself.
To avoid ``conda`` or ``pip`` dependency problems in parsl, we install it directly from source:

.. code-block:: bash

    git clone https://github.com/parsl/parsl.git
    cd parsl
    python setup.py install --user

Hopefully the next parsl release won't have this problem.

Plugin
======

Clone this package `from GitHub`_, set it up and build:

.. _from GitHub: https://github.com/lsst/ctrl_bps_parsl

.. code-block:: bash

    git clone https://github.com/lsst/ctrl_bps_parsl
    cd ctrl_bps_parsl
    setup -kr .
    scons

To use this plugin, you'll need to set it up in your environment, as you do for the rest of the LSST pipeline code:

.. code-block:: bash

    setup -kr /path/to/ctrl_bps_parsl
