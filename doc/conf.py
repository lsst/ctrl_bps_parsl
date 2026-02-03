"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
For more information, see:
https://developer.lsst.io/stack/building-single-package-docs.html
"""

# ruff: noqa: F403, F405

from documenteer.conf.guide import *

exclude_patterns.append("changes/*")

# numpydoc_show_class_members = False
