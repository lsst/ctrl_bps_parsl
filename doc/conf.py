"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
For more information, see:
https://developer.lsst.io/stack/building-single-package-docs.html
"""

from documenteer.sphinxconfig.stackconf import build_package_configs  # noqa: F401

master_doc = "index"
extensions = ["sphinx_automodapi.automodapi"]

project = "ctrl_bps_parsl"
html_theme_options = dict(logotext=project)
html_title = project
html_short_title = project
exclude_patterns = ["changes/*"]
numpydoc_show_class_members = False

intersphinx_mapping["lsst"] = ("https://pipelines.lsst.io/v/daily/", None)  # noqa
