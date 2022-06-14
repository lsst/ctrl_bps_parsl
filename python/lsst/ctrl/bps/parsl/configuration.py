import logging
import os
from typing import Any, Optional

from lsst.ctrl.bps import BpsConfig

__all__ = (
    "get_bps_config_value",
    "get_workflow_name",
    "get_workflow_filename",
    "set_parsl_logging",
)


def get_bps_config_value(
    config: BpsConfig,
    key: str,
    dataType: Optional[type] = None,
    default: Any = None,
    *,
    required: bool = False,
):
    """Get a value from the BPS configuration

    I find this more useful than ``BpsConfig.__getitem__`` or ``BpsConfig.get``.

    Parameters
    ----------
    config : `BpsConfig`
        Configuration from which to retrieve value.
    key : `str`
        Key name.
    dataType : `type`, optional
        If specified, require that the returned value have this type.
    default : optional
        Default value to be provided if ``key`` doesn't exist in the ``config``.
        A default value of `None` means that there is no default.
    required : `bool`, optional
        If ``True``, the returned value may come from the configuration or from
        the default, but it may not be `None`.

    Returns
    -------
    value
        Value for ``key`` in the `config`` if it exists, otherwise ``default``,
        if provided.

    Raises
    ------
    KeyError
        If ``key`` is not in ``config`` and no default is provided but a value
        is ``required``.
    RuntimeError
        If the value is not set or is of the wrong type.
    """
    options = dict(expandEnvVars=True, replaceVars=True, required=required)
    if default is not None:
        options["default"] = default
    found, value = config.search(key, options)
    if not found and required and default is None:
        raise KeyError(f"No value found for {key} and no default provided")
    if dataType is not None and not isinstance(value, dataType):
        raise RuntimeError(f"Configuration value {key}={value} is not of type {dataType}")
    return value


def get_workflow_name(config: BpsConfig) -> str:
    """Get name of this workflow

    The workflow name is constructed by joining the ``project`` and ``campaign``
    (if set; otherwise ``operator``) entries in the BPS configuration.

    Parameters
    ----------
    config : `BpsConfig`
        BPS configuration.

    Returns
    -------
    name : `str`
        Workflow name.
    """
    project = get_bps_config_value(config, "project", str, "bps")
    campaign = get_bps_config_value(
        config, "campaign", str, get_bps_config_value(config, "operator", str, required=True)
    )
    return f"{project}.{campaign}"


def get_workflow_filename(out_prefix: str) -> str:
    """Get filename for persisting workflow

    Parameters
    ----------
    out_prefix : `str`
        Directory which should contain workflow file.

    Returns
    -------
    filename : `str`
        Filename for persisting workflow.
    """
    return os.path.join(out_prefix, "parsl_workflow.pickle")


def set_parsl_logging(config: BpsConfig) -> int:
    """Set parsl logging levels

    The logging level is set by the ``parsl.log_level`` entry in the BPS
    configuration.

    Parameters
    ----------
    config : `BpsConfig`
        BPS configuration.

    Returns
    -------
    level : `int`
        Logging level applied to ``parsl`` loggers.
    """
    level = get_bps_config_value(config, ".parsl.log_level", str, "INFO")
    if level not in ("CRITICAL", "DEBUG", "ERROR", "FATAL", "INFO", "WARN"):
        raise RuntimeError(f"Unrecognised parsl.log_level: {level}")
    level = getattr(logging, level)
    for name in logging.root.manager.loggerDict:
        if name.startswith("parsl"):
            logging.getLogger(name).setLevel(level)
    logging.getLogger("database_manager").setLevel(logging.INFO)
    return level
