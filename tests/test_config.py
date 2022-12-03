import pytest
from lsst.daf.butler import Config


@pytest.mark.skip(reason="parsl not in LSST environment")
def test_config():
    """Super-basic test of configuration reading

    This is intended as a test of testing more than anything else.
    """
    from lsst.ctrl.bps import BpsConfig
    from lsst.ctrl.bps.parsl.configuration import get_bps_config_value

    config = BpsConfig(Config.fromString("foo: bar"))  # BpsConfig doesn't work directly with fromString
    assert get_bps_config_value(config, "foo", str) == "bar"
