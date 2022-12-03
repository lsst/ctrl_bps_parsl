import pytest


@pytest.mark.skip(reason="parsl not in LSST environment")
def test_import():
    import lsst.ctrl.bps.parsl  # noqa
