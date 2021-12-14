from datetime import timedelta, datetime
from src.scheduler import *


def test_well_constructor():
    well = Well(
        well_name="Well 1",
        drill_duration=45,
        frac_duration=15,
        release_date=datetime(2020, 1, 1),
    )
    assert isinstance(well, Well)


def test_rig_constructor():
    rig = Rig(
        name="Rig 1", start_date=datetime(2020, 1, 1), end_date=datetime(2030, 1, 1)
    )

    assert isinstance(rig, Resource)
    assert isinstance(rig, Rig)


def test_well_batch_constructor():
    wells = [Well(f"Well {i}", 45, 15, datetime(2020, 1, i)) for i in range(1, 6)]
    well_batch = WellBatch(name="Pad 1", wells=wells)
    assert isinstance(well_batch, WellBatch)


def test_well_batch_constructor_wo_release_date():
    wells = [Well(f"Well {i}", 45, 15) for i in range(1, 6)]
    well_batch = WellBatch(name="Pad 1", wells=wells)
    assert isinstance(well_batch, WellBatch)


def test_well_batch_set_drill_status():
    wells = [Well(f"Well {i}", 45, 15) for i in range(1, 6)]
    well_batch = WellBatch(name="Pad 1", wells=wells)
    well_batch.set_drill_status(datetime(2020, 2, 1))
    assert well_batch.is_drilled is True


def test_well_batch_set_frac_status():
    wells = [Well(f"Well {i}", 45, 15) for i in range(1, 6)]
    well_batch = WellBatch(name="Pad 1", wells=wells)
    well_batch.set_drill_status(datetime(2020, 2, 1))
    well_batch.set_frac_status(datetime(2020, 3, 2))
    assert well_batch.is_fraced is True


def test_well_batch_set_frac_status_exception():
    wells = [Well(f"Well {i}", 45, 15) for i in range(1, 6)]
    well_batch = WellBatch(name="Pad 1", wells=wells)
    well_batch.set_frac_status(datetime(2020, 3, 2))
    assert well_batch.is_fraced is True
