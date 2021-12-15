from datetime import datetime, timedelta
from typing import List, Dict


class Well:
    def __init__(
        self,
        well_name: str,
        drill_duration: int,
        frac_duration: int,
        release_date: datetime = None,
        due_date: datetime = None,
        lat: float = None,
        lon: float = None,
        priority: int = None,
    ):
        self.well_name = well_name
        self.drill_duration = drill_duration
        self.frac_duration = frac_duration
        self.release_date = release_date
        self.due_date = due_date
        self.lat = lat
        self.lon = lon
        self.priority = priority


class Resource:
    def __init__(
        self, name: str, start_date: datetime = None, end_date: datetime = None
    ):
        self.name = name
        self.start_date = start_date
        self.end_date = end_date

    def set_resource_availability(
        self, start_date: datetime, end_date: datetime = None
    ):
        self.start_date = start_date

    def __lt__(self, other):
        return self.start_date < other.start_date


class Rig(Resource):
    pass


class FracCrew(Resource):
    pass


class WellBatch:
    """
    WellBatch is the smallest unit of wells that need to be drilled by a single drilling rig. It could be a pad or subset of a pad, or multiple pads.
    """

    def __init__(self, name: str, wells: List[Well], priority: int = None):
        self.name = name
        self.wells = wells
        self.drill_duration = sum((well.drill_duration for well in wells))
        self.frac_duration = sum((well.frac_duration for well in wells))
        self.release_date = None
        self.due_date = None
        self.priority = priority
        for well in wells:
            if not well.release_date:
                continue
            if not self.release_date:
                self.release_date = well.release_date
            else:
                self.release_date = min(self.release_date, well.release_date)
        for well in wells:
            if not well.due_date:
                continue
            if not self.due_date:
                self.due_date = well.due_date
            else:
                self.due_date = max(self.due_date, well.due_date)
        if not priority:
            for well in wells:
                if not well.priority:
                    continue
                if not self.priority:
                    self.priority = well.priority
                else:
                    self.priority = min(self.priority, well.priority)

        # Status variables
        self.is_drilled = False
        self.drill_start = None
        self.drill_end = None
        self.is_fraced = False
        self.frac_start = None
        self.frac_end = None
        self.production_start = None
        self.production_end = None

    def set_drill_status(self, drill_start: datetime):
        self.is_drilled = True
        self.drill_start = drill_start
        self.drill_end = drill_start + timedelta(days=self.drill_duration)

    def set_frac_status(self, frac_start: datetime):
        if not self.is_drilled:
            raise Exception("Cannot frac before drill")
        if frac_start < self.drill_end:
            raise Exception("Frac cannot start before drill end")
        self.is_fraced = True
        self.frac_start = frac_start
        self.frac_end = frac_start + timedelta(days=self.frac_duration)

    def __lt__(self, other):
        if self.priority and other.priority:
            return self.priority < other.priority
        if self.priority and not other.priority:
            return True
        if not self.priority and other.priority:
            return False
        return True


class ScheduleEvent:
    def __init__(
        self,
        resource: Resource,
        well_batch: WellBatch,
        event_start: datetime,
        event_duration: int,
        event_end: datetime = None,
    ):
        self.resource = resource
        self.well_batch = well_batch
        self.event_start = event_start
        self.event_duration = event_duration
        if event_end:
            self.event_end = event_end
        else:
            self.event_end = event_start + timedelta(event_duration)

    def __repr__(self):
        return f"({self.resource.name}, {self.well_batch.name}, {self.event_start.strftime('%Y-%m-%d')})"


class Scheduler:
    def __init__(
        self, rigs: List[Rig], frac_crews: List[FracCrew], well_batches: List[WellBatch]
    ):
        self.rigs = rigs
        self.frac_crews = frac_crews
        self.well_batches = well_batches
        self.planning_period_start = None
        self.planning_period_end = None
        self.frac_lag = None
        self.production_lag = None
        self.schedule_events = []

    def set_planning_horizon(self, start: datetime, end: datetime):
        self.planning_period_start = start
        self.planning_period_end = end

    def set_frac_lag(self, frac_lag_days: int):
        self.frac_lag = frac_lag_days

    def set_production_lag(self, prod_lag_days: int):
        self.production_lag = prod_lag_days

    def schedule(self):
        self.well_batches.sort()
        self.rigs.sort(key=lambda x: x.start_date)
        self.frac_crews.sort(key=lambda x: x.start_date)

        for well_batch in self.well_batches:
            for rig in self.rigs:
                if self._is_valid(rig, well_batch):
                    if well_batch.release_date:
                        drill_start = max(rig.start_date, well_batch.release_date)
                    else:
                        drill_start = rig.start_date
                    drill_end = drill_start + timedelta(well_batch.drill_duration)
                    print(
                        f"{rig.name} assigned to {well_batch.name} start {drill_start} end {drill_end}"
                    )
                    rig.set_resource_availability(drill_end)
                    well_batch.set_drill_status(drill_start)
                    self.schedule_events.append(
                        ScheduleEvent(
                            rig,
                            well_batch,
                            drill_start,
                            drill_end,
                            well_batch.drill_duration,
                        )
                    )
                    break
            self.rigs.sort(key=lambda x: x.start_date)

        for well_batch in self.well_batches:
            if not well_batch.is_drilled:
                raise Exception("Cannot frac before drilling. Check logic")
            for frac_crew in self.frac_crews:
                if self._is_valid(frac_crew, well_batch):
                    frac_start = max(
                        frac_crew.start_date,
                        well_batch.drill_end + timedelta(self.frac_lag),
                    )
                    frac_end = frac_start + timedelta(well_batch.frac_duration)
                    print(
                        f"{frac_crew.name} assigned to {well_batch.name} start {frac_start} end {frac_end}"
                    )
                    frac_crew.set_resource_availability(frac_end)
                    well_batch.set_frac_status(frac_start)
                    self.schedule_events.append(
                        ScheduleEvent(
                            frac_crew,
                            well_batch,
                            frac_start,
                            frac_end,
                            well_batch.frac_duration,
                        )
                    )
                    break
            self.frac_crews.sort(key=lambda x: x.start_date)

    def get_schedule_events(self):
        if not self.schedule_events:
            raise Exception(
                "No schedule events exist. Did you run .schedule() to generate schedule events first?"
            )
        return self.schedule_events

    def _is_valid(self, resource: Resource, well_batch: WellBatch):
        if well_batch.release_date:
            start_date = max(resource.start_date, well_batch.release_date)
        else:
            start_date = resource.start_date
        if isinstance(resource, Rig):
            end_date = start_date + timedelta(days=well_batch.drill_duration)
        elif isinstance(resource, FracCrew):
            if not self.frac_lag:
                raise Exception("Set frac lag using set_grac_lag() before scheduling")
            end_date = start_date + timedelta(days=well_batch.frac_duration)
        else:
            raise Exception("Resource is neither rig or frac crew")
        if not resource.end_date and not well_batch.due_date:
            return True
        if resource.end_date and end_date > resource.end_date:
            return False
        if well_batch.due_date and end_date > well_batch.due_date:
            return False

        return True
