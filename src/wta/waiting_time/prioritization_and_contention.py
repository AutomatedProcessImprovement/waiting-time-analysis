from typing import Optional, Tuple, List

import numpy as np
import pandas as pd

from wta import log_ids_non_nil
from wta.helpers import EventLogIDs
from wta.waiting_time.resource_unavailability import other_processing_events_during_waiting_time_of_event


def detect_intervals(processing_events: pd.DataFrame, actual_event_enabled_time: pd.Series, log_ids: EventLogIDs, event: pd.DataFrame) -> Tuple[List[List], List[List]]:
    actual_event_enabled_time_utc = pd.to_datetime(actual_event_enabled_time[0], utc=True)
    events_due_to_prioritization = processing_events[processing_events[log_ids.enabled_time] > actual_event_enabled_time_utc]
    events_due_to_contention = processing_events[processing_events[log_ids.enabled_time] <= actual_event_enabled_time_utc]

    def calculate_intervals(events_due_to, actual_event_enabled_time):
        if events_due_to.size > 0:
            start_time = np.maximum(actual_event_enabled_time, events_due_to[log_ids.start_time].values)
            end_time = np.minimum(event[log_ids.start_time].values, events_due_to[log_ids.end_time].values)
            intervals = [(start, end) for start, end in zip(start_time, end_time) if start <= end]
            return [list(t) for t in zip(*intervals)] if intervals else ([], [])
        return ([], [])

    wt_contention_intervals = calculate_intervals(events_due_to_contention, actual_event_enabled_time)
    wt_prioritization_intervals = calculate_intervals(events_due_to_prioritization, actual_event_enabled_time)

    return wt_contention_intervals, wt_prioritization_intervals


def detect_contention_and_prioritization_intervals(
        event_index: pd.Index,
        log: pd.DataFrame,
        log_ids: Optional[EventLogIDs] = None) -> Tuple[List, List]:

    log_ids = log_ids_non_nil(log_ids)

    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    other_processing_events = other_processing_events_during_waiting_time_of_event(event_index, log, log_ids=log_ids)

    event_batch_instance_id = event[log_ids.batch_id].values[0]
    other_processing_events_in_batch = other_processing_events.query(
        f'{log_ids.batch_id} == @event_batch_instance_id')

    other_processing_events_out_batch = pd.concat([
        other_processing_events,
        other_processing_events_in_batch
    ]).drop_duplicates(keep=False)

    actual_event_enabled_time = event[log_ids.enabled_time].values
    wt_contention_intervals_in_batch, wt_prioritization_intervals_in_batch = detect_intervals(
        other_processing_events_in_batch, actual_event_enabled_time, log_ids, event)

    actual_event_enabled_time = min(event[log_ids.batch_instance_enabled].values, event[log_ids.start_time].values)
    if pd.isna(actual_event_enabled_time).all():
        actual_event_enabled_time = event[log_ids.enabled_time].values
    wt_contention_intervals_out_batch, wt_prioritization_intervals_out_batch = detect_intervals(
        other_processing_events_out_batch, actual_event_enabled_time, log_ids, event)

    wt_contention_intervals = (
        list(wt_contention_intervals_in_batch[0]) + list(wt_contention_intervals_out_batch[0]),
        list(wt_contention_intervals_in_batch[1]) + list(wt_contention_intervals_out_batch[1])
    )

    wt_prioritization_intervals = (
        list(wt_prioritization_intervals_in_batch[0]) + list(wt_prioritization_intervals_out_batch[0]),
        list(wt_prioritization_intervals_in_batch[1]) + list(wt_prioritization_intervals_out_batch[1])
    )
    return wt_contention_intervals, wt_prioritization_intervals
