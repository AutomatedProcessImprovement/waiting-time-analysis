from typing import Optional, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm

from batch_processing_analysis.config import EventLogIDs
from process_waste import WAITING_TIME_CONTENTION_KEY, WAITING_TIME_PRIORITIZATION_KEY, default_log_ids, log_ids_non_nil
from process_waste.waiting_time.resource_unavailability import other_processing_events_during_waiting_time_of_event


def run_analysis(log: pd.DataFrame) -> pd.DataFrame:
    log[WAITING_TIME_PRIORITIZATION_KEY] = pd.Timedelta(0)
    log[WAITING_TIME_CONTENTION_KEY] = pd.Timedelta(0)
    for i in tqdm(log.index, desc='Resource prioritization and contention analysis'):
        index = pd.Index([i])
        detect_and_update_prioritization_or_contention(index, log)
    return log


def detect_and_update_prioritization_or_contention(
        event_index: pd.Index,
        log: pd.DataFrame,
        log_ids: Optional[EventLogIDs] = None):
    log_ids = log_ids_non_nil(log_ids)

    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    event_enabled_time = event[log_ids.enabled_time].values[0]
    event_enabled_time = pd.to_datetime(event_enabled_time, utc=True)

    other_processing_events = other_processing_events_during_waiting_time_of_event(event_index, log, log_ids=log_ids)

    # determining activities due to prioritization or contention
    events_due_to_prioritization = other_processing_events[
        other_processing_events[log_ids.enabled_time] > event_enabled_time]
    events_due_to_contention = other_processing_events[
        other_processing_events[log_ids.enabled_time] <= event_enabled_time]

    # calculating the waiting times

    if events_due_to_prioritization.size > 0:
        wt_prioritization = (
                np.minimum(event[log_ids.start_time].values, events_due_to_prioritization[log_ids.end_time].values) -
                np.maximum(event[log_ids.enabled_time].values, events_due_to_prioritization[log_ids.start_time].values)
        ).sum()
    else:
        wt_prioritization = pd.Timedelta(0)

    if events_due_to_contention.size > 0:
        wt_contention = (
                np.minimum(event[log_ids.start_time].values, events_due_to_contention[log_ids.end_time].values) -
                np.maximum(event[log_ids.enabled_time].values, events_due_to_contention[log_ids.start_time].values)
        ).sum()
    else:
        wt_contention = pd.Timedelta(0)

    # updating the dataframe
    log.loc[event_index, WAITING_TIME_PRIORITIZATION_KEY] = wt_prioritization
    log.loc[event_index, WAITING_TIME_CONTENTION_KEY] = wt_contention


def detect_contention_and_prioritization_intervals(
        event_index: pd.Index,
        log: pd.DataFrame,
        log_ids: Optional[EventLogIDs] = None) -> Tuple:
    log_ids = log_ids_non_nil(log_ids)

    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    event_enabled_time = event[log_ids.enabled_time].values[0]
    event_enabled_time = pd.to_datetime(event_enabled_time, utc=True)

    other_processing_events = other_processing_events_during_waiting_time_of_event(event_index, log, log_ids=log_ids)

    # determining activities due to prioritization or contention

    events_due_to_prioritization = other_processing_events[
        other_processing_events[log_ids.enabled_time] > event_enabled_time]

    events_due_to_contention = other_processing_events[
        other_processing_events[log_ids.enabled_time] <= event_enabled_time]

    # calculating the waiting time intervals

    if events_due_to_contention.size > 0:
        wt_contention_intervals = (
            # start time of contention
            np.maximum(event[log_ids.enabled_time].values, events_due_to_contention[log_ids.start_time].values),
            # end time of contention
            np.minimum(event[log_ids.start_time].values, events_due_to_contention[log_ids.end_time].values)
        )
    else:
        wt_contention_intervals = None

    if events_due_to_prioritization.size > 0:
        wt_prioritization_intervals = (
            # start time of prioritization
            np.maximum(event[log_ids.enabled_time].values, events_due_to_prioritization[log_ids.start_time].values),
            # end time of prioritization
            np.minimum(event[log_ids.start_time].values, events_due_to_prioritization[log_ids.end_time].values)
        )
    else:
        wt_prioritization_intervals = None

    return wt_contention_intervals, wt_prioritization_intervals
