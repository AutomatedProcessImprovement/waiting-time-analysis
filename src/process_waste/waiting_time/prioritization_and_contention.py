from typing import Optional, Tuple, List

import numpy as np
import pandas as pd

from batch_processing_analysis.config import EventLogIDs
from process_waste import log_ids_non_nil, BATCH_INSTANCE_ENABLED_KEY, BATCH_INSTANCE_ID_KEY
from process_waste.waiting_time.resource_unavailability import other_processing_events_during_waiting_time_of_event


def detect_contention_and_prioritization_intervals(
        event_index: pd.Index,
        log: pd.DataFrame,
        log_ids: Optional[EventLogIDs] = None) -> Tuple:
    log_ids = log_ids_non_nil(log_ids)

    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    other_processing_events = other_processing_events_during_waiting_time_of_event(event_index, log, log_ids=log_ids)

    # determining intervals due to prioritization or contention depending on batching

    other_processing_events_in_batch = other_processing_events[~other_processing_events[BATCH_INSTANCE_ID_KEY].isna()][
        other_processing_events[BATCH_INSTANCE_ID_KEY] == event[BATCH_INSTANCE_ID_KEY].values[0]]

    other_processing_events_out_batch = pd.concat([
        other_processing_events,
        other_processing_events_in_batch
    ]).drop_duplicates(keep=False)

    def __detect_intervals(processing_events: pd.DataFrame, actual_event_enabled_time: pd.Series) -> Tuple[List, List]:
        events_due_to_prioritization = processing_events[
            processing_events[log_ids.enabled_time] > pd.to_datetime(actual_event_enabled_time[0], utc=True)]

        events_due_to_contention = processing_events[
            processing_events[log_ids.enabled_time] <= pd.to_datetime(actual_event_enabled_time[0], utc=True)]

        # calculating the waiting time intervals

        if events_due_to_contention.size > 0:
            wt_contention_intervals = (
                # start time of contention
                np.maximum(actual_event_enabled_time, events_due_to_contention[log_ids.start_time].values),
                # end time of contention
                np.minimum(event[log_ids.start_time].values, events_due_to_contention[log_ids.end_time].values)
            )
        else:
            wt_contention_intervals = ([], [])

        if events_due_to_prioritization.size > 0:
            wt_prioritization_intervals = (
                # start time of prioritization
                np.maximum(actual_event_enabled_time, events_due_to_prioritization[log_ids.start_time].values),
                # end time of prioritization
                np.minimum(event[log_ids.start_time].values, events_due_to_prioritization[log_ids.end_time].values)
            )
        else:
            wt_prioritization_intervals = ([], [])

        return wt_contention_intervals, wt_prioritization_intervals

    actual_event_enabled_time = event[log_ids.enabled_time].values
    wt_contention_intervals_in_batch, wt_prioritization_intervals_in_batch = __detect_intervals(
        other_processing_events_in_batch, actual_event_enabled_time)

    actual_event_enabled_time = event[BATCH_INSTANCE_ENABLED_KEY].values
    wt_contention_intervals_out_batch, wt_prioritization_intervals_out_batch = __detect_intervals(
        other_processing_events_out_batch, actual_event_enabled_time)

    # concatenating the intervals in tuples

    wt_contention_intervals = (
        list(wt_contention_intervals_in_batch[0]) + list(wt_contention_intervals_out_batch[0]),
        list(wt_contention_intervals_in_batch[1]) + list(wt_contention_intervals_out_batch[1])
    )

    wt_prioritization_intervals = (
        list(wt_prioritization_intervals_in_batch[0]) + list(wt_prioritization_intervals_out_batch[0]),
        list(wt_prioritization_intervals_in_batch[1]) + list(wt_prioritization_intervals_out_batch[1])
    )

    return wt_contention_intervals, wt_prioritization_intervals
