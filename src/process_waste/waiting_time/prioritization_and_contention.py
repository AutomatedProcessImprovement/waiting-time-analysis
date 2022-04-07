import pandas as pd
from tqdm import tqdm

from process_waste import START_TIMESTAMP_KEY, RESOURCE_KEY, WAITING_TIME_CONTENTION_KEY, \
    END_TIMESTAMP_KEY, ENABLED_TIMESTAMP_KEY, WAITING_TIME_PRIORITIZATION_KEY


def run_analysis(log: pd.DataFrame) -> pd.DataFrame:
    log[WAITING_TIME_PRIORITIZATION_KEY] = pd.Timedelta(0)
    log[WAITING_TIME_CONTENTION_KEY] = pd.Timedelta(0)
    for i in tqdm(log.index, desc='Resource prioritization and contention analysis'):
        index = pd.Index([i])
        detect_prioritization_or_contention(index, log)
    return log


def detect_prioritization_or_contention(event_index: pd.Index, log: pd.DataFrame) -> pd.DataFrame:
    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    # current event variables
    event_start_time = event[START_TIMESTAMP_KEY].values[0]
    event_start_time = pd.to_datetime(event_start_time, utc=True)
    event_enabled_time = event[ENABLED_TIMESTAMP_KEY].values[0]
    event_enabled_time = pd.to_datetime(event_enabled_time, utc=True)
    resource = event[RESOURCE_KEY].values[0]

    # resource events throughout the event log except the current event
    resource_events = log[log[RESOURCE_KEY] == resource]
    resource_events = resource_events.loc[resource_events.index.difference(event_index)]

    # taking activities that resource started before event_start_time but after event_enabled_time
    other_processing_events = resource_events[
        (resource_events[START_TIMESTAMP_KEY] < event_start_time) &
        (resource_events[END_TIMESTAMP_KEY] >= event_enabled_time)]

    # determining activities due to prioritization or contention
    events_due_to_prioritization = other_processing_events[
        other_processing_events[ENABLED_TIMESTAMP_KEY] > event_enabled_time]
    events_due_to_contention = other_processing_events[
        other_processing_events[ENABLED_TIMESTAMP_KEY] <= event_enabled_time]

    # calculating the waiting times
    wt_prioritization = (
            events_due_to_prioritization[END_TIMESTAMP_KEY] - events_due_to_prioritization[START_TIMESTAMP_KEY]
    ).sum()
    wt_contention = (
            events_due_to_contention[END_TIMESTAMP_KEY] - events_due_to_contention[START_TIMESTAMP_KEY]
    ).sum()

    # updating the dataframe
    log.loc[event_index, WAITING_TIME_PRIORITIZATION_KEY] = wt_prioritization
    log.loc[event_index, WAITING_TIME_CONTENTION_KEY] = wt_contention

    return log
