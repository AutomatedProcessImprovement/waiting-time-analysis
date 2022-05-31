import pandas as pd
from tqdm import tqdm

from process_waste import START_TIMESTAMP_KEY, WAITING_TIME_CONTENTION_KEY, \
    END_TIMESTAMP_KEY, ENABLED_TIMESTAMP_KEY, WAITING_TIME_PRIORITIZATION_KEY
from process_waste.waiting_time.resource_unavailability import other_processing_events_during_waiting_time_of_event


def run_analysis(log: pd.DataFrame) -> pd.DataFrame:
    log[WAITING_TIME_PRIORITIZATION_KEY] = pd.Timedelta(0)
    log[WAITING_TIME_CONTENTION_KEY] = pd.Timedelta(0)
    for i in tqdm(log.index, desc='Resource prioritization and contention analysis'):
        index = pd.Index([i])
        detect_prioritization_or_contention(index, log)
    return log


def detect_prioritization_or_contention(event_index: pd.Index, log: pd.DataFrame):
    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    event_enabled_time = event[ENABLED_TIMESTAMP_KEY].values[0]
    event_enabled_time = pd.to_datetime(event_enabled_time, utc=True)

    other_processing_events = other_processing_events_during_waiting_time_of_event(event_index, log)

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


def detect_prioritizations_and_contentions(log: pd.DataFrame):
    for i in log.index:
        event_index = pd.Index([i])
        detect_prioritization_or_contention(event_index, log)
