import pandas as pd
from tqdm import tqdm

from process_waste import RESOURCE_KEY, WAITING_TIME_TOTAL_KEY, START_TIMESTAMP_KEY, END_TIMESTAMP_KEY, \
    WAITING_TIME_CONTENTION_KEY


def run_analysis(log: pd.DataFrame) -> pd.DataFrame:
    log[WAITING_TIME_CONTENTION_KEY] = pd.Timedelta(0)
    for index in tqdm(log.index, desc='contention analysis'):
        contention_for_event(index, log)
    return log


def contention_for_event(event_index: pd.Index, log: pd.DataFrame) -> pd.DataFrame:
    event = log.loc[event_index]
    if isinstance(event, pd.Series):
        event = event.to_frame().T

    event_start = event[START_TIMESTAMP_KEY].values[0]
    event_start = pd.to_datetime(event_start, utc=True)
    resource = event[RESOURCE_KEY].values[0]
    wt_total = event[WAITING_TIME_TOTAL_KEY].values[0]
    start_time = event[START_TIMESTAMP_KEY].values[0]
    wt_start_time = start_time - wt_total
    wt_start_time = pd.to_datetime(wt_start_time, utc=True)
    resource_events = log[log[RESOURCE_KEY] == resource]
    resource_events = resource_events[~resource_events.index.isin([event_index])]
    wt_contention = pd.Timedelta(0)

    other_processing_start_time = resource_events[
        (resource_events[START_TIMESTAMP_KEY] < event_start) &
        (resource_events[START_TIMESTAMP_KEY] >= wt_start_time)][START_TIMESTAMP_KEY].min()
    if not other_processing_start_time:
        log.at[event_index, WAITING_TIME_CONTENTION_KEY] = wt_contention
        return log

    other_processing_end_time = resource_events[
        (resource_events[END_TIMESTAMP_KEY] <= event_start)][END_TIMESTAMP_KEY].max()
    if not other_processing_end_time:
        log.at[event_index, WAITING_TIME_CONTENTION_KEY] = wt_contention
        return log

    wt_contention = other_processing_end_time - other_processing_start_time
    log.at[event_index, WAITING_TIME_CONTENTION_KEY] = wt_contention

    return log
