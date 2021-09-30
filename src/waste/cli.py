import datetime
import os.path

import click
import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.importer.xes import importer as xes_importer
from pm4py.objects.log.util import interval_lifecycle
from pm4py.statistics.concurrent_activities.pandas import get as concurrent_activities_get

# handoff_id uniquely identifies a handoff
handoff_id = ['source_activity', 'source_resource', 'destination_activity', 'destination_resource']


# Transportation: Hand-off
# Metrics:
# - frequency
# - duration
# - score = frequency * duration
def calculate_handoff_per_case(case: pd.DataFrame) -> pd.DataFrame:
    global handoff_id

    case = case.sort_values(by='time:timestamp')

    # Processing concurrent activities
    params = {
        concurrent_activities_get.Parameters.TIMESTAMP_KEY: "time:timestamp",
        concurrent_activities_get.Parameters.START_TIMESTAMP_KEY: "start_timestamp"
    }
    concurrent_activities = concurrent_activities_get.apply(case, parameters=params)
    case_without_concurrent_activities = case.copy()
    for activities_pair in concurrent_activities:
        # TODO: not all events in these activities do overlap, check for timestamp overlap
        # TODO: not all events overlap between each other, there could be many sequential sets of events
        #       that overlap only inside the set
        concurrent = case[(case['Activity'] == activities_pair[0]) | (case['Activity'] == activities_pair[1])]
        # TODO: event might be already dropped in previous loops
        case_without_concurrent_activities.drop(concurrent.index, inplace=True, errors='ignore')
        # TODO: there could be more than 2 events in 'concurrent'
        if concurrent.iloc[0]['time:timestamp'] >= concurrent.iloc[1]['time:timestamp']:
            case_without_concurrent_activities = case_without_concurrent_activities.append(concurrent.iloc[0])
        else:
            case_without_concurrent_activities = case_without_concurrent_activities.append(concurrent.iloc[1])
    case_without_concurrent_activities.sort_values(by='start_timestamp', inplace=True)
    case_without_concurrent_activities.reset_index(drop=True, inplace=True)

    # Handoff Identification
    case_processed = case_without_concurrent_activities
    resource_changed = case_processed['Resource'] != case_processed.shift(-1)['Resource']
    activity_changed = case_processed['Activity'] != case_processed.shift(-1)['Activity']
    handoff_occurred = resource_changed & activity_changed  # both conditions must be satisfied
    handoff = pd.DataFrame(
        columns=['source_activity', 'source_resource', 'destination_activity', 'destination_resource', 'duration'])
    handoff.loc[:, 'source_activity'] = case_processed[handoff_occurred]['Activity']
    handoff.loc[:, 'source_resource'] = case_processed[handoff_occurred]['Resource']
    handoff.loc[:, 'destination_activity'] = case_processed[handoff_occurred].shift(-1)['Activity']
    handoff.loc[:, 'destination_resource'] = case_processed[handoff_occurred].shift(-1)['Resource']
    # TODO: last value has NaN because of shift in Production.xes (why it doesn't so in PurchasingExample.xes?)
    empty_timestamp = pd.to_datetime('2000-01-01 00:00 +0200', infer_datetime_format=True)
    handoff['duration'] = case_processed[handoff_occurred].shift(-1, fill_value=empty_timestamp)['start_timestamp'] - \
                          case_processed[handoff_occurred]['time:timestamp']
    # dropping an event at the end which is always 'True'
    handoff.drop(handoff.tail(1).index, inplace=True)

    # Frequency
    handoff_per_case = pd.DataFrame(
        columns=['source_activity', 'source_resource', 'destination_activity', 'destination_resource', 'duration'])
    grouped = handoff.groupby(by=handoff_id)
    for group in grouped:
        pair, records = group
        handoff_per_case = handoff_per_case.append(pd.Series({
            'source_activity': pair[0],
            'source_resource': pair[1],
            'destination_activity': pair[2],
            'destination_resource': pair[3],
            'duration': records['duration'].sum(),
            'frequency': len(records)
        }), ignore_index=True)

    return handoff_per_case


@click.command()
@click.option('-l', '--log_path', default=None, required=True,
              help='Path to an event log in XES-format.')
@click.option('-o', '--output_dir', default='./', show_default=True,
              help='Path to an output directory where statistics will be saved.')
def main(log_path, output_dir):
    log = xes_importer.apply(log_path)

    # converting lifecycle log to interval log
    log_interval = interval_lifecycle.to_interval(log)
    # conversion to pd.DataFrame format
    event_log_interval = log_converter.apply(log_interval, variant=log_converter.Variants.TO_DATA_FRAME)
    # adjusting time slightly to avoid unnecessary concurrent events
    event_log_interval['start_timestamp'] = event_log_interval['start_timestamp'] + pd.Timedelta('1us')

    # grouping by case ID
    event_log_interval_by_case = event_log_interval.groupby(by='case:concept:name')

    print('Calculating handoff for each case...')
    results_per_case = {}
    for (case_id, case) in event_log_interval_by_case:
        result = calculate_handoff_per_case(case)
        results_per_case[case_id] = result

    print('Post-processing final statistics...')
    statistics = pd.DataFrame(columns=handoff_id + ['frequency', 'duration'])
    results_per_case_grouped = pd.concat(results_per_case, ignore_index=True).groupby(by=handoff_id)
    for group in results_per_case_grouped:
        pair, records = group
        frequency = records['frequency'].sum()  # len(records) == frequency
        duration = records['duration'].sum()
        statistics = statistics.append(pd.Series({
            'source_activity': pair[0],
            'source_resource': pair[1],
            'destination_activity': pair[2],
            'destination_resource': pair[3],
            'duration': duration,
            'frequency': frequency
        }), ignore_index=True)

    file_name, _ = os.path.splitext(os.path.basename(log_path))
    result_path = os.path.join(output_dir, file_name + '.xlsx')
    print(f'Saving results to {result_path}')
    statistics['duration_seconds'] = pd.to_numeric(statistics['duration'])
    statistics.to_excel(result_path)


# DONE: Sequential events
#   - identical dates
#   - parallel gateways (enabled timestamp)
#   - modify event log to create a parallel or overlapping activity by time

# DONE: Handoff types, can we discover resources, calendars (identify human, system, internal or external resource)?
# We can't discover, but we can label it manually.

# DONE: Separate dataframe for metrics

# NOTE: Requirements:
#   - only interval event logs, i.e., event logs where each event has a start timestamp and a completion timestamp

# TODO: Ping-pong handoff is not identified yet
# TODO: CTE, processing time / full time
#   - Do we count only business hours? Yes. Using 24 hours for PurchasingExample.xes
# DONE: Add total handoff frequency, frequency per case using unique pairs source+resource
# TODO: Mark manually some events with resource_type label "system" to add handoff type identification

# TODO: Production.xes fails with the current approach

if __name__ == '__main__':
    main()
