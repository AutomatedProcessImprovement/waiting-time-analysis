from typing import Tuple

import pandas as pd
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.importer.xes import importer as xes_importer

log = xes_importer.apply('data/PurchasingExample.xes')
event_log = log_converter.apply(log, variant=log_converter.Variants.TO_DATA_FRAME)


# Transportation: Hand-off
# Metrics:
# - frequency
# - duration
# - score = frequency * duration
def calculate_handoff_per_case(case: pd.DataFrame):
    case = case.sort_values(by='time:timestamp')

    # Handoff Identification
    resource_changed = case['Resource'] != case.shift(-1)['Resource']
    activity_changed = case['Activity'] != case.shift(-1)['Activity']
    # because of NaN at the end of the shifted dataframe, we always have True
    resource_changed.iloc[-1] = False
    activity_changed.iloc[-1] = False
    # both conditions must be satisfied
    handoff_occurred = resource_changed & activity_changed
    case['handoff_occurred'] = handoff_occurred

    # Frequency
    case.loc[case['handoff_occurred'] == True, 'handoff_frequency'] = 1

    # Duration
    handoff_start = case.loc[handoff_occurred, 'time:timestamp']
    handoff_end = case.loc[handoff_occurred.shift(1, fill_value=False), 'time:timestamp']
    case.loc[handoff_occurred, 'handoff_duration'] = handoff_end.values - handoff_start.values

    # Score
    case['handoff_score'] = case['handoff_duration'] * case['handoff_frequency']

    # Reporting
    for (activity, group) in case[case['handoff_score'].notna()].groupby(by='Activity'):
        score_per_activity = group['handoff_score'].sum()
        print(f'{activity}: \t{score_per_activity}')


event_log_by_case = event_log.groupby(by='case:concept:name')
for (case_id, case) in event_log_by_case:
    if case_id not in ['1', '10']:
        continue
    print(case_id)
    calculate_handoff_per_case(case)
