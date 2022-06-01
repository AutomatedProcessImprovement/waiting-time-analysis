import pandas as pd
import pytest

from batch_processing_analysis.config import EventLogIDs
from process_waste import identify, default_log_ids

icpm_data = [
    # {'log_name': 'LoanApp_infRes_24-7_noTimers.CSV', 'parallel_run': True},  # no WT at all
    # {'log_name': 'LoanApp_infRes_24-7_timers.CSV', 'parallel_run': True},  # all WT due to extraneous factors
    {'log_name': 'LoanApp_fewRes_24-7_noTimers.CSV', 'parallel_run': True},  # all WT due to resource contention
]


def __remove_nan_resources(event_log: pd.DataFrame) -> pd.DataFrame:
    event_log = event_log.dropna(subset=['Resource'])
    return event_log


def __filter_specific_case(event_log: pd.DataFrame) -> pd.DataFrame:
    event_log = event_log[event_log['case_id'] == 20]
    return event_log


@pytest.mark.icpm
@pytest.mark.parametrize('test_data', icpm_data,
                         ids=map(lambda x: f"{x['log_name']}, parallel={x['parallel_run']}", icpm_data))
def test_handoffs_for_icpm_conference(assets_path, test_data):
    log_path = assets_path / 'icpm/input' / test_data['log_name']
    parallel = test_data['parallel_run']
    output_dir = assets_path / 'icpm/output'

    log_ids = EventLogIDs()
    log_ids.start_time = 'start_time'
    log_ids.end_time = 'end_time'
    log_ids.case = 'case_id'
    log_ids.activity = 'Activity'
    log_ids.resource = 'Resource'

    result = identify(log_path, parallel, log_ids=log_ids)

    output_dir.mkdir(parents=True, exist_ok=True)
    extension_suffix = '.csv'

    # handoff
    if result['handoff'] is not None:
        handoff_output_path = output_dir / (log_path.stem + '_handoff')
        handoff_csv_path = handoff_output_path.with_suffix(extension_suffix)
        print(f'Saving handoff report to {handoff_csv_path}')
        result['handoff'].to_csv(handoff_csv_path, index=False)
    else:
        print('No handoffs found')