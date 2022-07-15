import pandas as pd
import pytest

import wta.helpers
from wta.main import run

manual_log_calendar = {
    'Marcus': [
        {'from': 'MONDAY', 'to': 'MONDAY', 'beginTime': '10:00:00', 'endTime': '12:00:00'},
        {'from': 'TUESDAY', 'to': 'TUESDAY', 'beginTime': '10:00:00', 'endTime': '12:00:00'},
        {'from': 'WEDNESDAY', 'to': 'WEDNESDAY', 'beginTime': '10:00:00', 'endTime': '12:00:00'},
        {'from': 'THURSDAY', 'to': 'THURSDAY', 'beginTime': '10:00:00', 'endTime': '12:00:00'},
        {'from': 'FRIDAY', 'to': 'FRIDAY', 'beginTime': '10:00:00', 'endTime': '12:00:00'},
        {'from': 'MONDAY', 'to': 'MONDAY', 'beginTime': '12:30:00', 'endTime': '20:00:00'},
        {'from': 'TUESDAY', 'to': 'TUESDAY', 'beginTime': '12:30:00', 'endTime': '20:00:00'},
        {'from': 'WEDNESDAY', 'to': 'WEDNESDAY', 'beginTime': '12:30:00', 'endTime': '20:00:00'},
        {'from': 'THURSDAY', 'to': 'THURSDAY', 'beginTime': '12:30:00', 'endTime': '20:00:00'},
        {'from': 'FRIDAY', 'to': 'FRIDAY', 'beginTime': '12:30:00', 'endTime': '20:00:00'},
    ],
    'Anya': [
        {'from': 'MONDAY', 'to': 'MONDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'TUESDAY', 'to': 'TUESDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'WEDNESDAY', 'to': 'WEDNESDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'THURSDAY', 'to': 'THURSDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'FRIDAY', 'to': 'FRIDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
    ],
    'Dom': [
        {'from': 'MONDAY', 'to': 'MONDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'TUESDAY', 'to': 'TUESDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'WEDNESDAY', 'to': 'WEDNESDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'THURSDAY', 'to': 'THURSDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'FRIDAY', 'to': 'FRIDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
    ],
    'Carmine': [
        {'from': 'MONDAY', 'to': 'MONDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'TUESDAY', 'to': 'TUESDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'WEDNESDAY', 'to': 'WEDNESDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'THURSDAY', 'to': 'THURSDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'FRIDAY', 'to': 'FRIDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
    ],
    'Cole': [
        {'from': 'MONDAY', 'to': 'MONDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'TUESDAY', 'to': 'TUESDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'WEDNESDAY', 'to': 'WEDNESDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'THURSDAY', 'to': 'THURSDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
        {'from': 'FRIDAY', 'to': 'FRIDAY', 'beginTime': '10:00:00', 'endTime': '20:00:00'},
    ],
}

icpm_data = [
    # Automated testing

    {'log_name': 'handoff-test.csv',
     'parallel_run': True,
     'batch_size': 10,
     'expected': 'handoff-test_expected.csv',
     'save_report': False},
    {'log_name': 'manual_log_1.csv',
     'parallel_run': True,
     'batch_size': 2,
     'calendar': manual_log_calendar,
     'expected': 'manual_log_1_expected.csv',
     'save_report': False},
    {'log_name': 'manual_log_2.csv',
     'parallel_run': True,
     'batch_size': 2,
     'calendar': manual_log_calendar,
     'expected': 'manual_log_2_expected.csv',
     'save_report': False},
    {'log_name': 'manual_log_3.csv',
     'parallel_run': True,
     'batch_size': 2,
     'calendar': manual_log_calendar,
     'expected': 'manual_log_3_expected.csv',
     'save_report': False},
    {'log_name': 'manual_log_4.csv',
     'parallel_run': True,
     'batch_size': 2,
     'calendar': manual_log_calendar,
     'expected': 'manual_log_4_expected.csv',
     'save_report': False},
    {'log_name': 'manual_log_5.csv',
     'parallel_run': True,
     'batch_size': 2,
     'calendar': manual_log_calendar,
     'expected': 'manual_log_5_expected.csv',
     'save_report': False},

    # Manual testing

    # {'log_name': 'Production.csv', 'parallel_run': True, 'batch_size': 2},
    # {'log_name': 'LoanApp_infRes_9-5_timers_prior.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_24-7_timers_prior.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_9-5_timers.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_9-5_noTimers.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_9-5_noTimers_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_9-5_timers_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_9-5_noTimers_prior_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_24-7_noTimers_prior.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_9-5_noTimers_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_24-7_noTimers.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_24-7_timers_prior.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_24-7_noTimers_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_24-7_noTimers_prior_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_24-7_noTimers.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_9-5_timers_prior.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_9-5_timers.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_9-5_noTimers_prior.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_9-5_timers_prior_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_9-5_timers_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_24-7_timers_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_24-7_noTimers_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_9-5_noTimers.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_24-7_timers_prior_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_9-5_timers_prior_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_9-5_noTimers_prior_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_24-7_timers_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_24-7_timers.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_9-5_noTimers_prior.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_24-7_noTimers_prior_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_24-7_noTimers_prior.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_infRes_24-7_timers_prior_batch.CSV', 'parallel_run': True, 'batch_size': 10},
    # {'log_name': 'LoanApp_fewRes_24-7_timers.CSV', 'parallel_run': True, 'batch_size': 10},
]


def __remove_nan_resources(event_log: pd.DataFrame) -> pd.DataFrame:
    event_log = event_log.dropna(subset=['Resource'])
    return event_log


def __filter_specific_case(event_log: pd.DataFrame) -> pd.DataFrame:
    event_log = event_log[event_log['case_id'] == 20]
    return event_log


def __generate_calendars(log_name: str):
    prefixes = ["Clerk", "Loan Officer", "AML Investigator", "Appraiser", "Senior Officer", "Applicant"]
    calendars_24_7 = {}
    calendars_9_5 = {}
    for prefix in prefixes:
        for i in range(1000):
            resource = "{}-{number:06}".format(prefix, number=(i + 1))
            calendars_24_7[resource] = [
                {'from': 'MONDAY', 'beginTime': '00:00:00', 'to': 'MONDAY', 'endTime': '23:59:59.999999'},
                {'from': 'TUESDAY', 'beginTime': '00:00:00', 'to': 'TUESDAY', 'endTime': '23:59:59.999999'},
                {'from': 'WEDNESDAY', 'beginTime': '00:00:00', 'to': 'WEDNESDAY', 'endTime': '23:59:59.999999'},
                {'from': 'THURSDAY', 'beginTime': '00:00:00', 'to': 'THURSDAY', 'endTime': '23:59:59.999999'},
                {'from': 'FRIDAY', 'beginTime': '00:00:00', 'to': 'FRIDAY', 'endTime': '23:59:59.999999'},
                {'from': 'SATURDAY', 'beginTime': '00:00:00', 'to': 'SATURDAY', 'endTime': '23:59:59.999999'},
                {'from': 'SUNDAY', 'beginTime': '00:00:00', 'to': 'SUNDAY', 'endTime': '23:59:59.999999'}
            ]
            if prefix == "Loan Officer":
                calendars_9_5[resource] = [
                    {'from': 'MONDAY', 'beginTime': '09:00:00', 'to': 'MONDAY', 'endTime': '17:00:00'},
                    {'from': 'TUESDAY', 'beginTime': '09:00:00', 'to': 'TUESDAY', 'endTime': '17:00:00'},
                    {'from': 'WEDNESDAY', 'beginTime': '09:00:00', 'to': 'WEDNESDAY', 'endTime': '17:00:00'}
                ]
            elif prefix == "Senior Officer":
                calendars_9_5[resource] = [
                    {'from': 'THURSDAY', 'beginTime': '09:00:00', 'to': 'THURSDAY', 'endTime': '17:00:00'},
                    {'from': 'FRIDAY', 'beginTime': '09:00:00', 'to': 'FRIDAY', 'endTime': '17:00:00'}
                ]
            else:
                calendars_9_5[resource] = [
                    {'from': 'MONDAY', 'beginTime': '09:00:00', 'to': 'MONDAY', 'endTime': '17:00:00'},
                    {'from': 'TUESDAY', 'beginTime': '09:00:00', 'to': 'TUESDAY', 'endTime': '17:00:00'},
                    {'from': 'WEDNESDAY', 'beginTime': '09:00:00', 'to': 'WEDNESDAY', 'endTime': '17:00:00'},
                    {'from': 'THURSDAY', 'beginTime': '09:00:00', 'to': 'THURSDAY', 'endTime': '17:00:00'},
                    {'from': 'FRIDAY', 'beginTime': '09:00:00', 'to': 'FRIDAY', 'endTime': '17:00:00'}
                ]

    # log_name in the format of "LoanApp_fewRes_9-5_noTimers.CSV"
    calendar_time = log_name.split('_')[2]
    if calendar_time == '9-5':
        return calendars_9_5
    elif calendar_time == '24-7':
        return calendars_24_7
    else:
        raise Exception("Unknown calendar time: {}".format(calendar_time))


@pytest.mark.icpm
@pytest.mark.integration
@pytest.mark.parametrize(
    'test_data', icpm_data,
    ids=map(
        lambda x: f"{x['log_name']}, parallel={x['parallel_run']}, batch_size={x['batch_size']}",
        icpm_data))
def test_handoffs_for_icpm_conference(assets_path, test_data):
    log_path = assets_path / 'icpm/handoff-logs' / test_data['log_name']
    parallel = test_data['parallel_run']
    output_dir = assets_path / 'icpm/handoff-logs'

    log_ids = wta.EventLogIDs()
    log_ids.start_time = 'start_time'
    log_ids.end_time = 'end_time'
    log_ids.case = 'case_id'
    log_ids.activity = 'Activity'
    log_ids.resource = 'Resource'

    calendar = test_data['calendar'] if 'calendar' in test_data else None

    batch_size = test_data['batch_size']

    result = run(log_path, parallel, log_ids=log_ids, calendar=calendar, batch_size=batch_size)

    output_dir.mkdir(parents=True, exist_ok=True)
    extension_suffix = '.csv'

    # test case expected values if available
    expected_path = (assets_path / 'icpm/handoff-logs' / test_data['expected']) if 'expected' in test_data else None
    expected_data = wta.helpers.read_csv(expected_path) if expected_path else None

    # assert
    if expected_data is not None:
        expected_data[log_ids.wt_total] = pd.to_timedelta(expected_data[log_ids.wt_total])
        expected_data[log_ids.wt_batching] = pd.to_timedelta(expected_data[log_ids.wt_batching])
        expected_data[log_ids.wt_contention] = pd.to_timedelta(expected_data[log_ids.wt_contention])
        expected_data[log_ids.wt_prioritization] = pd.to_timedelta(expected_data[log_ids.wt_prioritization])
        expected_data[log_ids.wt_unavailability] = pd.to_timedelta(expected_data[log_ids.wt_unavailability])
        expected_data[log_ids.wt_extraneous] = pd.to_timedelta(expected_data[log_ids.wt_extraneous])

        result['handoff']['cases'] = result['handoff']['cases'].astype(object)
        expected_data['cases'] = expected_data['cases'].astype(object)

        columns_to_compare = [
            'source_activity', 'source_resource', 'destination_activity', 'destination_resource', 'frequency',
            log_ids.wt_total, log_ids.wt_batching, log_ids.wt_contention,
            log_ids.wt_prioritization, log_ids.wt_unavailability, log_ids.wt_extraneous
        ]

        assert result['handoff'][columns_to_compare].equals(expected_data[columns_to_compare])

    # save handoff results
    if test_data.get('save_report'):
        if result['handoff'] is not None:
            handoff_output_path = output_dir / (log_path.stem + '_handoff')
            handoff_csv_path = handoff_output_path.with_suffix(extension_suffix)
            print(f'Saving handoff report to {handoff_csv_path}')
            result['handoff'].to_csv(handoff_csv_path, index=False)
        else:
            print('No handoffs found')
