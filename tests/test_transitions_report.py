import pytest

from wta import EventLogIDs, read_csv
from wta.main import run
from wta.transitions_report import TransitionsReport

test_cases = [
    {'log_name': 'icpm/handoff-logs/handoff-test.csv',
     'parallel_run': True,
     'batch_size': 10,
     'save_report': False,
     'log_ids': EventLogIDs(
             start_time='start_time',
             end_time='end_time',
             case='case_id',
             resource='Resource',
             activity='Activity',
         )},
    # {'log_name': 'PurchasingExample.csv',
    #  'parallel_run': True,
    #  'batch_size': 10,
    #  'save_report': False,
    #  'log_ids': EventLogIDs(
    #      start_time='start_timestamp',
    #      end_time='time:timestamp',
    #      case='concept:name',
    #      resource='Resource',
    #      activity='Activity',
    #  )},
]

@pytest.mark.parametrize('test_data', test_cases, ids=map(lambda x: x['log_name'], test_cases))
def test_transitions_report(assets_path, test_data):
    log_path = assets_path / test_data['log_name']
    parallel = test_data['parallel_run']
    log_ids = test_data['log_ids']
    batch_size = test_data['batch_size']

    report: TransitionsReport = run(log_path, parallel, log_ids=log_ids, batch_size=batch_size)

    assert report is not None

    log = read_csv(log_path, log_ids=log_ids)
    assert report.num_cases == len(log[log_ids.case].unique())
    assert report.num_activities == len(log[log_ids.activity].unique())
    assert report.num_activity_instances == len(log)
    assert report.num_transitions == len(report.report)
    assert report.num_transition_instances == report.transitions_report['frequency'].sum()
    assert report.total_wt == report.transitions_report[log_ids.wt_total].sum().total_seconds()