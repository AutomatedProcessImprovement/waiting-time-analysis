import pandas as pd
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

    # per case data check

    per_case_wt = pd.DataFrame(columns=[log_ids.case, log_ids.wt_total, log_ids.pt_total, log_ids.cte_impact])

    for (case_id, case_log) in report.log.groupby(by=[log_ids.case]):
        case_pt = (case_log[log_ids.end_time] - case_log[log_ids.start_time]).sum()
        case_wt = case_log[log_ids.wt_total].sum()
        case_cte = case_pt / (case_pt + case_wt)

        per_case_wt = per_case_wt.append({
            log_ids.case: case_id,
            log_ids.wt_total: case_wt.total_seconds(),
            log_ids.pt_total: case_pt.total_seconds(),
            log_ids.cte_impact: case_cte,
        }, ignore_index=True)

    assert ((report.per_case_wt == per_case_wt).all()).all()
