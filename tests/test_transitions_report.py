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
    {'log_name': 'column_mapping.csv',
     'parallel_run': False,
     'batch_size': 10,
     'save_report': True,
     'log_ids': EventLogIDs(
         start_time='Start_time',
         end_time='Finish_time',
         case='Contract_ID',
         resource='Employee',
         activity='Activity_name',
     )},
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
    assert report.total_wt == report.transitions_report[log_ids.wt_total].sum()

    # per case data check

    case_column = 'case_id'
    wt_total_column = 'wt_total'
    pt_total_column = 'pt_total'
    cte_impact_column = 'cte_impact'

    per_case_wt = pd.DataFrame(columns=[case_column, wt_total_column, pt_total_column, cte_impact_column])

    for (case_id, case_log) in report.log.groupby(by=[log_ids.case]):
        case_pt = (case_log[log_ids.end_time] - case_log[log_ids.start_time]).sum()
        case_wt = case_log[log_ids.wt_total].sum()
        case_cte = case_pt / (case_pt + case_wt)

        per_case_wt = per_case_wt.append({
            case_column: case_id,
            wt_total_column: case_wt.total_seconds(),
            pt_total_column: case_pt.total_seconds(),
            cte_impact_column: case_cte,
        }, ignore_index=True)

    assert ((report.per_case_wt == per_case_wt).all()).all()
