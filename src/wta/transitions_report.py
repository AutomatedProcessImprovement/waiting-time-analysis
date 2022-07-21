import json
from functools import reduce
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from wta import EventLogIDs


class TransitionsReport:
    transitions_report: pd.DataFrame
    report: List[Dict[str, Any]]
    num_cases: int
    num_activities: int
    num_activity_instances: int
    num_transitions: int
    num_transition_instances: int
    total_wt: float
    total_batching_wt: float
    total_prioritization_wt: float
    total_contention_wt: float
    total_unavailability_wt: float
    total_extraneous_wt: float

    def __init__(self, transitions_report: pd.DataFrame, log: pd.DataFrame, log_ids: EventLogIDs):
        self.transitions_report = transitions_report
        self.num_cases = log[log_ids.case].nunique()
        self.num_activities = log[log_ids.activity].nunique()
        self.num_activity_instances = len(log[log_ids.activity])
        self.num_transitions = len(transitions_report)
        self.num_transition_instances = self.transitions_report['frequency'].sum()

        self.report = self.__regroup_report(log_ids)

        self.total_wt = self.transitions_report[log_ids.wt_total].sum().total_seconds()
        self.total_batching_wt = self.transitions_report[log_ids.wt_batching].sum().total_seconds()
        self.total_prioritization_wt = self.transitions_report[log_ids.wt_prioritization].sum().total_seconds()
        self.total_contention_wt = self.transitions_report[log_ids.wt_contention].sum().total_seconds()
        self.total_unavailability_wt = self.transitions_report[log_ids.wt_unavailability].sum().total_seconds()
        self.total_extraneous_wt = self.transitions_report[log_ids.wt_extraneous].sum().total_seconds()

    def __regroup_report(self, log_ids) -> List[Dict[str, Any]]:
        new_report = []

        for (activities, report) in self.transitions_report.groupby(by=['source_activity', 'destination_activity']):
            wt_by_resource = []

            for (resources, resources_report) in report.groupby(by=['source_resource', 'destination_resource']):
                wt_by_resource.append({
                    'source_resource': resources[0],
                    'destination_resource': resources[1],
                    'case_freq': len(resources_report['cases'].values[0].split(',')),
                    'total_freq': resources_report['frequency'].sum(),
                    'total_wt': resources_report[log_ids.wt_total].sum().total_seconds(),
                    'batching_wt': resources_report[log_ids.wt_batching].sum().total_seconds(),
                    'prioritization_wt': resources_report[log_ids.wt_prioritization].sum().total_seconds(),
                    'contention_wt': resources_report[log_ids.wt_contention].sum().total_seconds(),
                    'unavailability_wt': resources_report[log_ids.wt_unavailability].sum().total_seconds(),
                    'extraneous_wt': resources_report[log_ids.wt_extraneous].sum().total_seconds(),
                })

            new_report.append({
                'source_activity': activities[0],
                'destination_activity': activities[1],
                'case_freq': len(report['cases'].values[0].split(',')),
                'total_freq': report['frequency'].sum(),
                'total_wt': report[log_ids.wt_total].sum().total_seconds(),
                'batching_wt': report[log_ids.wt_batching].sum().total_seconds(),
                'prioritization_wt': report[log_ids.wt_prioritization].sum().total_seconds(),
                'contention_wt': report[log_ids.wt_contention].sum().total_seconds(),
                'unavailability_wt': report[log_ids.wt_unavailability].sum().total_seconds(),
                'extraneous_wt': report[log_ids.wt_extraneous].sum().total_seconds(),
                'wt_by_resource': wt_by_resource
            })

        return new_report

    def to_json(self, filepath: Path):
        with filepath.open('w') as f:
            data = {
                'num_cases': self.num_cases,
                'num_activities': self.num_activities,
                'num_activity_instances': self.num_activity_instances,
                'num_transitions': self.num_transitions,
                'num_transition_instances': self.num_transition_instances,
                'total_wt': self.total_wt,
                'total_batching_wt': self.total_batching_wt,
                'total_prioritization_wt': self.total_prioritization_wt,
                'total_contention_wt': self.total_contention_wt,
                'total_unavailability_wt': self.total_unavailability_wt,
                'total_extraneous_wt': self.total_extraneous_wt,
                'report': self.report,
            }
            f.write(json.dumps(data))
