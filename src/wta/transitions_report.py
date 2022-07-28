import json
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from wta import EventLogIDs, get_total_processing_time, CTEImpactAnalysis, calculate_cte_impact


class TransitionsReport:
    transitions_report: pd.DataFrame
    report: List[Dict[str, Any]]

    # General statistics

    num_cases: int
    num_activities: int
    num_activity_instances: int
    num_transitions: int
    num_transition_instances: int
    total_pt: float

    # Waiting time

    total_wt: float
    total_batching_wt: float
    total_prioritization_wt: float
    total_contention_wt: float
    total_unavailability_wt: float
    total_extraneous_wt: float

    # CTE impact

    process_cte: float
    cte_impact: CTEImpactAnalysis

    def __init__(self, transitions_report: pd.DataFrame, log: pd.DataFrame, log_ids: EventLogIDs):

        self.transitions_report = transitions_report.rename(columns={'destination_activity': 'target_activity',
                                                                     'destination_resource': 'target_resource'})

        # General statistics

        self.num_cases = log[log_ids.case].nunique()
        self.num_activities = log[log_ids.activity].nunique()
        self.num_activity_instances = len(log[log_ids.activity])
        self.num_transitions = len(self.transitions_report.groupby(by=['source_activity', 'target_activity']))
        self.num_transition_instances = self.transitions_report['frequency'].sum()
        self.total_pt = get_total_processing_time(log, log_ids).total_seconds()

        # Waiting time

        self.total_wt = self.transitions_report[log_ids.wt_total].sum().total_seconds()
        self.total_batching_wt = self.transitions_report[log_ids.wt_batching].sum().total_seconds()
        self.total_prioritization_wt = self.transitions_report[log_ids.wt_prioritization].sum().total_seconds()
        self.total_contention_wt = self.transitions_report[log_ids.wt_contention].sum().total_seconds()
        self.total_unavailability_wt = self.transitions_report[log_ids.wt_unavailability].sum().total_seconds()
        self.total_extraneous_wt = self.transitions_report[log_ids.wt_extraneous].sum().total_seconds()

        # CTE impact

        self.process_cte = self.total_pt / (self.total_pt + self.total_wt)
        self.cte_impact = calculate_cte_impact(transitions_report, self.total_pt, self.total_wt, log_ids=log_ids)

        # Regroup the entries in the report

        self.report = self.__regroup_report(log_ids)

    def __regroup_report(self, log_ids) -> List[Dict[str, Any]]:
        new_report = []

        for (activities, report) in self.transitions_report.groupby(by=['source_activity', 'target_activity']):
            wt_by_resource = []

            for (resources, resources_report) in report.groupby(by=['source_resource', 'target_resource']):
                wt_by_resource.append({
                    'source_resource': resources[0],
                    'target_resource': resources[1],
                    'case_freq': len(resources_report['cases'].values[0].split(',')) / self.num_cases,
                    'total_freq': resources_report['frequency'].sum(),
                    'total_wt': resources_report[log_ids.wt_total].sum().total_seconds(),
                    'batching_wt': resources_report[log_ids.wt_batching].sum().total_seconds(),
                    'prioritization_wt': resources_report[log_ids.wt_prioritization].sum().total_seconds(),
                    'contention_wt': resources_report[log_ids.wt_contention].sum().total_seconds(),
                    'unavailability_wt': resources_report[log_ids.wt_unavailability].sum().total_seconds(),
                    'extraneous_wt': resources_report[log_ids.wt_extraneous].sum().total_seconds(),
                })

            cte_impact_total = \
                self.total_pt / (self.total_pt + self.total_wt - report[log_ids.wt_total].sum().total_seconds())
            cte_impact = calculate_cte_impact(report, self.total_pt, self.total_wt, log_ids=log_ids)

            new_report.append({
                'source_activity': activities[0],
                'target_activity': activities[1],
                'case_freq': len(report['cases'].values[0].split(',')) / self.num_cases,
                'total_freq': report['frequency'].sum(),
                'total_wt': report[log_ids.wt_total].sum().total_seconds(),
                'batching_wt': report[log_ids.wt_batching].sum().total_seconds(),
                'prioritization_wt': report[log_ids.wt_prioritization].sum().total_seconds(),
                'contention_wt': report[log_ids.wt_contention].sum().total_seconds(),
                'unavailability_wt': report[log_ids.wt_unavailability].sum().total_seconds(),
                'extraneous_wt': report[log_ids.wt_extraneous].sum().total_seconds(),
                'cte_impact_total_wt': cte_impact_total,
                'cte_impact': cte_impact.to_dict(),
                'wt_by_resource': wt_by_resource,
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
                'total_pt': self.total_pt,
                'total_wt': self.total_wt,
                'total_batching_wt': self.total_batching_wt,
                'total_prioritization_wt': self.total_prioritization_wt,
                'total_contention_wt': self.total_contention_wt,
                'total_unavailability_wt': self.total_unavailability_wt,
                'total_extraneous_wt': self.total_extraneous_wt,
                'process_cte': self.process_cte,
                'cte_impact': self.cte_impact.to_dict(),
                'report': self.report,
            }
            f.write(json.dumps(data))
