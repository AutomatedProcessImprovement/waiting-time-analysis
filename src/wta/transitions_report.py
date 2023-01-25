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

    # Per case data

    per_case_wt: pd.DataFrame

    def __init__(self, transitions_report: pd.DataFrame, log: pd.DataFrame, log_ids: EventLogIDs):

        self.log = log

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

        # NOTE: to avoid "overflow in timedelta operation", we convert times to seconds before summing up to convert
        #  pd.Timedelta objects to floats that don't have that limit.
        self.transitions_report[log_ids.wt_total] = \
            self.transitions_report[log_ids.wt_total].dt.total_seconds()
        self.transitions_report[log_ids.wt_batching] = \
            self.transitions_report[log_ids.wt_batching].dt.total_seconds()
        self.transitions_report[log_ids.wt_prioritization] = \
            self.transitions_report[log_ids.wt_prioritization].dt.total_seconds()
        self.transitions_report[log_ids.wt_contention] = \
            self.transitions_report[log_ids.wt_contention].dt.total_seconds()
        self.transitions_report[log_ids.wt_unavailability] = \
            self.transitions_report[log_ids.wt_unavailability].dt.total_seconds()
        self.transitions_report[log_ids.wt_extraneous] = \
            self.transitions_report[log_ids.wt_extraneous].dt.total_seconds()

        self.total_wt = self.transitions_report[log_ids.wt_total].sum()
        self.total_batching_wt = self.transitions_report[log_ids.wt_batching].sum()
        self.total_prioritization_wt = self.transitions_report[log_ids.wt_prioritization].sum()
        self.total_contention_wt = self.transitions_report[log_ids.wt_contention].sum()
        self.total_unavailability_wt = self.transitions_report[log_ids.wt_unavailability].sum()
        self.total_extraneous_wt = self.transitions_report[log_ids.wt_extraneous].sum()

        # CTE impact

        self.process_cte = self.total_pt / (self.total_pt + self.total_wt)

        # Converting timedelta to seconds to avoid overflow error during summation
        transitions_report[log_ids.wt_total] = transitions_report[log_ids.wt_total].dt.total_seconds()
        transitions_report[log_ids.wt_batching] = transitions_report[log_ids.wt_batching].dt.total_seconds()
        transitions_report[log_ids.wt_prioritization] = transitions_report[log_ids.wt_prioritization].dt.total_seconds()
        transitions_report[log_ids.wt_contention] = transitions_report[log_ids.wt_contention].dt.total_seconds()
        transitions_report[log_ids.wt_unavailability] = transitions_report[log_ids.wt_unavailability].dt.total_seconds()
        transitions_report[log_ids.wt_extraneous] = transitions_report[log_ids.wt_extraneous].dt.total_seconds()

        self.cte_impact = calculate_cte_impact(transitions_report, self.total_pt, self.total_wt, log_ids=log_ids)

        # Regroup the entries in the report

        self.report = self.__regroup_report(log_ids)

        # Per case data

        self.__add_per_case_data(log, log_ids)

    def __add_per_case_data(self, log: pd.DataFrame, log_ids: EventLogIDs):
        # NOTE: These columns shouldn't be mapped using log_ids, because the report must have an expected structure
        # for backend to be able to parse it. Column mapping is only for accessing columns in the event log.
        case_column = 'case_id'
        wt_total_column = 'wt_total'
        pt_total_column = 'pt_total'
        cte_impact_column = 'cte_impact'

        per_case_wt = pd.DataFrame(columns=[case_column, wt_total_column, pt_total_column, cte_impact_column])

        for (case_id, case_log) in log.groupby(by=log_ids.case):
            case_pt = (case_log[log_ids.end_time] - case_log[log_ids.start_time]).sum()
            case_wt = case_log[log_ids.wt_total].sum()
            case_cte = case_pt / (case_pt + case_wt)

            # NOTE: we deliberately don't use log_ids for keys below, because the downstream backend service parses
            # the JSON and expects the keys to be well known before. log_ids is used for only accessing data in the log.

            per_case_wt = pd.concat([per_case_wt, pd.DataFrame({
                case_column: [case_id],
                wt_total_column: [case_wt.total_seconds()],
                pt_total_column: [case_pt.total_seconds()],
                cte_impact_column: [case_cte]
            })], ignore_index=True)

        # Converting case_id to string to avoid JSON serialization error
        per_case_wt[case_column] = per_case_wt[case_column].astype(str)

        self.per_case_wt = per_case_wt

    def __regroup_report(self, log_ids) -> List[Dict[str, Any]]:
        new_report = []

        # NOTE: all times should be in seconds, not pd.Timedelta objects
        for (activities, report) in self.transitions_report.groupby(by=['source_activity', 'target_activity']):
            wt_by_resource = []

            for (resources, resources_report) in report.groupby(by=['source_resource', 'target_resource']):
                cte_impact_total = \
                    self.total_pt / (self.total_pt + self.total_wt - resources_report[log_ids.wt_total].sum())
                cte_impact = calculate_cte_impact(resources_report, self.total_pt, self.total_wt, log_ids=log_ids)

                case_freq = len(resources_report['cases'].apply(lambda x: x.split(',')).explode().unique()) / self.num_cases

                wt_by_resource.append({
                    'source_resource': resources[0],
                    'target_resource': resources[1],
                    'case_freq': case_freq,
                    'total_freq': resources_report['frequency'].sum(),
                    'total_wt': resources_report[log_ids.wt_total].sum(),
                    'batching_wt': resources_report[log_ids.wt_batching].sum(),
                    'prioritization_wt': resources_report[log_ids.wt_prioritization].sum(),
                    'contention_wt': resources_report[log_ids.wt_contention].sum(),
                    'unavailability_wt': resources_report[log_ids.wt_unavailability].sum(),
                    'extraneous_wt': resources_report[log_ids.wt_extraneous].sum(),
                    'cte_impact_total': cte_impact_total,
                    'cte_impact': cte_impact.to_dict(),
                })

            cte_impact_total = \
                self.total_pt / (self.total_pt + self.total_wt - report[log_ids.wt_total].sum())
            cte_impact = calculate_cte_impact(report, self.total_pt, self.total_wt, log_ids=log_ids)

            case_freq = len(report['cases'].apply(lambda x: x.split(',')).explode().unique()) / self.num_cases

            new_report.append({
                'source_activity': activities[0],
                'target_activity': activities[1],
                'case_freq': case_freq,
                'total_freq': report['frequency'].sum(),
                'total_wt': report[log_ids.wt_total].sum(),
                'batching_wt': report[log_ids.wt_batching].sum(),
                'prioritization_wt': report[log_ids.wt_prioritization].sum(),
                'contention_wt': report[log_ids.wt_contention].sum(),
                'unavailability_wt': report[log_ids.wt_unavailability].sum(),
                'extraneous_wt': report[log_ids.wt_extraneous].sum(),
                'cte_impact_total': cte_impact_total,
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
                'per_case_wt': self.per_case_wt.to_dict(orient='records'),
            }
            f.write(json.dumps(data))
