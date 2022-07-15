from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from batch_processing_analysis.config import EventLogIDs
from process_waste.helpers import WAITING_TIME_TOTAL_KEY, WAITING_TIME_BATCHING_KEY, WAITING_TIME_CONTENTION_KEY, \
    WAITING_TIME_PRIORITIZATION_KEY, WAITING_TIME_UNAVAILABILITY_KEY, WAITING_TIME_EXTRANEOUS_KEY, CTE_IMPACT_KEY, \
    log_ids_non_nil


@dataclass
class CTEImpactAnalysis:
    """Cycle time efficiency impact analysis."""
    batching_impact: float
    contention_impact: float
    prioritization_impact: float
    unavailability_impact: float
    extraneous_impact: float

    def to_json(self, filepath: Path):
        """Write CTE impact analysis to JSON file."""
        with filepath.open('w') as f:
            f.write(self.to_json_string())

    def to_json_string(self):
        """Return CTE impact analysis as JSON string."""
        return f'{{\n' \
               f'    "batching_impact": {self.batching_impact},\n' \
               f'    "contention_impact": {self.contention_impact},\n' \
               f'    "prioritization_impact": {self.prioritization_impact},\n' \
               f'    "unavailability_impact": {self.unavailability_impact},\n' \
               f'    "extraneous_impact": {self.extraneous_impact}\n' \
               f'}}'


def calculate_cte_impact(handoff_report, log: pd.DataFrame, log_ids: Optional[EventLogIDs] = None) -> CTEImpactAnalysis:
    """Calculates CTE impact of different types of wait time on the process level and transitions level."""
    log_ids = log_ids_non_nil(log_ids)

    # global CTE impact

    total_processing_time = log[log_ids.end_time].max() - log[log_ids.start_time].min()
    total_waiting_time = handoff_report[WAITING_TIME_TOTAL_KEY].sum()
    total_wt_batching = handoff_report[WAITING_TIME_BATCHING_KEY].sum()
    total_wt_prioritization = handoff_report[WAITING_TIME_PRIORITIZATION_KEY].sum()
    total_wt_contention = handoff_report[WAITING_TIME_CONTENTION_KEY].sum()
    total_wt_unavailability = handoff_report[WAITING_TIME_UNAVAILABILITY_KEY].sum()
    total_wt_extraneous = handoff_report[WAITING_TIME_EXTRANEOUS_KEY].sum()

    batching_impact = total_processing_time / (total_processing_time + total_waiting_time - total_wt_batching)
    contention_impact = total_processing_time / (total_processing_time + total_waiting_time - total_wt_contention)
    prioritization_impact = total_processing_time / (
            total_processing_time + total_waiting_time - total_wt_prioritization)
    unavailability_impact = total_processing_time / (
            total_processing_time + total_waiting_time - total_wt_unavailability)
    extraneous_impact = total_processing_time / (total_processing_time + total_waiting_time - total_wt_extraneous)

    result = CTEImpactAnalysis(
        batching_impact=batching_impact,
        contention_impact=contention_impact,
        prioritization_impact=prioritization_impact,
        unavailability_impact=unavailability_impact,
        extraneous_impact=extraneous_impact)

    # transitions CTE impact

    handoff_report[CTE_IMPACT_KEY] = total_processing_time / (
            total_processing_time + total_waiting_time - handoff_report[WAITING_TIME_TOTAL_KEY])

    return result
