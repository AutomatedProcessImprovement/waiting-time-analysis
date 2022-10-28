import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from wta.helpers import log_ids_non_nil, EventLogIDs


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

    def to_dict(self):
        return {
            'batching_impact': self.batching_impact,
            'contention_impact': self.contention_impact,
            'prioritization_impact': self.prioritization_impact,
            'unavailability_impact': self.unavailability_impact,
            'extraneous_impact': self.extraneous_impact,
        }

    def to_json_string(self):
        """Return CTE impact analysis as JSON string."""
        return json.dumps(self.to_dict())


def calculate_cte_impact(
        report: pd.DataFrame,
        total_processing_time: float,
        total_waiting_time: float,
        log_ids: Optional[EventLogIDs] = None) -> CTEImpactAnalysis:
    """Calculates impact of waiting time on cycle time efficiency."""
    log_ids = log_ids_non_nil(log_ids)

    # NOTE: all times should be in seconds, not pd.Timedelta objects
    total_wt_batching = report[log_ids.wt_batching].sum()
    total_wt_prioritization = report[log_ids.wt_prioritization].sum()
    total_wt_contention = report[log_ids.wt_contention].sum()
    total_wt_unavailability = report[log_ids.wt_unavailability].sum()
    total_wt_extraneous = report[log_ids.wt_extraneous].sum()

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

    return result
