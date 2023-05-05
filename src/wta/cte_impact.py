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

    total_wt = report[[log_ids.wt_batching, log_ids.wt_prioritization, log_ids.wt_contention, log_ids.wt_unavailability, log_ids.wt_extraneous]].sum()

    # Precompute the sum of total_processing_time and total_waiting_time
    total_time_sum = total_processing_time + total_waiting_time

    impact = total_processing_time / (total_time_sum - total_wt)

    result = CTEImpactAnalysis(
        batching_impact=impact[log_ids.wt_batching],
        prioritization_impact=impact[log_ids.wt_prioritization],
        contention_impact=impact[log_ids.wt_contention],
        unavailability_impact=impact[log_ids.wt_unavailability],
        extraneous_impact=impact[log_ids.wt_extraneous])

    return result
