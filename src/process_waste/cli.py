from pathlib import Path

import click
import pandas as pd

from process_waste.cte_impact import CTEImpactAnalysis
from process_waste.main import run


@click.command()
@click.option('-l', '--log_path', default=None, required=True, type=Path,
              help='Path to an event log in XES-format.')
@click.option('-o', '--output_dir', default='./', show_default=True, type=Path,
              help='Path to an output directory where statistics will be saved.')
@click.option('-p', '--parallel', is_flag=True, default=True, show_default=True,
              help='Run the tool using all available cores in parallel.')
def main(log_path: Path, output_dir: Path, parallel: bool):
    result = run(log_path, parallel)

    output_dir.mkdir(parents=True, exist_ok=True)
    extension_suffix = '.csv'

    handoff_report: pd.DataFrame = result.get('handoff')
    process_cte_impact: CTEImpactAnalysis = result.get('process_cte_impact')

    if handoff_report is not None:
        handoff_output_path = output_dir / (log_path.stem + '_handoff')

        handoff_csv_path = handoff_output_path.with_suffix(extension_suffix)
        print(f'Saving handoff report to {handoff_csv_path}')
        handoff_report.to_csv(handoff_csv_path, index=False)

        handoff_json_path = handoff_output_path.with_suffix('.json')
        print(f'Saving handoff report to {handoff_json_path}')
        handoff_report.to_json(handoff_json_path, orient='records')
    else:
        print('No handoffs found')

    if process_cte_impact:
        process_cte_impact_output_path = output_dir / (log_path.stem + '_process_cte_impact')
        process_cte_impact_json_path = process_cte_impact_output_path.with_suffix('.json')
        print(f'Saving process CTE impact report to {process_cte_impact_json_path}')
        process_cte_impact.to_json(process_cte_impact_json_path)


if __name__ == '__main__':
    main()
