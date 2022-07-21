from pathlib import Path

import click

from wta.cte_impact import CTEImpactAnalysis
from wta.main import run
from wta.transitions_report import TransitionsReport


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

    transitions_report: TransitionsReport = result.get('transitions_report')
    process_cte_impact: CTEImpactAnalysis = result.get('process_cte_impact')

    if transitions_report is not None:
        output_path = output_dir / (log_path.stem + '_transitions_report')

        csv_path = output_path.with_suffix(extension_suffix)
        print(f'Saving transitions report to {csv_path}')
        transitions_report.transitions_report.to_csv(csv_path, index=False)

        json_path = output_path.with_suffix('.json')
        print(f'Saving transitions report to {json_path}')
        transitions_report.to_json(json_path)
    else:
        print('No transitions found')

    if process_cte_impact:
        output_path = output_dir / (log_path.stem + '_process_cte_impact')
        json_path = output_path.with_suffix('.json')
        print(f'Saving process CTE impact report to {json_path}')
        process_cte_impact.to_json(json_path)


if __name__ == '__main__':
    main()
