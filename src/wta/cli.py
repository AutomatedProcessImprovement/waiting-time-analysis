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
    result: TransitionsReport = run(log_path, parallel)

    output_dir.mkdir(parents=True, exist_ok=True)
    extension_suffix = '.csv'

    if result is not None:
        output_path = output_dir / (log_path.stem + '_transitions_report')

        csv_path = output_path.with_suffix(extension_suffix)
        print(f'Saving transitions report to {csv_path}')
        result.transitions_report.to_csv(csv_path, index=False)

        json_path = output_path.with_suffix('.json')
        print(f'Saving transitions report to {json_path}')
        result.to_json(json_path)


if __name__ == '__main__':
    main()
