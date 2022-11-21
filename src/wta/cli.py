import json
from pathlib import Path
from typing import Optional

import click

from wta import EventLogIDs
from wta.main import run
from wta.transitions_report import TransitionsReport


@click.command()
@click.option('-l', '--log_path', default=None, required=False, type=Path,
              help='Path to an event log in XES-format.')
@click.option('-o', '--output_dir', default='./', show_default=True, type=Path,
              help='Path to an output directory where statistics will be saved.')
@click.option('-p', '--parallel/--no-parallel', is_flag=True, default=True, show_default=True,
              help='Run the tool using all available cores in parallel.')
@click.option('-c', '--columns_path', default=None, type=click.Path(exists=True, path_type=Path),
              help="Path to a JSON file containing column mappings for the event log. Only the following keys"
                   "are accepted: case, activity, resource, start_timestamp, end_timestamp.")
@click.option('-m', '--columns_json', default=None, type=str,
              help="JSON string containing column mappings for the event log.")
@click.option('-v', '--version', is_flag=True, default=False, show_default=True,
              help='Print the version of the tool.')
def main(
        log_path: Path,
        output_dir: Path,
        parallel: bool,
        columns_path: Optional[Path],
        columns_json: Optional[str],
        version: bool):
    if version:
        from wta import __version__
        click.echo(f'Waiting Time Analyzer v{__version__}')
        return

    log_ids = __column_mapping(columns_path, columns_json)

    result: TransitionsReport = run(log_path=log_path, parallel_run=parallel, log_ids=log_ids)

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


def __column_mapping(columns_path: Optional[Path], columns_json: Optional[str]) -> Optional[EventLogIDs]:
    log_ids: Optional[EventLogIDs] = None

    if columns_path is not None:
        with columns_path.open('r') as f:
            data = json.load(f)
            log_ids = EventLogIDs.from_dict(data)
    elif columns_json is not None:
        log_ids = EventLogIDs.from_json(columns_json)

    return log_ids


if __name__ == '__main__':
    main()
