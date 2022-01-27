from pathlib import Path

import click

from . import transportation as tp


@click.group()
def main():
    """Grouping commands under main group."""
    pass


@main.command()
@click.option('-l', '--log_path', default=None, required=True, type=Path,
              help='Path to an event log in XES-format.')
@click.option('-o', '--output_dir', default='./', show_default=True, type=Path,
              help='Path to an output directory where statistics will be saved.')
@click.option('-p', '--parallel', is_flag=True, default=True, show_default=True,
              help='Run the tool using all available cores in parallel.')
def transportation(log_path: Path, output_dir: Path, parallel: bool):
    result = tp.identify(log_path, parallel)

    output_dir.mkdir(parents=True, exist_ok=True)

    extension_suffix = '.csv'

    # handoff
    if result['handoff'] is not None:
        handoff_output_path = output_dir / (log_path.stem + '_handoff')
        handoff_csv_path = handoff_output_path.with_suffix(extension_suffix)
        print(f'Saving handoff report to {handoff_csv_path}')
        result['handoff'].to_csv(handoff_csv_path, index=False)
    else:
        print('No handoffs found')

    # pingpong
    if result['pingpong'] is not None:
        pingpong_output_path = output_dir / (log_path.stem + '_pingpong')
        pingpong_csv_path = pingpong_output_path.with_suffix(extension_suffix)
        print(f'Saving pingpong report to {pingpong_csv_path}')
        result['pingpong'].to_csv(pingpong_csv_path, index=False)
    else:
        print('No pingpongs found')


if __name__ == '__main__':
    main()
