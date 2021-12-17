from pathlib import Path

import click

from waste import handoff, pingpong as pp


@click.group()
def main():
    pass


@main.command()
@click.option('-l', '--log_path', default=None, required=True, type=Path, help='Path to an event log in XES-format.')
@click.option('-o', '--output_dir', default='./', show_default=True, type=Path,
              help='Path to an output directory where statistics will be saved.')
@click.option('-p', '--parallel', is_flag=True, help='Run the tool using all available cores in parallel.')
def handoff(log_path, output_dir, parallel):
    _call_cmd(log_path, output_dir, parallel, handoff.identify, '_handoff', '.csv')


@main.command()
@click.option('-l', '--log_path', default=None, required=True, type=Path, help='Path to an event log in XES-format.')
@click.option('-o', '--output_dir', default='./', show_default=True, type=Path,
              help='Path to an output directory where statistics will be saved.')
@click.option('-p', '--parallel', is_flag=True, help='Run the tool using all available cores in parallel.')
def pingpong(log_path, output_dir, parallel):
    _call_cmd(log_path, output_dir, parallel, pp.identify, '_pingpong', '.csv')


def _call_cmd(log_path, output_dir, parallel, fn, name_suffix, extension_suffix):
    log_path = Path(log_path)
    output_dir = Path(output_dir)

    result = fn(log_path, parallel)
    if result is None:
        print('Empty result')
        return

    output_path = output_dir / (log_path.stem + name_suffix)
    csv_path = output_path.with_suffix(extension_suffix)
    print(f'Saving results to {csv_path}')
    result.to_csv(csv_path, index=False)


if __name__ == '__main__':
    main()
