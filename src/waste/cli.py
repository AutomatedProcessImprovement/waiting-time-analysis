from pathlib import Path

import click
import numpy as np

from waste import core, handoff


@click.command()
@click.option('-l', '--log_path', default=None, required=True, type=Path,
              help='Path to an event log in XES-format.')
@click.option('-o', '--output_dir', default='./', show_default=True, type=Path,
              help='Path to an output directory where statistics will be saved.')
def main(log_path, output_dir):
    log_path = Path(log_path)
    output_dir = Path(output_dir)

    # hand-off identification
    result = handoff.identify(log_path)
    # saving results
    output_path = output_dir / log_path.name
    csv_path = output_path.with_suffix('.csv')
    print(f'Saving results to {csv_path}')
    result.to_csv(csv_path)


# DONE: Sequential events
#   - identical dates
#   - parallel gateways (enabled timestamp)
#   - modify event log to create a parallel or overlapping activity by time

# DONE: Handoff types, can we discover resources, calendars (identify human, system, internal or external resource)?
# We can't discover, but we can label it manually.

# DONE: Separate dataframe for metrics

# NOTE: Requirements:
#   - only interval event logs, i.e., event logs where each event has a start timestamp and a completion timestamp

# TODO: Ping-pong handoff is not identified yet
# TODO: CTE, processing time / full time
#   - Do we count only business hours? Yes. Using 24 hours for PurchasingExample.xes
# DONE: Add total handoff frequency, frequency per case using unique pairs source+resource
# TODO: Mark manually some events with resource_type label "system" to add handoff type identification

# TODO: Production.xes fails with the current approach

if __name__ == '__main__':
    main()
