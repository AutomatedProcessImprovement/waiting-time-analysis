from pathlib import Path

import click
import numpy as np

from waste import core, handoff


@click.command()
@click.option('-l', '--log_path', default=None, required=True, help='Path to an event log in XES-format.')
@click.option('-o', '--output_dir', default='./', show_default=True,
              help='Path to an output directory where statistics will be saved.')
def main(log_path, output_dir):
    log_path = Path(log_path)
    output_dir = Path(output_dir)

    # hand-off identification
    log = core.lifecycle_to_interval(log_path)
    log_grouped = log.groupby(by='case:concept:name')
    all_handoffs = []
    parallel_activities = core.parallel_activities_with_alpha_oracle(log)
    for (case_id, case) in log_grouped:
        case = case.sort_values(by='start_timestamp')
        handoffs = handoff.identify_handoffs(case, parallel_activities)
        if handoffs is not None:
            all_handoffs.append(handoffs)
    result = handoff.join_handoffs(all_handoffs)

    # saving results
    output_path = output_dir / log_path.name
    csv_path = output_path.with_suffix('.csv')
    print(f'Saving results to {csv_path}')
    result['duration_sum_seconds'] = result['duration_sum'] / np.timedelta64(1, 's')
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

# TODO: concurrency algorithm produces negative duration in BIMP_example

if __name__ == '__main__':
    main()
