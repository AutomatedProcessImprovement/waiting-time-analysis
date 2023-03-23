import json
import tempfile
from pathlib import Path

import pytest

from wta import cli, EventLogIDs

test_data = [
    {
        'log_name': 'Production.csv',
        'parallel': False,
        'columns': {
            'case': 'case:concept:name',
            'activity': 'concept:name',
            'resource': 'org:resource',
            'start_timestamp': 'start_timestamp',
            'end_timestamp': 'time:timestamp',
        },
    }
]


@pytest.mark.integration
@pytest.mark.parametrize('test_data', test_data, ids=map(lambda x: x['log_name'], test_data))
def test_main(assets_path, test_data):
    with tempfile.TemporaryDirectory() as output_dir:
        log_path = assets_path / test_data['log_name']
        output_path = Path(output_dir)
        parallel = test_data['parallel']
        log_ids = EventLogIDs.from_dict(test_data['columns'])

        cli._run(log_path, parallel, log_ids, output_path)

        assert (output_path / (log_path.stem + '_transitions_report.csv')).exists()
