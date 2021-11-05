import os
from pathlib import Path
from typing import List

import pandas as pd
import pytest


@pytest.fixture(scope='module')
def assets_path():
    if os.path.basename(os.getcwd()) == 'tests':
        return Path('assets')
    else:
        return Path('tests/assets')


@pytest.fixture
def bimp_example_path(assets_path) -> Path:
    return assets_path / 'BIMP_example.xes'


@pytest.fixture
def cases(assets_path) -> List[pd.DataFrame]:
    cases = [
        pd.read_csv(assets_path / 'bimp-example_case_21.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_272.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_293.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_409.csv'),
        pd.read_csv(assets_path / 'bimp-example_case_444.csv'),
    ]

    def _preprocess(case):
        case['start_timestamp'] = pd.to_datetime(case['start_timestamp'])
        case['time:timestamp'] = pd.to_datetime(case['time:timestamp'])
        return case.sort_values(by='time:timestamp')

    return list(map(_preprocess, cases))


@pytest.fixture
def xes_paths(assets_path) -> List[Path]:
    return [
        assets_path / 'BIMP_example.xes',
        assets_path / 'PurchasingExample.xes',
    ]
