import os
from pathlib import Path

import pytest


@pytest.fixture(scope='module')
def assets_path():
    if os.path.basename(os.getcwd()) == 'tests':
        return Path('assets')
    else:
        return Path('tests/assets')
