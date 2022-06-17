import pytest

from process_waste import WAITING_TIME_BATCHING_KEY, default_log_ids
from process_waste.transportation import identify


# @pytest.mark.integration
# def test_identify(assets_path):
#     log_path = assets_path / 'PurchasingExampleModified.csv'
#     result = identify(log_path, parallel_run=True, log_ids=default_log_ids)
#
#     assert result is not None
#     assert result['handoff'] is not None
#     assert result['pingpong'] is not None
#
#     assert WAITING_TIME_BATCHING_KEY in result['handoff'].columns
#     assert not result['handoff'][WAITING_TIME_BATCHING_KEY].isna().all()
#     assert not result['handoff'][WAITING_TIME_BATCHING_KEY].isnull().all()
