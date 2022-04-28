import pandas as pd
import pytest

from process_waste.calendar.intervals import pd_interval_to_interval
from process_waste.core import core
from process_waste.waiting_time.resource import non_processing_intervals, run_analysis


class TestResource:
    def test_non_processing_intervals(self, assets_path):
        log_path = assets_path / 'non_processing_intervals.csv'
        event_log = core.read_csv(log_path)
        event_index = pd.Index([2])

        result = non_processing_intervals(event_index, event_log)
        expected_interval = pd.Interval(pd.Timestamp('2011-01-01 11:30:00+00:00'),
                                        pd.Timestamp('2011-01-01 12:00:00+00:00'))
        expected_interval = pd_interval_to_interval(expected_interval)
        assert result == expected_interval

    def test_non_processing_intervals_2(self, assets_path):
        log_path = assets_path / 'non_processing_intervals_2.csv'
        event_log = core.read_csv(log_path)
        event_index = pd.Index([2])

        expected_result = []
        expected_result.extend(pd_interval_to_interval(
            pd.Interval(pd.Timestamp('2011-01-01 09:00:00+00:00'), pd.Timestamp('2011-01-01 10:00:00+00:00'))))
        expected_result.extend(pd_interval_to_interval(
            pd.Interval(pd.Timestamp('2011-01-01 10:30:00+00:00'), pd.Timestamp('2011-01-01 10:45:00+00:00'))))
        expected_result.extend(pd_interval_to_interval(
            pd.Interval(pd.Timestamp('2011-01-01 11:30:00+00:00'), pd.Timestamp('2011-01-01 12:00:00+00:00'))))

        result = non_processing_intervals(event_index, event_log)
        assert result == expected_result

    @pytest.mark.integration
    def test_run_analysis(self, assets_path):
        log_path = assets_path / 'PurchasingExample.csv'
        event_log = core.read_csv(log_path)
        core.add_enabled_timestamp(event_log)
        result = run_analysis(event_log)
        assert result is not None

    def test_run_analysis_2(self, assets_path):
        log_path = assets_path / 'non_processing_intervals.csv'
        event_log = core.read_csv(log_path)
        core.add_enabled_timestamp(event_log)
        result = run_analysis(event_log)
        assert result is not None
