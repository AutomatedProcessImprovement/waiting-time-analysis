import pandas as pd

# from batch_processing_analysis.batch_processing_analysis import BatchProcessingAnalysis
# from batch_processing_analysis.config import Configuration, EventLogIDs
from process_waste.core import core


# def test_batch_processing_analysis_pkg(assets_path):
#     log_ids = EventLogIDs()
#     log_ids.start_time = core.START_TIMESTAMP_KEY
#     log_ids.end_time = core.END_TIMESTAMP_KEY
#     log_ids.enabled_time = core.ENABLED_TIMESTAMP_KEY
#     log_ids.resource = core.RESOURCE_KEY
#     log_ids.case = core.CASE_KEY
#     log_ids.activity = core.ACTIVITY_KEY
#
#     config = Configuration()
#     config.log_ids = log_ids
#     config.PATH_LOGS_FOLDER = assets_path
#     preprocessed_log_path = config.PATH_LOGS_FOLDER.joinpath("PurchasingExample.csv")
#
#     # Read and preprocess event log
#     event_log = pd.read_csv(preprocessed_log_path)
#     event_log[config.log_ids.start_time] = pd.to_datetime(event_log[config.log_ids.start_time], utc=True)
#     event_log[config.log_ids.end_time] = pd.to_datetime(event_log[config.log_ids.end_time], utc=True)
#
#     # Run main analysis
#     batch_event_log = BatchProcessingAnalysis(event_log, config).analyze_batches()
#
#     assert batch_event_log is not None
