import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class DentCSVProcessor(FileProcessor):
    """FileProcessor for DENT CSV files.

    DENT files have 12 header rows to skip. The first data column is a throwaway,
    and columns 2 and 3 are the date and time strings that are combined into a
    time_pt index.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3):
        super().__init__(config, ".csv", start_time, end_time,
                         filename_date_format=filename_date_format, file_prefix=file_prefix,
                         data_sub_dir=data_sub_dir, date_string_start_idx=date_string_start_idx,
                         date_string_end_idx=date_string_end_idx, round_time_index=True)

    def _read_file_into_df(self, file_name: str) -> pd.DataFrame:
        data = pd.read_csv(file_name, skiprows=12)
        if len(data) != 0:
            if len(data.columns) >= 3:
                data.columns = ['temp', 'date', 'time'] + data.columns.tolist()[3:]
                data = data.drop(columns=['temp'])
                data['time_pt'] = pd.to_datetime(data['date'] + ' ' + data['time'])
                data = data.set_index('time_pt')
            else:
                print(f"Error reading {file_name}: No time columns found.")
                return pd.DataFrame()
        return data
