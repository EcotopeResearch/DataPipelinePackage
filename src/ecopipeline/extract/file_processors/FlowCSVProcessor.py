import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class FlowCSVProcessor(FileProcessor):
    """FileProcessor for flow meter CSV files.

    Flow files have 6 header rows to skip. The timestamp is reconstructed from
    individual Year, Month, Day, Hour, Minute, Second columns.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3):
        super().__init__(config, ".csv", start_time, end_time,
                         filename_date_format=filename_date_format, file_prefix=file_prefix,
                         data_sub_dir=data_sub_dir, date_string_start_idx=date_string_start_idx,
                         date_string_end_idx=date_string_end_idx, round_time_index=True)

    def _read_file_into_df(self, file_name: str) -> pd.DataFrame:
        data = pd.read_csv(file_name, skiprows=6)
        if len(data) != 0:
            required = ['Month', 'Day', 'Year', 'Hour', 'Minute', 'Second']
            if all(col in data.columns.tolist() for col in required):
                date_str = (
                    data['Year'].astype(str) + '-' +
                    data['Month'].astype(str).str.zfill(2) + '-' +
                    data['Day'].astype(str).str.zfill(2) + ' ' +
                    data['Hour'].astype(str).str.zfill(2) + ':' +
                    data['Minute'].astype(str).str.zfill(2) + ':' +
                    data['Second'].astype(str).str.zfill(2)
                )
                data['time_pt'] = pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S')
                data = data.set_index('time_pt')
            else:
                print(f"Error reading {file_name}: No time columns found.")
                return pd.DataFrame()
        return data
