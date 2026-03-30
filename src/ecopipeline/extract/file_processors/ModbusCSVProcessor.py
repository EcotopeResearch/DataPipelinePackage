import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime, timedelta
import re
from ecopipeline.extract.FileProcessor import FileProcessor


class ModbusCSVProcessor(FileProcessor):
    """FileProcessor for Modbus CSV files (e.g. Acquisuite).

    Reads standard CSVs whose first column is a time(UTC) timestamp. Column names
    are prefixed with the filename stem so data from multiple devices can be merged
    without column collisions. Index is floored to the minute after concat.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 raw_time_column: str = 'time(UTC)', filename_date_format: str = "%Y%m%d%H%M%S",
                 file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3):
        super().__init__(config, ".csv", start_time, end_time,
                         raw_time_column=raw_time_column,
                         filename_date_format=filename_date_format, file_prefix=file_prefix,
                         data_sub_dir=data_sub_dir, date_string_start_idx=date_string_start_idx,
                         date_string_end_idx=date_string_end_idx, round_time_index=True)
        
    def extract_new(self, filenames: list[str]) -> list[str]:
        
        base_date = datetime(1970, 1, 1)
        file_dates = [pd.Timestamp(base_date + timedelta(seconds = int(re.search(r'\.(.*?)_', filename.split("/")[-1]).group(1), 16))) for filename in filenames] #convert decihex to dates, these are in utc
        
        file_dates_local = [file_date.tz_localize('UTC').tz_localize(None) for file_date in file_dates] #convert utc 
        # if timeZone == None:
        #     file_dates_local = [file_date.tz_localize('UTC').tz_localize(None) for file_date in file_dates] #convert utc 
        # else:
        #     file_dates_local = [file_date.tz_localize('UTC').tz_convert(timezone(timeZone)).tz_localize(None) for file_date in file_dates] #convert utc to local zone with no awareness

        return_list = [filename for filename, local_time in zip(filenames, file_dates_local) if local_time > self.start_time and (self.end_time is None or local_time < self.end_time)]
        return return_list

    def _read_file_into_df(self, file_name: str) -> pd.DataFrame:
        data = pd.read_csv(file_name)
        if len(data) != 0:
            if self.raw_time_column in data.columns:
                prefix = file_name.split('/')[-1].split('.')[0]
                data[self.raw_time_column] = pd.to_datetime(data[self.raw_time_column])
                data = data.set_index(self.raw_time_column)
                data = data.rename(columns={col: f"{prefix}_{col}".replace(' ', '_') for col in data.columns})
            else:
                print(f"Error reading {file_name}: No '{self.raw_time_column}' column found.")
                return pd.DataFrame()
        return data
