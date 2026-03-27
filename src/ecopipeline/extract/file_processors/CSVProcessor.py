import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import datetime, timedelta
from ecopipeline.extract.FileProcessor import FileProcessor

class CSVProcessor(FileProcessor):
    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None, raw_time_column: str = 'DateTime', 
                 time_column_format: str = '%Y/%m/%d %H:%M:%S', filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "", 
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3, mb_prefix : bool = False, mb_column_overwrite : bool = False):
        self.mb_prefix = mb_prefix
        if self.mb_prefix and raw_time_column != "time(UTC)":
            print(f"File Processor Warning: processing modbus files and raw_time_column set to {raw_time_column}")
            if not mb_column_overwrite:
                print("Reseting raw_time_column to 'time(UTC)'. If this is not correct. rerun file processor with mb_column_overwrite set to True.")
                raw_time_column = "time(UTC)"

        super().__init__(config, ".csv", start_time, end_time, raw_time_column, time_column_format, filename_date_format, 
                         file_prefix, data_sub_dir, date_string_start_idx, date_string_end_idx)
        
    def _read_file_into_df(self, file_name : str) -> pd.DataFrame:
        data = super()._read_file_into_df(file_name)
        if len(data) != 0 and self.mb_prefix:
            if self.raw_time_column in data.columns:
                #prepend modbus prefix
                prefix = file_name.split("/")[-1].split('.')[0]
                data[self.raw_time_column] = pd.to_datetime(data[self.raw_time_column])
                data = data.set_index(self.raw_time_column)
                data = data.rename(columns={col: f"{prefix}_{col}".replace(" ","_") for col in data.columns})
            else:
                print(f"Error reading {file_name}: No 'time(UTC)' column found.")
        return data