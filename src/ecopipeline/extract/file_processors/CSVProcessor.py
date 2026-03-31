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
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3):
        super().__init__(config, ".csv", start_time, end_time, raw_time_column, time_column_format, filename_date_format,
                         file_prefix, data_sub_dir, date_string_start_idx, date_string_end_idx)
        
    def _read_file_into_df(self, file_name : str) -> pd.DataFrame:
        data = super()._read_file_into_df(file_name)
        if len(data) != 0:
            data['time_pt'] = pd.to_datetime(data[self.raw_time_column], format=self.time_column_format)
            data.set_index('time_pt', inplace=True)
                
        return data