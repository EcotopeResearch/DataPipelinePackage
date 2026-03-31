import pandas as pd
import numpy as np
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class EGaugeCSVProcessor(FileProcessor):
    """FileProcessor for eGauge CSV files.

    eGauge files use a 'Date & Time' epoch column (unit=seconds). Column names are
    prefixed with the filename stem. The final dataframe is differenced to convert
    cumulative register values into interval deltas.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3,
                 time_zone: str = 'US/Pacific'):
        self.time_zone = time_zone
        super().__init__(config, ".csv", start_time, end_time,
                         filename_date_format=filename_date_format, file_prefix=file_prefix,
                         data_sub_dir=data_sub_dir, date_string_start_idx=date_string_start_idx,
                         date_string_end_idx=date_string_end_idx, round_time_index=True)

    def _read_file_into_df(self, file_name: str) -> pd.DataFrame:
        data = pd.read_csv(file_name)
        if len(data) != 0:
            prefix = file_name.split('.')[0].split('/')[-1]
            data['time_pt'] = pd.to_datetime(data['Date & Time'], unit='s', utc=True)
            data['time_pt'] = data['time_pt'].dt.tz_convert(self.time_zone).dt.tz_localize(None)
            data.set_index('time_pt', inplace=True)
            data.drop(columns='Date & Time', inplace=True)
            data = data.rename(columns={col: f"{prefix}_{col}".replace(' ', '_').replace('*', '_') for col in data.columns})
            data = data.dropna(how='all')
        return data

    def raw_files_to_df(self, filenames: list[str]) -> pd.DataFrame:
        df = super().raw_files_to_df(filenames)
        if not df.empty:
            df_diff = df - df.shift(1)
            df_diff[df.shift(1).isna()] = np.nan
            df_diff.iloc[0] = np.nan
            return df_diff
        return df
