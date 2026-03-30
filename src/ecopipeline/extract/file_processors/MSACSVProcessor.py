import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class MSACSVProcessor(FileProcessor):
    """FileProcessor for MSA CSV files.

    MSA files use a DateEpoch(secs) column for the timestamp, which is converted
    from UTC to the configured time zone. When mb_prefix is True, column names are
    prefixed with the filename stem.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3,
                 mb_prefix: bool = False, time_zone: str = 'US/Pacific'):
        self.mb_prefix = mb_prefix
        self.time_zone = time_zone
        super().__init__(config, ".csv", start_time, end_time,
                         filename_date_format=filename_date_format, file_prefix=file_prefix,
                         data_sub_dir=data_sub_dir, date_string_start_idx=date_string_start_idx,
                         date_string_end_idx=date_string_end_idx, round_time_index=True)

    def _read_file_into_df(self, file_name: str) -> pd.DataFrame:
        data = pd.read_csv(file_name)
        if len(data) != 0:
            data['time_pt'] = pd.to_datetime(data['DateEpoch(secs)'], unit='s', utc=True)
            data['time_pt'] = data['time_pt'].dt.tz_convert(self.time_zone).dt.tz_localize(None)
            data.set_index('time_pt', inplace=True)
            data.drop(columns='DateEpoch(secs)', inplace=True)
            if self.mb_prefix:
                prefix = file_name.split('.')[0].split('/')[-1]
                data = data.rename(columns={col: f"{prefix}{col}".replace(' ', '_').replace('*', '_') for col in data.columns})
        return data
