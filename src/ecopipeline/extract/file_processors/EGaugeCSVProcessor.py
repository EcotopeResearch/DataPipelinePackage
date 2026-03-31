import pandas as pd
import numpy as np
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class EGaugeCSVProcessor(FileProcessor):
    """FileProcessor for eGauge CSV files.

    eGauge files contain a ``'Date & Time'`` Unix-epoch column (seconds).
    Column names are prefixed with the filename stem so data from multiple
    devices can be merged without collisions.  After concatenation the
    cumulative register values are differenced to produce per-interval deltas.
    The index is floored to the minute and duplicate timestamps are averaged.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    start_time : datetime, optional
        Earliest filename-encoded timestamp to include.
    end_time : datetime, optional
        Latest filename-encoded timestamp to include (exclusive).
    filename_date_format : str, optional
        :func:`datetime.strftime` format for filename date comparison.
        Defaults to ``'%Y%m%d%H%M%S'``.
    file_prefix : str, optional
        Only process files whose names begin with this prefix.
        Defaults to an empty string.
    data_sub_dir : str, optional
        Sub-directory under the configured data directory containing the files.
        Defaults to an empty string.
    date_string_start_idx : int, optional
        Start index (from the end) of the date substring in the filename.
        Defaults to ``-17``.
    date_string_end_idx : int, optional
        End index (from the end) of the date substring in the filename.
        Defaults to ``-3``.
    time_zone : str, optional
        Timezone name used to convert UTC epoch timestamps to local time.
        Defaults to ``'US/Pacific'``.
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
        """Read a single eGauge CSV file and set a ``time_pt`` datetime index.

        Converts the ``'Date & Time'`` Unix-epoch column from UTC to
        ``time_zone``, drops timezone awareness, prefixes all data columns with
        the filename stem, and removes all-NaN rows.

        Parameters
        ----------
        file_name : str
            Absolute path to the eGauge ``.csv`` file to read.

        Returns
        -------
        pd.DataFrame
            DataFrame indexed by ``time_pt`` with prefixed column names.
            Returns an empty DataFrame when the file contains no rows.
        """
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
        """Concatenate files and difference cumulative register values.

        Calls the parent implementation to concatenate all files, then
        computes row-wise differences so cumulative register values are
        converted into per-interval deltas.  The first row is set to ``NaN``
        because its delta cannot be computed.

        Parameters
        ----------
        filenames : list of str
            Absolute file paths to read and concatenate.

        Returns
        -------
        pd.DataFrame
            Differenced DataFrame of interval deltas.  Returns an empty
            DataFrame when no files could be read.
        """
        df = super().raw_files_to_df(filenames)
        if not df.empty:
            df_diff = df - df.shift(1)
            df_diff[df.shift(1).isna()] = np.nan
            df_diff.iloc[0] = np.nan
            return df_diff
        return df
