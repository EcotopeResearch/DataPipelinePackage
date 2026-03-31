import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class MSACSVProcessor(FileProcessor):
    """FileProcessor for MSA CSV files.

    MSA files contain a ``'DateEpoch(secs)'`` Unix-epoch column that is
    converted from UTC to the configured timezone.  When ``mb_prefix`` is
    ``True``, all data columns are prefixed with the filename stem so that
    data from multiple devices can be merged without column collisions.
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
    mb_prefix : bool, optional
        If ``True``, prefix every column name with the filename stem.
        Defaults to ``False``.
    time_zone : str, optional
        Timezone name used to convert UTC epoch timestamps to local time.
        Defaults to ``'US/Pacific'``.
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
        """Read a single MSA CSV file and set a ``time_pt`` datetime index.

        Converts the ``'DateEpoch(secs)'`` column from UTC to ``time_zone``,
        drops timezone awareness, removes the epoch column, and optionally
        prefixes all remaining columns with the filename stem.

        Parameters
        ----------
        file_name : str
            Absolute path to the MSA ``.csv`` file to read.

        Returns
        -------
        pd.DataFrame
            DataFrame indexed by ``time_pt``.  Returns an empty DataFrame when
            the file contains no rows.
        """
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
