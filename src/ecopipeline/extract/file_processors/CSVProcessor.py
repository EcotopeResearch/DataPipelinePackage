import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import datetime, timedelta
from ecopipeline.extract.FileProcessor import FileProcessor

class CSVProcessor(FileProcessor):
    """FileProcessor for generic CSV files with a named timestamp column.

    Reads ``.csv`` files and converts the column identified by
    ``raw_time_column`` into a ``time_pt`` datetime index using
    ``time_column_format``.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    start_time : datetime, optional
        Earliest filename-encoded timestamp to include.
    end_time : datetime, optional
        Latest filename-encoded timestamp to include (exclusive).
    raw_time_column : str, optional
        Name of the column containing timestamp strings.
        Defaults to ``'DateTime'``.
    time_column_format : str, optional
        :func:`datetime.strptime` format used to parse ``raw_time_column``.
        Defaults to ``'%Y/%m/%d %H:%M:%S'``.
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
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None, raw_time_column: str = 'DateTime',
                 time_column_format: str = '%Y/%m/%d %H:%M:%S', filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3):
        super().__init__(config, ".csv", start_time, end_time, raw_time_column, time_column_format, filename_date_format,
                         file_prefix, data_sub_dir, date_string_start_idx, date_string_end_idx)

    def _read_file_into_df(self, file_name : str) -> pd.DataFrame:
        """Read a single CSV file and set a ``time_pt`` datetime index.

        Delegates raw CSV reading to the parent implementation, then parses
        ``raw_time_column`` with ``time_column_format`` and sets the result as
        the DataFrame index.

        Parameters
        ----------
        file_name : str
            Absolute path to the ``.csv`` file to read.

        Returns
        -------
        pd.DataFrame
            DataFrame indexed by ``time_pt``.  Returns an empty DataFrame when
            the file contains no rows.
        """
        data = super()._read_file_into_df(file_name)
        if len(data) != 0:
            data['time_pt'] = pd.to_datetime(data[self.raw_time_column], format=self.time_column_format)
            data.set_index('time_pt', inplace=True)

        return data