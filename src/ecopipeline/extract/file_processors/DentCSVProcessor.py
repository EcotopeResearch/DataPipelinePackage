import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class DentCSVProcessor(FileProcessor):
    """FileProcessor for DENT CSV files.

    DENT files have 12 header rows to skip.  The first data column is a
    throwaway index, and columns 2 and 3 are date and time strings that are
    combined into a ``time_pt`` datetime index.  The index is floored to the
    minute and duplicate timestamps are averaged.

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
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3):
        super().__init__(config, ".csv", start_time, end_time,
                         filename_date_format=filename_date_format, file_prefix=file_prefix,
                         data_sub_dir=data_sub_dir, date_string_start_idx=date_string_start_idx,
                         date_string_end_idx=date_string_end_idx, round_time_index=True)

    def _read_file_into_df(self, file_name: str) -> pd.DataFrame:
        """Read a single DENT CSV file and set a ``time_pt`` datetime index.

        Skips the first 12 header rows, renames the leading throwaway column,
        and builds the ``time_pt`` index by concatenating the ``date`` and
        ``time`` string columns.

        Parameters
        ----------
        file_name : str
            Absolute path to the DENT ``.csv`` file to read.

        Returns
        -------
        pd.DataFrame
            DataFrame indexed by ``time_pt``.  Returns an empty DataFrame when
            the file contains no rows or lacks the required time columns.
        """
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
