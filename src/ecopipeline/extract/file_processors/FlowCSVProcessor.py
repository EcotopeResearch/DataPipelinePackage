import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class FlowCSVProcessor(FileProcessor):
    """FileProcessor for flow meter CSV files.

    Flow meter files have 6 header rows to skip.  The timestamp is
    reconstructed from individual ``Year``, ``Month``, ``Day``, ``Hour``,
    ``Minute``, and ``Second`` columns into a ``time_pt`` datetime index.
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
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3):
        super().__init__(config, ".csv", start_time, end_time,
                         filename_date_format=filename_date_format, file_prefix=file_prefix,
                         data_sub_dir=data_sub_dir, date_string_start_idx=date_string_start_idx,
                         date_string_end_idx=date_string_end_idx, round_time_index=True)

    def _read_file_into_df(self, file_name: str) -> pd.DataFrame:
        """Read a single flow meter CSV file and set a ``time_pt`` datetime index.

        Skips the first 6 header rows, then assembles the ``time_pt`` index
        from the ``Year``, ``Month``, ``Day``, ``Hour``, ``Minute``, and
        ``Second`` columns.

        Parameters
        ----------
        file_name : str
            Absolute path to the flow meter ``.csv`` file to read.

        Returns
        -------
        pd.DataFrame
            DataFrame indexed by ``time_pt``.  Returns an empty DataFrame when
            the file contains no rows or the required time columns are absent.
        """
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
