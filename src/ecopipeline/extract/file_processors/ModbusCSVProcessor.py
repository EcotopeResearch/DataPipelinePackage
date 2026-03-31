import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime, timedelta
import re
from ecopipeline.extract.FileProcessor import FileProcessor


class ModbusCSVProcessor(FileProcessor):
    """FileProcessor for Modbus CSV files (e.g. Acquisuite).

    Reads standard CSV files whose first column is a ``time(UTC)`` timestamp
    string.  All data columns are prefixed with the filename stem so that data
    from multiple Modbus devices can be merged without column name collisions.
    The index is floored to the minute and duplicate timestamps are averaged
    after concatenation.

    Acquisuite filenames encode the file start time as a hexadecimal Unix
    timestamp in the filename, so :meth:`extract_new` overrides the default
    date-substring comparison with a hex-to-datetime conversion.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    start_time : datetime, optional
        Earliest local timestamp to include (compared against the hex date
        embedded in the filename).
    end_time : datetime, optional
        Latest local timestamp to include, exclusive (compared against the hex
        date embedded in the filename).
    raw_time_column : str, optional
        Name of the timestamp column in each CSV file.
        Defaults to ``'time(UTC)'``.
    filename_date_format : str, optional
        :func:`datetime.strftime` format used when comparing non-hex filenames.
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
                 raw_time_column: str = 'time(UTC)', filename_date_format: str = "%Y%m%d%H%M%S",
                 file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3):
        super().__init__(config, ".csv", start_time, end_time,
                         raw_time_column=raw_time_column,
                         filename_date_format=filename_date_format, file_prefix=file_prefix,
                         data_sub_dir=data_sub_dir, date_string_start_idx=date_string_start_idx,
                         date_string_end_idx=date_string_end_idx, round_time_index=True)

    def extract_new(self, filenames: list[str]) -> list[str]:
        """Filter filenames using the hexadecimal Unix timestamp in the filename.

        Acquisuite filenames embed the file start time as a hexadecimal number
        between the first ``.`` and the first ``_`` in the basename.  This
        method decodes those hex timestamps to UTC datetimes, strips timezone
        awareness, and keeps only the files whose decoded time falls within
        ``[start_time, end_time)``.

        Parameters
        ----------
        filenames : list of str
            Candidate file paths to filter.

        Returns
        -------
        list of str
            Only the file paths whose decoded hex timestamp satisfies
            ``start_time < local_time`` and, when ``end_time`` is set,
            ``local_time < end_time``.
        """
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
        """Read a single Modbus CSV file and set a datetime index.

        Parses the ``raw_time_column`` timestamp, sets it as the index, then
        renames all remaining columns with the filename stem as a prefix.

        Parameters
        ----------
        file_name : str
            Absolute path to the Modbus ``.csv`` file to read.

        Returns
        -------
        pd.DataFrame
            DataFrame indexed by the parsed timestamp with prefixed column
            names.  Returns an empty DataFrame when the file contains no rows
            or the expected timestamp column is absent.
        """
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
