import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import datetime, timedelta
import os

class FileProcessor:
    """Base class for reading raw data files into a pandas DataFrame.

    On instantiation the processor collects matching files from the configured
    data directory, optionally filters them by date range, and concatenates them
    into a single DataFrame accessible via :meth:`get_raw_data`.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    extension : str
        File extension of raw data files (e.g. ``".csv"``, ``".gz"``).
    start_time : datetime, optional
        Earliest timestamp (inclusive) used to filter files by the date encoded
        in their filename.  Uses local time matching the data index.
    end_time : datetime, optional
        Latest timestamp (exclusive) used to filter files by the date encoded in
        their filename.  Uses local time matching the data index.
    raw_time_column : str, optional
        Name of the column in the raw file that contains timestamp strings.
        Defaults to ``'DateTime'``.
    time_column_format : str, optional
        :func:`datetime.strptime` format string used to parse ``raw_time_column``.
        Defaults to ``'%Y/%m/%d %H:%M:%S'``.
    filename_date_format : str, optional
        :func:`datetime.strftime` format string used to convert ``start_time``
        and ``end_time`` to integers for filename comparison.
        Defaults to ``'%Y%m%d%H%M%S'``.
    file_prefix : str, optional
        Only files whose names begin with this prefix are processed.
        Defaults to an empty string (all files).
    data_sub_dir : str, optional
        Sub-directory appended to the configured data directory when locating
        files.  For example, if files live in ``'path/to/data/DENT/'`` and the
        configured data directory is ``'path/to/data/'``, pass ``'DENT/'``.
        Defaults to an empty string.
    date_string_start_idx : int, optional
        Start index (from the end) of the date substring within each filename.
        Defaults to ``-17``.
    date_string_end_idx : int, optional
        End index (from the end) of the date substring within each filename.
        Defaults to ``-3``.
    round_time_index : bool, optional
        If ``True``, floor the datetime index to the minute and average any
        duplicate timestamps after concatenation.  Defaults to ``False``.
    """

    def __init__(self, config : ConfigManager, extension: str, start_time: datetime = None, end_time: datetime = None, raw_time_column : str = 'DateTime',
                 time_column_format : str ='%Y/%m/%d %H:%M:%S', filename_date_format : str = "%Y%m%d%H%M%S", file_prefix : str = "", data_sub_dir : str = "",
                 date_string_start_idx : int = -17, date_string_end_idx : int = -3, round_time_index : bool = False):
        self.extension = extension
        self.start_time = start_time
        self.end_time = end_time
        self.raw_time_column = raw_time_column
        self.time_column_format = time_column_format
        self.filename_date_format = filename_date_format
        self.date_string_start_idx = date_string_start_idx
        self.date_string_end_idx = date_string_end_idx
        self.file_prefix = file_prefix
        self.data_sub_dir = data_sub_dir
        self.round_time_index = round_time_index
        self.raw_df = pd.DataFrame()
        try:
            filenames = self.extract_files(config)
            self.raw_df = self.raw_files_to_df(filenames)
        except Exception as e:
            print(f"File extraction failed: {e}")
            raise e
        
    def get_raw_data(self) -> pd.DataFrame:
        """Return the concatenated raw DataFrame produced during initialisation.

        Returns
        -------
        pd.DataFrame
            DataFrame containing all raw data read from the matching files.
            Returns an empty DataFrame when no files were found or all reads
            failed.
        """
        return self.raw_df

    def extract_files(self, config: ConfigManager) -> list[str]:
        """Collect the full paths of files that match the processor's criteria.

        Scans the configured data directory (plus any ``data_sub_dir``) for
        files whose names end with the configured extension and begin with the
        configured prefix.  When ``start_time`` is set the list is further
        filtered by :meth:`extract_new`.

        Parameters
        ----------
        config : ConfigManager
            The ConfigManager object that provides the base data directory path.

        Returns
        -------
        list of str
            Absolute file paths for all files that satisfy the criteria.
        """
        os.chdir(os.getcwd())
        filenames = []
        full_data_path = f"{config.data_directory}{self.data_sub_dir}"
        for file in os.listdir(full_data_path):
            if file.endswith(self.extension) and file.startswith(self.file_prefix):
                full_filename = os.path.join(full_data_path, file)
                filenames.append(full_filename)
        
        if not self.start_time is None:
            filenames = self.extract_new(filenames)

        return filenames
    
    def extract_new(self, filenames: list[str]) -> list[str]:
        """Filter a list of filenames to those whose encoded date falls in range.

        The date is extracted from each filename using ``date_string_start_idx``
        and ``date_string_end_idx``, then compared as an integer against the
        integer representations of ``start_time`` and ``end_time`` (formatted
        with ``filename_date_format``).

        Parameters
        ----------
        filenames : list of str
            Candidate file paths to filter.

        Returns
        -------
        list of str
            Only the file paths whose encoded date satisfies
            ``start_time <= date < end_time`` (``end_time`` bound is omitted
            when ``end_time`` is ``None``).
        """
        endTime_int = self.end_time
        startTime_int = int(self.start_time.strftime(self.filename_date_format))
        if not self.end_time is None:
            endTime_int = int(self.end_time.strftime(self.filename_date_format)
                            )
        return_list = list(filter(lambda filename: int(filename[self.date_string_start_idx:self.date_string_end_idx]) >= startTime_int and (endTime_int is None or int(filename[self.date_string_start_idx:self.date_string_end_idx]) < endTime_int), filenames))
        return return_list
    
    def _read_file_into_df(self, file_name : str) -> pd.DataFrame:
        """Read a single file into a DataFrame.

        Subclasses override this method to apply format-specific parsing
        (header skipping, timestamp construction, column renaming, etc.).

        Parameters
        ----------
        file_name : str
            Absolute path to the file to read.

        Returns
        -------
        pd.DataFrame
            Raw contents of the file as a DataFrame.  Returns an empty
            DataFrame when the file contains no data.
        """
        data = pd.read_csv(file_name)
        return data

    def raw_files_to_df(self, filenames : list[str]) -> pd.DataFrame:
        """Concatenate multiple raw files into a single DataFrame.

        Calls :meth:`_read_file_into_df` for each path in ``filenames``,
        ignores files that are not found or raise read errors, and
        concatenates the results.  When ``round_time_index`` is enabled the
        index is floored to the minute and duplicate timestamps are averaged.

        Parameters
        ----------
        filenames : list of str
            Absolute file paths to read and concatenate.

        Returns
        -------
        pd.DataFrame
            Concatenated DataFrame for all successfully read files.  Returns
            an empty DataFrame when no files could be read.
        """
        temp_dfs = []
        for file in filenames:
            try:
                data = self._read_file_into_df(file)
                if len(data) != 0:
                    temp_dfs.append(data)
            except FileNotFoundError:
                print("File Not Found: ", file)
                continue
            except Exception as e:
                print(f"Error reading {file}: {e}")
                continue
                    
        if len(temp_dfs) <= 0:
            print("No data for timefarme.")
            return pd.DataFrame()
        
        df = pd.concat(temp_dfs, ignore_index=False)

        if self.round_time_index:
            df.index = df.index.floor('min')
            df = df.groupby(df.index).mean(numeric_only=True)
            df.sort_index(inplace=True)

        return df