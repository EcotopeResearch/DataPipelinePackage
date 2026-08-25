import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class LongCSVProcessor(FileProcessor):
    """FileProcessor for long-form CSV files with field/value column pairs.

    Reads ``.csv`` files that store one observation per row rather than one
    timestamp per row.  Each file must contain at least three columns: a field
    name column, a timestamp column, and a value column.  The file is pivoted
    into wide form so that every distinct value of ``field_column_name``
    becomes its own column, indexed by ``time_pt`` in the same way as
    :class:`~ecopipeline.extract.file_processors.CSVProcessor.CSVProcessor`.

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
    field_column_name : str, optional
        Name of the column holding the sensor/field name for each row.  Each
        distinct value becomes a column in the returned wide DataFrame.
        Defaults to ``'field'``.
    value_column_name : str, optional
        Name of the column holding the measurement for each row.  Values are
        coerced to numeric; non-numeric entries become ``NaN``.
        Defaults to ``'value'``.
    strip_time_zone_suffix : bool, optional
        When ``True``, remove the final whitespace-delimited token from each
        timestamp string before parsing it with ``time_column_format``.  Use
        this for files that append a time-zone abbreviation to the timestamp
        (e.g. ``'22-Aug-26 12:00 AM PDT'`` becomes ``'22-Aug-26 12:00 AM'``,
        parsed with ``'%d-%b-%y %I:%M %p'``).  ``strptime``'s ``%Z`` only
        matches ``UTC``, ``GMT``, and the host machine's own local zone names,
        so abbreviations such as ``PDT`` cannot be parsed directly.  The
        abbreviation is discarded rather than applied, so timestamps are read
        as local wall-clock time and daylight-saving transitions remain in the
        index.  Defaults to ``False``.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None, raw_time_column: str = 'DateTime',
                 time_column_format: str = '%Y/%m/%d %H:%M:%S', filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3, field_column_name: str = 'field', value_column_name: str = 'value',
                 strip_time_zone_suffix: bool = False):
        self.field_column_name = field_column_name
        self.value_column_name = value_column_name
        self.strip_time_zone_suffix = strip_time_zone_suffix
        super().__init__(config, ".csv", start_time, end_time, raw_time_column, time_column_format, filename_date_format,
                         file_prefix, data_sub_dir, date_string_start_idx, date_string_end_idx)

    def _read_file_into_df(self, file_name: str) -> pd.DataFrame:
        """Read a single long-form CSV file and pivot it into wide form.

        Delegates raw CSV reading to the parent implementation, optionally
        strips the trailing time-zone token from ``raw_time_column``, parses
        it with ``time_column_format`` into ``time_pt``,
        coerces ``value_column_name`` to numeric, then pivots so that each
        distinct ``field_column_name`` value becomes a column.  Duplicate
        (timestamp, field) pairs are averaged.  Columns other than the
        timestamp, field, and value columns are dropped.

        Parameters
        ----------
        file_name : str
            Absolute path to the ``.csv`` file to read.

        Returns
        -------
        pd.DataFrame
            Wide-form DataFrame indexed by ``time_pt`` with one column per
            field name.  Returns an empty DataFrame when the file contains no
            rows.

        Raises
        ------
        KeyError
            If the file is missing the timestamp, field, or value column.
        """
        data = super()._read_file_into_df(file_name)
        if len(data) != 0:
            missing_columns = [col for col in [self.raw_time_column, self.field_column_name, self.value_column_name] if col not in data.columns]
            if len(missing_columns) > 0:
                raise KeyError(f"Long form csv {file_name} is missing required column(s): {', '.join(missing_columns)}")

            time_strings = data[self.raw_time_column]
            if self.strip_time_zone_suffix:
                time_strings = time_strings.str.rsplit(' ', n=1).str[0]
            data['time_pt'] = pd.to_datetime(time_strings, format=self.time_column_format)
            data[self.value_column_name] = pd.to_numeric(data[self.value_column_name], errors='coerce')
            data = data.pivot_table(index='time_pt', columns=self.field_column_name, values=self.value_column_name, aggfunc='mean')
            data.columns.name = None
            data.sort_index(inplace=True)

        return data
