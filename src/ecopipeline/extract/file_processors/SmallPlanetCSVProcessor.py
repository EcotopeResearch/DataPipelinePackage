import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class SmallPlanetCSVProcessor(FileProcessor):
    """FileProcessor for Small Planet Controls CSV files.

    Similar to :class:`MSACSVProcessor` but applies variable-name mapping
    from ``Variable_Names.csv`` at read time: column names are first prefixed
    with the filename stem, then renamed through the alias-to-true-name
    mapping, and finally any columns that still carry an alias name or are
    absent from the true-name list are dropped.  Only-NaN rows are also
    removed.  The index is floored to the minute and duplicate timestamps are
    averaged.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the
        pipeline.  Must provide a path to ``Variable_Names.csv`` via
        :meth:`~ecopipeline.ConfigManager.get_var_names_path`.
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
        Sub-directory under the configured data directory containing the
        files.  Defaults to an empty string.
    date_string_start_idx : int, optional
        Start index (from the end) of the date substring in the filename.
        Defaults to ``-17``.
    date_string_end_idx : int, optional
        End index (from the end) of the date substring in the filename.
        Defaults to ``-3``.
    site : str, optional
        If non-empty, only variable-name rows whose ``site`` column matches
        this value are used for column mapping.  Defaults to an empty string.
    system : str, optional
        If non-empty, only variable-name rows whose ``system`` column contains
        this substring are used for column mapping.  Defaults to an empty
        string.
    time_zone : str, optional
        Timezone name used to convert UTC epoch timestamps to local time.
        Defaults to ``'US/Pacific'``.

    Raises
    ------
    Exception
        If the ``Variable_Names.csv`` file cannot be found at the path
        returned by ``config.get_var_names_path()``.
    """

    def __init__(self, config: ConfigManager, start_time: datetime = None, end_time: datetime = None,
                 filename_date_format: str = "%Y%m%d%H%M%S", file_prefix: str = "", data_sub_dir: str = "",
                 date_string_start_idx: int = -17, date_string_end_idx: int = -3,
                 site: str = "", system: str = "", time_zone: str = 'US/Pacific'):
        self.site = site
        self.system = system
        self.time_zone = time_zone

        variable_names_path = config.get_var_names_path()
        try:
            variable_data = pd.read_csv(variable_names_path)
        except FileNotFoundError:
            raise Exception("Variable names file Not Found: " + variable_names_path)

        if site != "":
            variable_data = variable_data.loc[variable_data['site'] == site]
        if system != "":
            variable_data = variable_data.loc[variable_data['system'].str.contains(system, na=False)]

        variable_data = variable_data.loc[:, ['variable_alias', 'variable_name']]
        variable_data.dropna(axis=0, inplace=True)
        self.variable_alias = list(variable_data['variable_alias'])
        self.variable_true = list(variable_data['variable_name'])
        self.variable_alias_true_dict = dict(zip(self.variable_alias, self.variable_true))

        super().__init__(config, ".csv", start_time, end_time,
                         filename_date_format=filename_date_format, file_prefix=file_prefix,
                         data_sub_dir=data_sub_dir, date_string_start_idx=date_string_start_idx,
                         date_string_end_idx=date_string_end_idx, round_time_index=True)

    def _read_file_into_df(self, file_name: str) -> pd.DataFrame:
        """Read a single Small Planet CSV file, remap columns, and set a datetime index.

        Converts the ``'DateEpoch(secs)'`` column from UTC to ``time_zone``,
        prefixes all columns with the filename stem, applies the
        alias-to-true-name mapping loaded during initialisation, drops columns
        that still carry an alias name, drops columns not in the true-name
        list, and removes all-NaN rows.

        Parameters
        ----------
        file_name : str
            Absolute path to the Small Planet Controls ``.csv`` file to read.

        Returns
        -------
        pd.DataFrame
            DataFrame indexed by ``time_pt`` containing only the mapped
            true-name columns.  Returns an empty DataFrame when the file
            contains no rows.
        """
        data = pd.read_csv(file_name)
        if len(data) != 0:
            prefix = file_name.split('.')[0].split('/')[-1]
            data['time_pt'] = pd.to_datetime(data['DateEpoch(secs)'], unit='s', utc=True)
            data['time_pt'] = data['time_pt'].dt.tz_convert(self.time_zone).dt.tz_localize(None)
            data.set_index('time_pt', inplace=True)
            data.drop(columns='DateEpoch(secs)', inplace=True)
            data = data.rename(columns={col: f"{prefix}{col}".replace(' ', '_').replace('*', '_') for col in data.columns})
            data.rename(columns=self.variable_alias_true_dict, inplace=True)
            data.drop(columns=[col for col in data if col in self.variable_alias], inplace=True)
            data.drop(columns=[col for col in data if col not in self.variable_true], inplace=True)
            data = data.dropna(how='all')
        return data
