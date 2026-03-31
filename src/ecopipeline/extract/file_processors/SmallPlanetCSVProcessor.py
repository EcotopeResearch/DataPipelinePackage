import pandas as pd
from ecopipeline import ConfigManager
from datetime import datetime
from ecopipeline.extract.FileProcessor import FileProcessor


class SmallPlanetCSVProcessor(FileProcessor):
    """FileProcessor for Small Planet Controls CSV files.

    Like MSACSVProcessor but variable names are mapped through Variable_Names.csv,
    columns without a matching variable_name are dropped, and columns that keep an
    alias name (unmapped) are also dropped.
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
