import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class PowerRatio(Alarm):
    """
    Detects when power variables fall outside their expected ratio of a group total over a rolling period.
    Variables are grouped by element ID derived from their variable_name, and each variable's contribution
    is checked against its expected low-high percentage range.

    Variable_Names.csv configuration:
      alarm_codes column: POWRRAT:low-high where low-high is the acceptable percentage range
        (e.g., POWRRAT:60-80 means the variable should account for 60-80% of its group total).
        Bounds (low-high) come from alarm_codes and are not part of the variable_name.
      variable_name column: Must start with PowerIn_. Determines the grouping of variables:
        PowerIn_Total - Total system power. Used as the denominator for the 'Total' group.
        PowerIn_[name containing '_HPWH'] - Grouped under the 'HPWH' element ID. The sum of all HPWH
            variables is the group denominator; each variable's ratio is checked against its range.
        PowerIn_[other] - All other PowerIn variables are grouped under the 'Total' element ID and their
            ratios are calculated against the PowerIn_Total variable.

    Parameters
    ----------
    day_table_name : str
        Name of the site's daily aggregate value table in the database for fetching historical data.
    ratio_period_days : int
        Number of days to use for the rolling power ratio calculation (default 7). Each block sums
        values over this many days before calculating ratios.
    """
    def __init__(self, bounds_df : pd.DataFrame, day_table_name : str, ratio_period_days : int = 7):
        alarm_tag = 'POWRRAT'
        type_default_dict = {}
        self.ratio_period_days = ratio_period_days
        self.day_table_name = day_table_name # TODO this could be a security issue. Swap it for config manager
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = False, range_bounds=True, daily_only=True)

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        daily_df_copy = daily_df.copy()
        if self.ratio_period_days > 1:
            daily_df_copy = self._append_previous_days_to_df(daily_df_copy, config, self.ratio_period_days, self.day_table_name)
        elif self.ratio_period_days < 1:
            print("power ratio alarm period, ratio_period_days, must be more than 1")
            return

        # Create blocks of ratio_period_days
        blocks_df = self._create_period_blocks(daily_df_copy)

        if blocks_df.empty:
            print("No complete blocks available for analysis")
            return

        for alarm_id in self.bounds_df['alarm_code_id'].unique():
            # Calculate total for each block
            var_list = self.bounds_df[self.bounds_df['alarm_code_id'] == alarm_id]['variable_name'].unique()
            for var in var_list:
                if var not in blocks_df.columns:
                    blocks_df[var] = 0.0
            if alarm_id == 'Total':
                tp_codes = self.bounds_df[self.bounds_df['alarm_code_type'] == 'PowerIn_Total']
                if len(tp_codes) != 1:
                    raise Exception(f"POWRRAT Error: There must be exactly one Total Power variable. Found {len(tp_codes)}")
                if tp_codes.iloc[0]['variable_name'] in blocks_df.columns:
                    blocks_df[alarm_id] = blocks_df[tp_codes.iloc[0]['variable_name']]
                    var_list = var_list[var_list != tp_codes.iloc[0]['variable_name']]
                else:
                   raise Exception(f"POWRRAT Error: PowerIn_Total variable missing from total power ratio") 
            else:
                blocks_df[alarm_id] = blocks_df[var_list].sum(axis=1)

            for variable in var_list:
                # Calculate ratio for each block
                blocks_df[f"{variable}_{alarm_id}"] = (blocks_df[variable]/blocks_df[alarm_id]) * 100

                # Get bounds from bounds_df for this variable and alarm_id
                var_row = self.bounds_df[(self.bounds_df['variable_name'] == variable) & (self.bounds_df['alarm_code_id'] == alarm_id)]
                if var_row.empty:
                    continue
                low_bound = var_row.iloc[0]['bound']
                high_bound = var_row.iloc[0]['bound2']
                pretty_name = var_row.iloc[0]['pretty_name']
                if alarm_id == 'Total':
                    # report all TM heating that is out of bounds
                    alarm_blocks_df = blocks_df.loc[(blocks_df[f"{variable}_{alarm_id}"] < low_bound) | (blocks_df[f"{variable}_{alarm_id}"] > high_bound)]
                else:
                    # report only HPWHs that are underreporting
                    alarm_blocks_df = blocks_df.loc[(blocks_df[f"{variable}_{alarm_id}"] < low_bound)]
                if not alarm_blocks_df.empty:
                    for block_end_date, values in alarm_blocks_df.iterrows():
                        block_start_date = block_end_date - timedelta(days=self.ratio_period_days - 1)
                        actual_ratio = values[f'{variable}_{alarm_id}']
                        self._add_an_alarm(block_start_date, block_end_date + timedelta(1), variable,
                            f"Power ratio alarm ({self.ratio_period_days}-day block ending {block_end_date.strftime('%Y-%m-%d')}): {pretty_name} accounted for {actual_ratio:.1f}% of {alarm_id} energy use. {low_bound:.1f}-{high_bound:.1f}% expected.", add_one_minute_to_end=False) 
    
    def _create_period_blocks(self, daily_df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
        """
        Create blocks of ratio_period_days by summing values within each block.
        Each block will be represented by its end date.
        """
        if len(daily_df) < self.ratio_period_days:
            if verbose:
                print(f"Not enough data for {self.ratio_period_days}-day blocks. Need at least {self.ratio_period_days} days but only have {len(daily_df)} days in data")
            return pd.DataFrame()
        
        blocks = []
        block_dates = []
        
        # Create blocks by summing consecutive groups of ratio_period_days
        for i in range(self.ratio_period_days - 1, len(daily_df)):
            start_idx = i - self.ratio_period_days + 1
            end_idx = i + 1
            
            block_data = daily_df.iloc[start_idx:end_idx].sum()
            blocks.append(block_data)
            # Use the end date of the block as the identifier
            block_dates.append(daily_df.index[i])
        
        if not blocks:
            return pd.DataFrame()
        
        blocks_df = pd.DataFrame(blocks, index=block_dates)
        
        if verbose:
            print(f"Created {len(blocks_df)} blocks of {self.ratio_period_days} days each")
            print(f"Block date range: {blocks_df.index.min()} to {blocks_df.index.max()}")
        
        return blocks_df
    
    def _organize_alarm_codes(self, bounds_df : pd.DataFrame) -> list:
        alarm_code_parts = []
        seen_total_power = False
        for idx, row in bounds_df.iterrows():
            element_id = 'Total'
            parts = row['variable_name'].split('_')
            if len(parts) <= 1:
                raise Exception(f"Improper variable name for '{row['variable_name']}', must be in form '[Unit Type]_[Element Identifier]' (e.g. 'Temp_HPWH' or 'PowerIn_SwingTank1').")
            elif parts[0] != "PowerIn":
                raise Exception(f"Error in POWRRAT: {row['variable_name']} is not designated as a power variable (must begin with 'PowerIn_').")
            if parts[0] == "PowerIn" and parts[1] == "Total":
                # total power is own catagory
                if seen_total_power:
                    raise Exception(f"Multiple instances of PowerIn_Total seen for alarm code {self.alarm_tag}. There may only be one variable that starts with 'PowerIn_Total'. This should be total system power.")
                alarm_code_parts.append(["PowerIn_Total", element_id])
                seen_total_power = True
            else:
                if "_HPWH" in row['variable_name']:
                    element_id = "HPWH"
                    
                alarm_code_parts.append([parts[0], element_id])

        return alarm_code_parts