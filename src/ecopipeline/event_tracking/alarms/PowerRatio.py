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
    Detects when power variables fall outside their expected ratio of total power over a rolling period.
    Variables are grouped by alarm ID, and each variable's ratio is checked against its expected low-high
    range as a percentage of the group total.

    VarNames syntax:
    POWRRAT_[ID]:###-### - Power variable to track. [ID] groups variables together for ratio calculation.
        ###-### is the expected low-high percentage range (e.g., PR_HPWH:60-80 means this variable
        should account for 60-80% of the HPWH group total).

    Parameters
    ----------
    day_table_name : str
        Name of the site's daily aggregate value table in the database for fetching historical data.
    ratio_period_days : int
        Number of days to use for the rolling power ratio calculation (default 7). Each block sums
        the values over this many days before calculating ratios.
    """
    def __init__(self, bounds_df : pd.DataFrame, day_table_name : str, ratio_period_days : int = 7):
        alarm_tag = 'POWRRAT'
        type_default_dict = {}
        self.ratio_period_days = ratio_period_days
        self.day_table_name = day_table_name # TODO this could be a security issue. Swap it for config manager
        super().__init__(bounds_df, alarm_tag,type_default_dict, two_part_tag = False, range_bounds=True, alarm_db_type='POWER_RATIO', daily_only=True)

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

                alarm_blocks_df = blocks_df.loc[(blocks_df[f"{variable}_{alarm_id}"] < low_bound) | (blocks_df[f"{variable}_{alarm_id}"] > high_bound)]
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
    
    def _organize_alarm_codes(self, bounds_df : pd.DataFrame) -> pd.DataFrame:
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

        if alarm_code_parts:
            bounds_df[['alarm_code_type', 'alarm_code_id']] = pd.DataFrame(alarm_code_parts, index=bounds_df.index)

            # Replace None bounds with appropriate defaults based on alarm_code_type
            for idx, row in bounds_df.iterrows():
                if pd.isna(row['bound']) or row['bound'] is None:
                    if row['alarm_code_type'] in self.type_default_dict.keys():
                        bounds_df.at[idx, 'bound'] = self.type_default_dict[row['alarm_code_type']][0]
                        bounds_df.at[idx, 'bound2'] = self.type_default_dict[row['alarm_code_type']][1]
            # Coerce bound column to float
            bounds_df['bound'] = pd.to_numeric(bounds_df['bound'], errors='coerce').astype(float)
            if self.range_bounds:
                bounds_df['bound2'] = pd.to_numeric(bounds_df['bound2'], errors='coerce').astype(float)

        return bounds_df