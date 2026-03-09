import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from ecopipeline.event_tracking.Alarm import Alarm

class SOOChange(Alarm):
    """
    Detects unexpected state of operation (SOO) changes by checking if the heat pump turns on or off
    when the corresponding temperature is not near the expected aquastat setpoint threshold. An alarm
    triggers if the HP turns on/off and the temperature differs by more than 5.0 degrees from expected.

    Variable_Names.csv configuration:
      The role of each variable is determined by the first underscore-separated part of its variable_name,
      while the alarm_codes column carries the tag, sub-type, and bound:
        variable_name: PowerIn_[name], alarm_codes: SOOCHNG:### — HP system power variable (total power
            across all primary HPs). Bound (###) from alarm_codes is the power threshold (default 1.0).
        variable_name: Temp_[name], alarm_codes: SOOCHNG_ON_[Mode]:### — Temperature variable checked at
            HP turn-on events. Bound (###) from alarm_codes is the expected ON temperature (default 115.0).
        variable_name: Temp_[name], alarm_codes: SOOCHNG_OFF[Mode]:### — Temperature variable checked at
            HP turn-off events. Bound (###) from alarm_codes is the expected OFF temperature (default 140.0).
      [Mode] in alarm_codes is optional and must be one of: loadUp, shed, criticalPeak, gridEmergency, advLoadUp.
        If omitted, the variable applies to normal (non-load-shifting) operation.
      Each mode requires exactly one ON and one OFF temperature variable.

    Parameters
    ----------
    default_power_threshold : float
        Default power threshold for PowerIn variables when no bound is specified (default 1.0).
    default_on_temp : float
        Default ON temperature threshold (default 115.0). Alarm triggers if temperature differs by
        more than 5.0 degrees from this value when the HP turns on.
    default_off_temp : float
        Default OFF temperature threshold (default 140.0). Alarm triggers if temperature differs by
        more than 5.0 degrees from this value when the HP turns off.
    """
    def __init__(self, bounds_df : pd.DataFrame, default_power_threshold : float = 1.0, default_on_temp : float = 115.0, default_off_temp : float = 140.0):
        alarm_tag = 'SOOCHNG'
        self.default_power_threshold = default_power_threshold
        self.default_on_temp = default_on_temp
        self.default_off_temp = default_off_temp
        self.soo_dict = {
                'loadUp' : 'LOAD UP',
                'shed' : 'SHED',
                'criticalPeak': 'CRITICAL PEAK',
                'gridEmergency' : 'GRID EMERGENCY',
                'advLoadUp' : 'ADVANCED LOAD UP'
            }
        type_default_dict = {
                'PowerIn' : default_power_threshold,
                'ON' : default_on_temp,
                'OFF' : default_off_temp
            }
        super().__init__(bounds_df, alarm_tag, type_default_dict, alarm_db_type='SOO_CHANGE')

    def specific_alarm_function(self, df: pd.DataFrame, daily_df : pd.DataFrame, config : ConfigManager):
        ls_df = config.get_ls_df()
        pow_codes = self.bounds_df[self.bounds_df['alarm_code_type'] == 'PowerIn']
        if len(pow_codes) != 1:
            raise Exception(f"Improper alarm codes for SOO changes; must have 1 power variable to indicate power to HPWH(s).")
        pow_var_name = pow_codes.iloc[0]['variable_name']
        pow_thresh = pow_codes.iloc[0]['bound']
        non_pow_bounds_df = self.bounds_df[self.bounds_df['alarm_code_type'] != 'PowerIn']

        for alarm_id in non_pow_bounds_df['alarm_code_id'].unique():
            ls_filtered_df = df.copy()
            soo_mode_name = 'NORMAL'
            if alarm_id in self.soo_dict.keys():
                if not ls_df.empty:
                    # Filter ls_filtered_df for only date ranges in the right mode of ls_df
                    mode_rows = ls_df[ls_df['event'] == alarm_id]
                    mask = pd.Series(False, index=ls_filtered_df.index)
                    for _, row in mode_rows.iterrows():
                        mask |= (ls_filtered_df.index >= row['startDateTime']) & (ls_filtered_df.index < row['endDateTime'])
                    ls_filtered_df = ls_filtered_df[mask]
                    soo_mode_name = self.soo_dict[alarm_id]
                else:
                    print(f"Cannot check for {alarm_id} because there are no {alarm_id} periods in time frame.")
                    continue
            elif not ls_df.empty:
                # Filter out all date range rows from ls_filtered_df's indexes
                mask = pd.Series(True, index=ls_filtered_df.index)
                for _, row in ls_df.iterrows():
                    mask &= ~((ls_filtered_df.index >= row['startDateTime']) & (ls_filtered_df.index < row['endDateTime']))
                ls_filtered_df = ls_filtered_df[mask]

            for day in daily_df.index:
                next_day = day + pd.Timedelta(days=1)
                filtered_df = ls_filtered_df.loc[(ls_filtered_df.index >= day) & (ls_filtered_df.index < next_day)]
                id_group = non_pow_bounds_df[non_pow_bounds_df['alarm_code_id'] == alarm_id]
                on_t_codes = id_group[id_group['alarm_code_type'] == 'ON']
                off_t_codes = id_group[id_group['alarm_code_type'] == 'OFF']
                if len(on_t_codes) != 1 or len(off_t_codes) != 1:
                    raise Exception(f"Improper alarm codes for SOO changes with id {alarm_id}. Must have 1 ON and 1 OFF variable")
                on_t_var_name = on_t_codes.iloc[0]['variable_name']
                on_t_pretty_name = on_t_codes.iloc[0]['pretty_name']
                on_t_thresh = on_t_codes.iloc[0]['bound']
                off_t_var_name = off_t_codes.iloc[0]['variable_name']
                off_t_pretty_name = off_t_codes.iloc[0]['pretty_name']
                off_t_thresh = off_t_codes.iloc[0]['bound']
                if pow_var_name in filtered_df.columns:
                    power_below = filtered_df[pow_var_name] <= pow_thresh
                    power_above = filtered_df[pow_var_name] > pow_thresh

                    # Check all turn-on events
                    if on_t_var_name in filtered_df.columns:
                        power_turn_on = power_below.shift(1) & power_above
                        power_on_times = filtered_df.index[power_turn_on.fillna(False)]
                        # Check if temperature is within 5.0 of on_t_thresh at each turn-on moment
                        for power_time in power_on_times:
                            temp_at_turn_on = filtered_df.loc[power_time, on_t_var_name]
                            if abs(temp_at_turn_on - on_t_thresh) > 5.0:
                                self._add_an_alarm(power_time, power_time, on_t_var_name,
                                    f"Unexpected SOO change: during {soo_mode_name}, HP turned on at {power_time} but {on_t_pretty_name} was {temp_at_turn_on:.1f} F (setpoint at {on_t_thresh} F).",
                                    certainty="med")

                    # Check all turn-off events
                    if off_t_var_name in filtered_df.columns:
                        power_turn_off = power_above.shift(1) & power_below
                        power_off_times = filtered_df.index[power_turn_off.fillna(False)]
                        # Check if temperature is within 5.0 of off_t_thresh at each turn-off moment
                        for power_time in power_off_times:
                            temp_at_turn_off = filtered_df.loc[power_time, off_t_var_name]
                            if abs(temp_at_turn_off - off_t_thresh) > 5.0:
                                self._add_an_alarm(power_time, power_time, off_t_var_name,
                                    f"Unexpected SOO change: during {soo_mode_name}, HP turned off at {power_time} but {off_t_pretty_name} was {temp_at_turn_off:.1f} F (setpoint at {off_t_thresh} F).",
                                    certainty="med")
                                
    def _organize_alarm_codes(self, bounds_df : pd.DataFrame) -> list:
        alarm_code_parts = []
        for idx, row in bounds_df.iterrows():
            var_name_parts = row['variable_name'].split('_')
            if len(var_name_parts) <= 1:
                raise Exception(f"Improper variable name for '{row['variable_name']}', mustt be in form '[Unit Type]_[Element Identifier]' (e.g. 'Temp_HPWH' or 'PowerIn_SwingTank1').")
            if var_name_parts[0] == "PowerIn":
                alarm_code_parts.append(["PowerIn", "No ID"])
            elif var_name_parts[0] == "Temp":
                parts = row['alarm_codes'].split('_')
                if len(parts) == 2:
                    alarm_code_parts.append([parts[1], "No ID"])
                elif len(parts) == 3:
                    alarm_code_parts.append([parts[1], parts[2]])
                else:
                    raise Exception(f"improper {self.alarm_tag} alarm code format for {row['variable_name']}")
            else:
                raise Exception(f"{var_name_parts[0]} is not a proper unit type for SOOCHNG alarms.")

        return alarm_code_parts