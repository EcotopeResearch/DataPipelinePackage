import pandas as pd
import numpy as np
import datetime as datetime
from ecopipeline import ConfigManager
import re
import mysql.connector.errors as mysqlerrors
from datetime import timedelta
from .alarms.ShortCycle import ShortCycle
from .alarms.TempRange import TempRange
from .alarms.LSInconsist import LSInconsist
from .alarms.SOOChange import SOOChange
from .alarms.BlownFuse import BlownFuse
from .alarms.HPWHOutage import HPWHOutage
from .alarms.BackupUse import BackupUse
from .alarms.HPWHOutlet import HPWHOutlet
from .alarms.HPWHInlet import HPWHInlet
from .alarms.BalancingValve import BalancingValve
from .alarms.TMSetpoint import TMSetpoint
from .alarms.AbnormalCOP import AbnormalCOP
from .alarms.PowerRatio import PowerRatio
from .alarms.Boundary import Boundary

def central_alarm_df_creator(df: pd.DataFrame, daily_data : pd.DataFrame, config : ConfigManager, system: str = "", 
                             default_cop_high_bound : float = 4.5, default_cop_low_bound : float = 0,
                             default_boundary_fault_time : int = 15, site_name : str = None, day_table_name_header : str = "day",
                             power_ratio_period_days : int = 7) -> pd.DataFrame:
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    if (system != ""):
        if not 'system' in bounds_df.columns:
            raise Exception("system parameter is non null, however, system is not present in Variable_Names.csv")
        bounds_df = bounds_df.loc[bounds_df['system'] == system]
    
    day_list = daily_data.index.to_list()
    print('Checking for alarms...')
    alarm_df = _convert_silent_alarm_dict_to_df({})
    dict_of_alarms = {}
    dict_of_alarms['boundary'] = Boundary(bounds_df, default_fault_time= default_boundary_fault_time)
    # flag_boundary_alarms(df, config, full_days=day_list, system=system, default_fault_time= default_boundary_fault_time)
    dict_of_alarms['power ratio'] = PowerRatio(bounds_df, day_table_name = config.get_table_name(day_table_name_header), ratio_period_days=power_ratio_period_days)
    # power_ratio_alarm(daily_data, config, day_table_name = config.get_table_name(day_table_name_header), system=system, ratio_period_days=power_ratio_period_days)
    dict_of_alarms['abnormal COP'] = AbnormalCOP(bounds_df, default_high_bound=default_cop_high_bound, default_low_bound=default_cop_low_bound)
    # flag_abnormal_COP(daily_data, config, system = system, default_high_bound=default_cop_high_bound, default_low_bound=default_cop_low_bound)
    dict_of_alarms['temperature maintenance setpoint'] = TMSetpoint(bounds_df)
    # flag_high_tm_setpoint(df, daily_data, config, system=system)
    dict_of_alarms['recirculation loop balancing valve'] = BalancingValve(bounds_df)
    # flag_recirc_balance_valve(daily_data, config, system=system)
    dict_of_alarms['HPWH inlet temperature'] = HPWHInlet(bounds_df)
    # flag_hp_inlet_temp(df, daily_data, config, system)
    dict_of_alarms['HPWH outlet temperature'] = HPWHOutlet(bounds_df)
    # flag_hp_outlet_temp(df, daily_data, config, system)
    dict_of_alarms['improper backup heating use'] = BackupUse(bounds_df)
    # flag_backup_use(df, daily_data, config, system)
    dict_of_alarms['HPWH outage'] = HPWHOutage(bounds_df, day_table_name = config.get_table_name(day_table_name_header))
    # flag_HP_outage(df, daily_data, config, day_table_name = config.get_table_name(day_table_name_header), system=system)
    dict_of_alarms['blown equipment fuse'] = BlownFuse(bounds_df)
    # flag_blown_fuse(df, daily_data, config, system)
    dict_of_alarms['unexpected SOO change'] = SOOChange(bounds_df)
    # flag_unexpected_soo_change(df, daily_data, config, system)
    dict_of_alarms['short cycle'] = ShortCycle(bounds_df)
    # flag_shortcycle(df, daily_data, config, system)
    dict_of_alarms['unexpected temperature'] = TempRange(bounds_df)
    # flag_unexpected_temp(df, daily_data, config, system)
    dict_of_alarms['demand response inconsistency'] = LSInconsist(bounds_df)
    # flag_ls_mode_inconsistancy(df, daily_data, config, system)
    # return alarm.find_alarms(df, daily_df, config)

    ongoing_COP_exception = ['abnormal COP']
    for key, value in dict_of_alarms.items():
        # if key in ongoing_COP_exception and _check_if_during_ongoing_cop_alarm(daily_data, config, site_name):
        #     print("Ongoing DATA_LOSS_COP detected. ABNORMAL_COP events will be uploaded")
        specific_alarm_df = value.find_alarms(df, daily_data, config)
        if len(specific_alarm_df) > 0:
            print(f"Detected {key} alarm(s). Adding to event df...")
            alarm_df = pd.concat([alarm_df, specific_alarm_df])
        else:
            print(f"No {key} alarm(s) detected.")

    # for key, value in dict_of_alarms.items():
    #     if key in ongoing_COP_exception and _check_if_during_ongoing_cop_alarm(daily_data, config, site_name):
    #         print("Ongoing DATA_LOSS_COP detected. No further DATA_LOSS_COP events will be uploaded")
    #     elif len(value) > 0:
    #         print(f"Detected {key} alarm(s). Adding to event df...")
    #         alarm_df = pd.concat([alarm_df, value])
    #     else:
    #         print(f"No {key} alarm(s) detected.")

    return alarm_df

def flag_abnormal_COP(daily_data: pd.DataFrame, config : ConfigManager, system: str = "", default_high_bound : float = 4.5, default_low_bound : float = 0) -> pd.DataFrame:
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    alarm = AbnormalCOP(bounds_df, default_high_bound, default_low_bound)
    return alarm.find_alarms(None, daily_data, config)


def flag_boundary_alarms(df: pd.DataFrame, config : ConfigManager, default_fault_time : int = 15, system: str = "", full_days : list = None) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    Parameters
    ----------
    df: pd.DataFrame
        post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file 
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least three columns which must be titled "variable_name", "low_alarm", and "high_alarm" which should contain the
        name of each variable in the dataframe that requires the alarming, the lower bound for acceptable data, and the upper bound for
        acceptable data respectively
    default_fault_time : int
        Number of consecutive minutes that a sensor must be out of bounds for to trigger an alarm. Can be customized for each variable with 
        the fault_time column in Variable_Names.csv
    system: str
        string of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not aplicable.
    full_days : list
        list of pd.Datetimes that should be considered full days here. If set to none, will take any day at all present in df

    Returns
    ------- 
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag boundary alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    alarm = Boundary(bounds_df, default_fault_time)
    return alarm.find_alarms(df, None, config)

def flag_high_tm_setpoint(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, default_fault_time : int = 3, 
                             system: str = "", default_setpoint : float = 130.0, default_power_indication : float = 1.0,
                             default_power_ratio : float = 0.4) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    VarNames syntax:
    TMSTPT_T_ID:### - Swing Tank Outlet Temperature. Alarm triggered if over number ### (or 130) for 3 minutes with power on
    TMSTPT_SP_ID:### - Swing Tank Power. ### is lowest recorded power for Swing Tank to be considered 'on'. Defaults to 1.0
    TMSTPT_TP_ID:### - Total System Power for ratio alarming for alarming if swing tank power is more than ### (40% default) of usage
    TMSTPT_ST_ID:### - Swing Tank Setpoint that should not change at all from ### (default 130)

    Parameters
    ----------
    df: pd.DataFrame
        post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        post-transformed dataframe for daily data. Used for checking power ratios and determining which days to process.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the TMSTPT alarm codes (e.g., TMSTPT_T_1:140, TMSTPT_SP_1:2.0)
    default_fault_time : int
        Number of consecutive minutes for T+SP alarms (default 3). T+SP alarms trigger when tank is powered and temperature exceeds
        setpoint for this many consecutive minutes.
    system: str
        string of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not aplicable.
    default_setpoint : float
        Default temperature setpoint in degrees for T and ST alarm codes when no custom bound is specified (default 130.0)
    default_power_indication : float
        Default power threshold in kW for SP alarm codes when no custom bound is specified (default 1.0)
    default_power_ratio : float
        Default power ratio threshold (as decimal, e.g., 0.4 for 40%) for TP alarm codes when no custom bound is specified (default 0.4)

    Returns
    ------- 
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag swing tank setpoint alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    alarm = TMSetpoint(bounds_df, default_fault_time, default_setpoint, default_power_indication, default_power_ratio)
    return alarm.find_alarms(df, daily_df, config)

def flag_backup_use(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, 
                             system: str = "", default_setpoint : float = 130.0, default_power_ratio : float = 0.1) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    VarNames syntax:
    BU_P_ID - Back Up Tank Power Variable. Must be in same power units as total system power
    BU_TP_ID:### - Total System Power for ratio alarming for alarming if back up power is more than ### (40% default) of usage
    BU_ST_ID:### - Back Up Setpoint that should not change at all from ### (default 130)

    Parameters
    ----------
    df: pd.DataFrame
        post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        post-transformed dataframe for daily data. Used for checking power ratios and determining which days to process.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the STS alarm codes (e.g., STS_T_1:140, STS_SP_1:2.0)
    system: str
        string of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not aplicable.
    default_setpoint : float
        Default temperature setpoint in degrees for T and ST alarm codes when no custom bound is specified (default 130.0)
    default_power_indication : float
        Default power threshold in kW for SP alarm codes when no custom bound is specified (default 1.0)
    default_power_ratio : float
        Default power ratio threshold (as decimal, e.g., 0.4 for 40%) for TP alarm codes when no custom bound is specified (default 0.4)

    Returns
    ------- 
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag swing tank setpoint alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    alarm = BackupUse(bounds_df,  default_setpoint, default_power_ratio)
    return alarm.find_alarms(df, daily_df, config)

def flag_HP_outage(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, day_table_name : str, system: str = "", default_power_ratio : float = 0.3,
                   ratio_period_days : int = 7) -> pd.DataFrame:
    """
    Detects possible heat pump failures or outages by checking if heat pump power consumption falls below
    an expected ratio of total system power over a rolling period, or by checking for non-zero values in
    a direct alarm variable from the heat pump controller.

    VarNames syntax:
    HPOUT_POW_[OPTIONAL ID]:### - Heat pump power variable. ### is the minimum expected ratio of HP power to total power
        (default 0.3 for 30%). Must be in same power units as total system power.
    HPOUT_TP_[OPTIONAL ID] - Total system power variable for ratio comparison. Required when using POW codes.
    HPOUT_ALRM_[OPTIONAL ID] - Direct alarm variable from HP controller. Alarm triggers if any non-zero value is detected.

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. Used for checking ALRM codes for non-zero values.
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data. Used for checking power ratios over the rolling period.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the HPOUT alarm codes (e.g., HPOUT_POW_1:0.3, HPOUT_TP_1, HPOUT_ALRM_1).
    day_table_name : str
        Name of the daily database table to fetch previous days' data for the rolling period calculation.
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_ratio : float
        Default minimum power ratio threshold (as decimal, e.g., 0.3 for 30%) for POW alarm codes when no custom bound is specified (default 0.3).
        An alarm triggers if HP power falls below this ratio of total power over the rolling period.
    ratio_period_days : int
        Number of days to use for the rolling power ratio calculation (default 7). Must be greater than 1.

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag swing tank setpoint alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    
    alarm = HPWHOutage(bounds_df,  day_table_name, default_power_ratio, ratio_period_days)
    return alarm.find_alarms(df, daily_df, config) 

def flag_recirc_balance_valve(daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_ratio : float = 0.4) -> pd.DataFrame:
    """
    Detects recirculation balance issues by comparing sum of ER (equipment recirculation) heater
    power to either total power or heating output.

    VarNames syntax:
    BV_ER_[OPTIONAL ID] - Indicates a power variable for an ER heater (equipment recirculation).
        Multiple ER variables with the same ID will be summed together.
    BV_TP_[OPTIONAL ID]:### - Indicates the Total Power of the system. Optional ### for the percentage
        threshold that should not be crossed by the ER elements (default 0.4 for 40%).
        Alarm triggers when sum of ER >= total_power * threshold.
    BV_OUT_[OPTIONAL ID] - Indicates the heating output variable the ER heating contributes to.
        Alarm triggers when sum of ER > sum of OUT * 0.95 (i.e., ER exceeds 95% of heating output).
        Multiple OUT variables with the same ID will be summed together.

    Note: Each alarm ID requires at least one ER code AND either one TP code OR at least one OUT code.
    If a TP code exists for an ID, it takes precedence over OUT codes.

    Parameters
    ----------
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data. Used for checking recirculation balance by comparing sum of ER equipment
        power to total power or heating output power.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the BV alarm codes (e.g., BV_ER_1, BV_TP_1:0.3)
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_ratio : float
        Default power ratio threshold (as decimal, e.g., 0.4 for 40%) for TP alarm codes when no custom bound is specified (default 0.4).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if daily_df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    alarm = BalancingValve(bounds_df, default_power_ratio)
    return alarm.find_alarms(None, daily_df, config)

def flag_hp_inlet_temp(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_threshold : float = 1.0,
                       default_temp_threshold : float = 115.0, fault_time : int = 5) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    VarNames syntax:
    HPI_POW_[OPTIONAL ID]:### - Indicates a power variable for the heat pump. ### is the power threshold (default 1.0) above which
        the heat pump is considered 'on'
    HPI_T_[OPTIONAL ID]:### - Indicates heat pump inlet temperature variable. ### is the temperature threshold (default 120.0)
        that should not be exceeded while the heat pump is on

    Parameters
    ----------
    df: pd.DataFrame
        post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        post-transformed dataframe for daily data.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the HPI alarm codes (e.g., HPI_POW_1:0.5, HPI_T_1:125.0)
    system: str
        string of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not aplicable.
    default_power_threshold : float
        Default power threshold for POW alarm codes when no custom bound is specified (default 0.4). Heat pump is considered 'on'
        when power exceeds this value.
    default_temp_threshold : float
        Default temperature threshold for T alarm codes when no custom bound is specified (default 120.0). Alarm triggers when
        temperature exceeds this value while heat pump is on.
    fault_time : int
        Number of consecutive minutes that both power and temperature must exceed their thresholds before triggering an alarm (default 10).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    alarm = HPWHInlet(bounds_df, default_power_threshold, default_temp_threshold, fault_time)
    return alarm.find_alarms(df, daily_df, config)

def flag_hp_outlet_temp(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_threshold : float = 1.0,
                       default_temp_threshold : float = 140.0, fault_time : int = 5) -> pd.DataFrame:
    """
    Detects low heat pump outlet temperature by checking if the outlet temperature falls below a threshold
    while the heat pump is running. The first 10 minutes after each HP turn-on are excluded as a warmup
    period. An alarm triggers if the temperature stays below the threshold for `fault_time` consecutive
    minutes after the warmup period.

    VarNames syntax:
    HPO_POW_[OPTIONAL ID]:### - Indicates a power variable for the heat pump. ### is the power threshold (default 1.0) above which
        the heat pump is considered 'on'.
    HPO_T_[OPTIONAL ID]:### - Indicates heat pump outlet temperature variable. ### is the temperature threshold (default 140.0)
        that should always be exceeded while the heat pump is on after the 10-minute warmup period.

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the HPO alarm codes (e.g., HPO_POW_1:1.0, HPO_T_1:140.0).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_threshold : float
        Default power threshold for POW alarm codes when no custom bound is specified (default 1.0). Heat pump is considered 'on'
        when power exceeds this value.
    default_temp_threshold : float
        Default temperature threshold for T alarm codes when no custom bound is specified (default 140.0). Alarm triggers when
        temperature falls BELOW this value while heat pump is on (after warmup period).
    fault_time : int
        Number of consecutive minutes that temperature must be below threshold (after warmup) before triggering an alarm (default 5).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    
    alarm = HPWHOutlet(bounds_df, default_power_threshold, default_temp_threshold, fault_time)
    return alarm.find_alarms(df, daily_df, config)

def flag_blown_fuse(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_threshold : float = 1.0,
                       default_power_range : float = 2.0, default_power_draw : float = 30, fault_time : int = 3) -> pd.DataFrame:
    """
    Detects blown fuse alarms for heating elements by identifying when an element is drawing power
    but significantly less than expected, which may indicate a blown fuse.

    VarNames syntax:
    BF_[OPTIONAL ID]:### - Indicates a blown fuse alarm for an element. ### is the expected kW input when the element is on.

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the BF alarm codes (e.g., BF:30, BF_1:25).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_threshold : float
        Power threshold to determine if the element is "on" (default 1.0). Element is considered on when power exceeds this value.
    default_power_range : float
        Allowable variance below the expected power draw (default 2.0). An alarm triggers when the actual power draw is less than
        (expected_power_draw - default_power_range) while the element is on.
    default_power_draw : float
        Default expected power draw in kW when no custom bound is specified in the alarm code (default 30).
    fault_time : int
        Number of consecutive minutes that the fault condition must persist before triggering an alarm (default 3).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    alarm = BlownFuse(bounds_df, default_power_threshold, default_power_range, default_power_draw,fault_time)
    return alarm.find_alarms(df, daily_df, config)

def flag_unexpected_soo_change(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_threshold : float = 1.0,
                       default_on_temp : float = 115.0, default_off_temp : float = 140.0) -> pd.DataFrame:
    """
    Detects unexpected state of operation (SOO) changes by checking if the heat pump turns on or off
    when the temperature is not near the expected aquastat setpoint thresholds. An alarm is triggered
    if the HP turns on/off and the corresponding temperature is more than 5.0 degrees away from the
    expected threshold.

    VarNames syntax:
    SOOCHNG_POW:### - Indicates a power variable for the heat pump system (should be total power across all primary heat pumps). ### is the power threshold (default 1.0) above which
        the heat pump system is considered 'on'.
    SOOCHNG_ON_[Mode ID]:### - Indicates the temperature variable at the ON aquastat fraction. ### is the temperature (default 115.0)
        that should trigger the heat pump to turn ON. Mode ID should be the load up mode from ['loadUp','shed','criticalPeak','gridEmergency','advLoadUp','normal'] or left blank for normal mode
    SOOCHNG_OFF_[Mode ID]:### - Indicates the temperature variable at the OFF aquastat fraction (can be same as ON aquastat). ### is the temperature (default 140.0)
        that should trigger the heat pump to turn OFF. Mode ID should be the load up mode from ['loadUp','shed','criticalPeak','gridEmergency','advLoadUp','normal'] or left blank for normal mode

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the SOOCHNG alarm codes (e.g., SOOCHNG_POW_normal:1.0, SOOCHNG_ON_normal:115.0, SOOCHNG_OFF_normal:140.0).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_threshold : float
        Default power threshold for POW alarm codes when no custom bound is specified (default 1.0). Heat pump is considered 'on'
        when power exceeds this value.
    default_on_temp : float
        Default ON temperature threshold (default 115.0). When the HP turns on, an alarm triggers if the temperature
        is more than 5.0 degrees away from this value.
    default_off_temp : float
        Default OFF temperature threshold (default 140.0). When the HP turns off, an alarm triggers if the temperature
        is more than 5.0 degrees away from this value.

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    alarm = SOOChange(bounds_df, default_power_threshold, default_on_temp, default_off_temp)
    return alarm.find_alarms(df, daily_df, config)

def flag_ls_mode_inconsistancy(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "") -> pd.DataFrame:
    """
    Detects when reported loadshift mode does not match its expected value during a load shifting event.
    An alarm is triggered if the variable value does not equal the expected value during the
    time periods defined in the load shifting schedule for that mode.

    VarNames syntax:
    SOO_[mode]:### - Indicates a variable that should equal ### during [mode] load shifting events.
        [mode] can be: normal, loadUp, shed, criticalPeak, gridEmergency, advLoadUp
        ### is the expected value (e.g., SOO_loadUp:1 means the variable should be 1 during loadUp events)

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. It should be noted that this function expects consecutive,
        in order minutes. If minutes are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        Pandas dataframe with daily data. This dataframe should have a datetime index.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems.

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag load shift mode inconsistency alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    
    alarm = LSInconsist(bounds_df)
    return alarm.find_alarms(df, daily_df, config)

def flag_unexpected_temp(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_high_temp : float = 130,
                       default_low_temp : float = 115, fault_time : int = 10) -> pd.DataFrame:
    """
    Detects when a temperature value falls outside an acceptable range for
    too long. An alarm is triggered if the temperature is above the high bound or below the low bound
    for `fault_time` consecutive minutes.

    VarNames syntax:
    TMPRNG_[OPTIONAL ID]:###-### - Indicates a temperature variable. ###-### is the acceptable temperature range
        (e.g., TMPRNG:110-130 means temperature should stay between 110 and 130 degrees).

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data. Used for determining which days to process.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the DHW alarm codes (e.g., DHW:110-130, DHW_1:115-125).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_high_temp : float
        Default high temperature bound when no custom range is specified in the alarm code (default 130). Temperature above this triggers alarm.
    default_low_temp : float
        Default low temperature bound when no custom range is specified in the alarm code (default 130). Temperature below this triggers alarm.
    fault_time : int
        Number of consecutive minutes that temperature must be outside the acceptable range before triggering an alarm (default 10).

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    temp_alarm = TempRange(bounds_df, default_high_temp, default_low_temp, fault_time)
    return temp_alarm.find_alarms(df, daily_df, config)

def flag_shortcycle(df: pd.DataFrame, daily_df: pd.DataFrame, config : ConfigManager, system: str = "", default_power_threshold : float = 1.0,
                       short_cycle_time : int = 15) -> pd.DataFrame:
    """
    Detects short cycling by identifying when the heat pump turns on for less than `short_cycle_time`
    consecutive minutes before turning off again. Short cycling can indicate equipment issues or
    improper system sizing.

    VarNames syntax:
    SHRTCYC_[OPTIONAL ID]:### - Indicates a power variable for the heat pump. ### is the power threshold (default 1.0) above which
        the heat pump is considered 'on'.

    Parameters
    ----------
    df: pd.DataFrame
        Post-transformed dataframe for minute data. It should be noted that this function expects consecutive, in order minutes. If minutes
        are out of order or have gaps, the function may return erroneous alarms.
    daily_df: pd.DataFrame
        Post-transformed dataframe for daily data.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name" and "alarm_codes" which should contain the
        name of each variable in the dataframe that requires alarming and the SHRTCYC alarm codes (e.g., SHRTCYC:1.0, SHRTCYC_1:0.5).
    system: str
        String of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not applicable.
    default_power_threshold : float
        Default power threshold when no custom bound is specified in the alarm code (default 1.0). Heat pump is considered 'on'
        when power exceeds this value.
    short_cycle_time : int
        Minimum expected run time in minutes (default 15). An alarm triggers if the heat pump runs for fewer than this many
        consecutive minutes before turning off.

    Returns
    -------
    pd.DataFrame:
        Pandas dataframe with alarm events
    """
    if df.empty:
        print("cannot flag missing balancing valve alarms. Dataframe is empty")
        return pd.DataFrame()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()

    short_alarm = ShortCycle(bounds_df, default_power_threshold, short_cycle_time)
    return short_alarm.find_alarms(df, daily_df, config)


def _convert_silent_alarm_dict_to_df(alarm_dict : dict) -> pd.DataFrame:
    events = {
        'start_time_pt' : [],
        'end_time_pt' : [],
        'alarm_type' : [],
        'alarm_detail' : [],
        'variable_name' : []
    }
    for key, value_list in alarm_dict.items():
        for value in value_list:
            events['start_time_pt'].append(key)
            # Use end_time from value[2] if provided, otherwise use key
            events['end_time_pt'].append(value[2] if len(value) > 2 else key)
            events['alarm_type'].append(value[3] if len(value) > 3 else 'SILENT_ALARM')
            events['alarm_detail'].append(value[1])
            events['variable_name'].append(value[0])

    event_df = pd.DataFrame(events)
    event_df.set_index('start_time_pt', inplace=True)
    return event_df


def power_ratio_alarm(daily_df: pd.DataFrame, config : ConfigManager, day_table_name : str, system: str = "", verbose : bool = False, ratio_period_days : int = 7) -> pd.DataFrame:
    """
    Function will take a pandas dataframe of daily data and location of alarm information in a csv,
    and create an dataframe with applicable alarm events

    Parameters
    ----------
    daily_df: pd.DataFrame
        post-transformed dataframe for daily data. It should be noted that this function expects consecutive, in order days. If days
        are out of order or have gaps, the function may return erroneous alarms.
    config : ecopipeline.ConfigManager
        The ConfigManager object that holds configuration data for the pipeline. Among other things, this object will point to a file 
        called Variable_Names.csv in the input folder of the pipeline (e.g. "full/path/to/pipeline/input/Variable_Names.csv").
        The file must have at least two columns which must be titled "variable_name", "alarm_codes" which should contain the
        name of each variable in the dataframe that requires the alarming and the ratio alarm code in the form "PR_{Power Ratio Name}:{low percentage}-{high percentage}
    system: str
        string of system name if processing a particular system in a Variable_Names.csv file with multiple systems. Leave as an empty string if not aplicable.
    verbose : bool
        add print statements in power ratio

    Returns
    ------- 
    pd.DataFrame:
        Pandas dataframe with alarm events, empty if no alarms triggered
    """
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    alarm = PowerRatio(bounds_df, day_table_name, ratio_period_days)
    return alarm.find_alarms(None, daily_df, config)
