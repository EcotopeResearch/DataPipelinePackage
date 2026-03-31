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
                             default_boundary_fault_time : int = 15, day_table_name_header : str = "day",
                             power_ratio_period_days : int = 7) -> pd.DataFrame:
    """
    Run all available alarm detectors and return a combined alarm event DataFrame.

    Iterates over every alarm type (boundary, power ratio, abnormal COP, TM
    setpoint, balancing valve, HPWH inlet/outlet, backup use, HPWH outage,
    blown fuse, unexpected SOO change, short cycle, unexpected temperature, and
    demand-response inconsistency) and concatenates any detected events into a
    single result.

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    daily_data : pd.DataFrame
        Post-transformed daily-level data DataFrame with a datetime index.
    config : ConfigManager
        Pipeline configuration object.  Used to locate ``Variable_Names.csv``
        and to resolve database table names.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_cop_high_bound : float, optional
        Upper COP threshold for the abnormal-COP alarm (default 4.5).
    default_cop_low_bound : float, optional
        Lower COP threshold for the abnormal-COP alarm (default 0).
    default_boundary_fault_time : int, optional
        Consecutive minutes a sensor must be out of bounds before a boundary
        alarm triggers (default 15).
    day_table_name_header : str, optional
        Key passed to ``config.get_table_name()`` to resolve the daily database
        table name (default ``"day"``).
    power_ratio_period_days : int, optional
        Rolling window in days used by the power-ratio alarm (default 7).

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events with columns ``end_time_pt``, ``alarm_type``,
        ``alarm_detail``, and ``variable_name``, indexed by ``start_time_pt``.
        Returns an empty DataFrame if ``df`` is empty or ``Variable_Names.csv``
        cannot be found.

    Raises
    ------
    Exception
        If ``system`` is non-empty but the ``system`` column is absent from
        ``Variable_Names.csv``.
    """
    print("++++++++++++ ALARM ++++++++++++")
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
    print('Checking for alarms...')
    alarm_df = _convert_silent_alarm_dict_to_df({})
    dict_of_alarms = {}
    dict_of_alarms['boundary'] = Boundary(bounds_df, default_fault_time= default_boundary_fault_time)
    dict_of_alarms['power ratio'] = PowerRatio(bounds_df, day_table_name = config.get_table_name(day_table_name_header), ratio_period_days=power_ratio_period_days)
    dict_of_alarms['abnormal COP'] = AbnormalCOP(bounds_df, default_high_bound=default_cop_high_bound, default_low_bound=default_cop_low_bound)
    dict_of_alarms['temperature maintenance setpoint'] = TMSetpoint(bounds_df)
    dict_of_alarms['recirculation loop balancing valve'] = BalancingValve(bounds_df)
    dict_of_alarms['HPWH inlet temperature'] = HPWHInlet(bounds_df)
    dict_of_alarms['HPWH outlet temperature'] = HPWHOutlet(bounds_df)
    dict_of_alarms['improper backup heating use'] = BackupUse(bounds_df)
    dict_of_alarms['HPWH outage'] = HPWHOutage(bounds_df, day_table_name = config.get_table_name(day_table_name_header))
    dict_of_alarms['blown equipment fuse'] = BlownFuse(bounds_df)
    dict_of_alarms['unexpected SOO change'] = SOOChange(bounds_df)
    dict_of_alarms['short cycle'] = ShortCycle(bounds_df)
    dict_of_alarms['unexpected temperature'] = TempRange(bounds_df)
    dict_of_alarms['demand response inconsistency'] = LSInconsist(bounds_df)

    # ongoing_COP_exception = ['abnormal COP']
    for key, value in dict_of_alarms.items():
        # if key in ongoing_COP_exception and _check_if_during_ongoing_cop_alarm(daily_data, config, site_name):
        #     print("Ongoing DATA_LOSS_COP detected. ABNORMAL_COP events will be uploaded")
        specific_alarm_df = value.find_alarms(df, daily_data, config)
        if len(specific_alarm_df) > 0:
            print(f"Detected {key} alarm(s). Adding to event df...")
            alarm_df = pd.concat([alarm_df, specific_alarm_df])
        else:
            print(f"No {key} alarm(s) detected.")

    return alarm_df

def flag_abnormal_COP(daily_data: pd.DataFrame, config : ConfigManager, system: str = "", default_high_bound : float = 4.5, default_low_bound : float = 0) -> pd.DataFrame:
    """
    Detect days with an abnormal coefficient of performance (COP) value.

    Reads alarm configuration from ``Variable_Names.csv`` via ``config`` and
    delegates detection to :class:`AbnormalCOP`.

    Parameters
    ----------
    daily_data : pd.DataFrame
        Post-transformed daily-level data DataFrame with a datetime index.
    config : ConfigManager
        Pipeline configuration object used to locate ``Variable_Names.csv``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_high_bound : float, optional
        Upper COP threshold above which an alarm is triggered (default 4.5).
    default_low_bound : float, optional
        Lower COP threshold below which an alarm is triggered (default 0).

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if
        ``Variable_Names.csv`` cannot be found.
    """
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
    Flag out-of-bounds sensor readings and return alarm events.

    Reads acceptable sensor ranges from ``Variable_Names.csv`` via ``config``
    and triggers an alarm whenever a sensor stays outside its bounds for
    ``default_fault_time`` consecutive minutes.

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name``, ``low_alarm``,
        and ``high_alarm``.
    default_fault_time : int, optional
        Number of consecutive minutes a sensor must be out of bounds before an
        alarm is triggered (default 15).  Can be overridden per variable using
        a ``fault_time`` column in ``Variable_Names.csv``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    full_days : list, optional
        List of ``pd.Timestamp`` objects representing complete days to consider.
        If ``None`` (default), every day present in ``df`` is used.

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    Detect temperature-maintenance (TM) setpoint violations on swing-tank equipment.

    Triggers alarms when the swing tank outlet temperature exceeds the setpoint
    while the tank is powered, or when the swing tank consumes a disproportionate
    share of total system power.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``TMSTPT_T_ID:###`` — Swing Tank Outlet Temperature.  Alarm triggers if
      temperature exceeds ``###`` (default ``default_setpoint``) for
      ``default_fault_time`` consecutive minutes with power on.
    - ``TMSTPT_SP_ID:###`` — Swing Tank Power.  ``###`` is the minimum power (kW)
      for the tank to be considered *on* (default ``default_power_indication``).
    - ``TMSTPT_TP_ID:###`` — Total System Power for ratio alarming.  Alarm triggers
      when swing-tank power exceeds ``###`` fraction (default
      ``default_power_ratio``) of total power.
    - ``TMSTPT_ST_ID:###`` — Swing Tank Setpoint that must remain equal to ``###``
      (default ``default_setpoint``).

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.  Used for power-ratio checks
        and to determine which days to process.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.
    default_fault_time : int, optional
        Consecutive minutes the T+SP fault condition must persist before
        triggering (default 3).
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_setpoint : float, optional
        Default temperature setpoint in degrees F for T and ST alarm codes when
        no custom bound is provided (default 130.0).
    default_power_indication : float, optional
        Default power threshold in kW for SP alarm codes when no custom bound is
        provided (default 1.0).
    default_power_ratio : float, optional
        Default power-ratio threshold (e.g., 0.4 for 40 %) for TP alarm codes
        when no custom bound is provided (default 0.4).

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    Detect improper backup heating use based on power consumption and setpoint checks.

    Triggers alarms when the backup tank consumes a disproportionate share of
    total system power or when the backup setpoint deviates from its expected
    value.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``BU_P_ID`` — Backup tank power variable.  Must use the same power units
      as total system power.
    - ``BU_TP_ID:###`` — Total system power for ratio alarming.  Alarm triggers
      when backup power exceeds ``###`` fraction of total power (default
      ``default_power_ratio``).
    - ``BU_ST_ID:###`` — Backup setpoint that must not deviate from ``###``
      (default ``default_setpoint``).

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.  Used for power-ratio checks
        and to determine which days to process.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_setpoint : float, optional
        Default temperature setpoint in degrees F for ST alarm codes when no
        custom bound is provided (default 130.0).
    default_power_ratio : float, optional
        Default power-ratio threshold (e.g., 0.1 for 10 %) for TP alarm codes
        when no custom bound is provided (default 0.1).

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    Detect possible heat pump failures or outages.

    Checks whether heat pump power consumption falls below an expected ratio of
    total system power over a rolling period, or whether a direct alarm variable
    from the heat pump controller reports a non-zero value.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``HPOUT_POW_[OPTIONAL ID]:###`` — Heat pump power variable.  ``###`` is
      the minimum expected ratio of HP power to total power (default
      ``default_power_ratio``).  Must use the same power units as total system
      power.
    - ``HPOUT_TP_[OPTIONAL ID]`` — Total system power variable for ratio
      comparison.  Required when using POW codes.
    - ``HPOUT_ALRM_[OPTIONAL ID]`` — Direct alarm variable from the HP
      controller.  Alarm triggers if any non-zero value is detected.

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Used for checking ALRM
        codes for non-zero values.
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.  Used for checking power
        ratios over the rolling period.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.
    day_table_name : str
        Name of the daily database table used to fetch previous days' data for
        the rolling-period calculation.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_power_ratio : float, optional
        Minimum power-ratio threshold (e.g., 0.3 for 30 %) for POW alarm codes
        when no custom bound is provided (default 0.3).  An alarm triggers when
        HP power falls below this ratio of total power over the rolling period.
    ratio_period_days : int, optional
        Number of days in the rolling power-ratio window (default 7).  Must be
        greater than 1.

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    Detect recirculation loop balancing valve issues.

    Compares the sum of equipment-recirculation (ER) heater power to either
    total system power or heating output to identify imbalance.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``BV_ER_[OPTIONAL ID]`` — Power variable for an ER heater.  Multiple ER
      variables sharing the same ID are summed together.
    - ``BV_TP_[OPTIONAL ID]:###`` — Total system power.  Optional ``###`` sets
      the ratio threshold (default ``default_power_ratio``).  Alarm triggers
      when sum of ER >= total_power * threshold.
    - ``BV_OUT_[OPTIONAL ID]`` — Heating output variable that ER heating
      contributes to.  Alarm triggers when sum of ER > sum of OUT * 0.95.
      Multiple OUT variables sharing the same ID are summed together.

    Each alarm ID requires at least one ER code and either one TP code or at
    least one OUT code.  When a TP code is present it takes precedence over OUT
    codes.

    Parameters
    ----------
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.  Used to compare ER
        equipment power against total power or heating output.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_power_ratio : float, optional
        Default power-ratio threshold (e.g., 0.4 for 40 %) for TP alarm codes
        when no custom bound is provided (default 0.4).

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if
        ``daily_df`` is empty or ``Variable_Names.csv`` cannot be found.
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
    Detect high heat pump inlet temperature while the heat pump is running.

    Triggers an alarm when the heat pump is on and its inlet temperature exceeds
    the threshold for ``fault_time`` consecutive minutes.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``HPI_POW_[OPTIONAL ID]:###`` — Heat pump power variable.  ``###`` is the
      power threshold (default ``default_power_threshold``) above which the heat
      pump is considered *on*.
    - ``HPI_T_[OPTIONAL ID]:###`` — Heat pump inlet temperature variable.
      ``###`` is the temperature threshold (default ``default_temp_threshold``)
      that must not be exceeded while the heat pump is on.

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_power_threshold : float, optional
        Default power threshold in kW for POW alarm codes when no custom bound
        is provided (default 1.0).  Heat pump is considered *on* when power
        exceeds this value.
    default_temp_threshold : float, optional
        Default temperature threshold in degrees F for T alarm codes when no
        custom bound is provided (default 115.0).  Alarm triggers when
        temperature exceeds this value while the heat pump is on.
    fault_time : int, optional
        Consecutive minutes that both power and temperature must exceed their
        thresholds before an alarm is triggered (default 5).

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    Detect low heat pump outlet temperature while the heat pump is running.

    The first 10 minutes after each HP turn-on are excluded as a warm-up period.
    An alarm triggers if the outlet temperature stays below the threshold for
    ``fault_time`` consecutive minutes after the warm-up period.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``HPO_POW_[OPTIONAL ID]:###`` — Heat pump power variable.  ``###`` is the
      power threshold (default ``default_power_threshold``) above which the heat
      pump is considered *on*.
    - ``HPO_T_[OPTIONAL ID]:###`` — Heat pump outlet temperature variable.
      ``###`` is the temperature threshold (default ``default_temp_threshold``)
      that must be exceeded while the heat pump is on after the 10-minute
      warm-up period.

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_power_threshold : float, optional
        Default power threshold in kW for POW alarm codes when no custom bound
        is provided (default 1.0).  Heat pump is considered *on* when power
        exceeds this value.
    default_temp_threshold : float, optional
        Default temperature threshold in degrees F for T alarm codes when no
        custom bound is provided (default 140.0).  Alarm triggers when
        temperature falls below this value while the heat pump is on (after
        warm-up period).
    fault_time : int, optional
        Consecutive minutes that temperature must remain below threshold (after
        warm-up) before an alarm is triggered (default 5).

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    Detect blown fuse conditions on heating elements.

    Identifies when a heating element is drawing power but significantly less
    than expected, which may indicate a blown fuse.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``BF_[OPTIONAL ID]:###`` — Blown fuse alarm for an element.  ``###`` is
      the expected kW draw when the element is on.

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_power_threshold : float, optional
        Power threshold in kW used to determine whether the element is *on*
        (default 1.0).  Element is considered on when power exceeds this value.
    default_power_range : float, optional
        Allowable variance below the expected power draw in kW (default 2.0).
        An alarm triggers when actual power is less than
        ``expected_power_draw - default_power_range`` while the element is on.
    default_power_draw : float, optional
        Default expected power draw in kW when no custom bound is specified in
        the alarm code (default 30).
    fault_time : int, optional
        Consecutive minutes the fault condition must persist before an alarm is
        triggered (default 3).

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    Detect unexpected state-of-operation (SOO) changes.

    Triggers an alarm when the heat pump turns on or off at a temperature that
    is more than 5.0 degrees away from the expected aquastat setpoint threshold
    for the current load-shifting mode.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``SOOCHNG_POW:###`` — Total power variable across all primary heat pumps.
      ``###`` is the power threshold (default ``default_power_threshold``) above
      which the HP system is considered *on*.
    - ``SOOCHNG_ON_[Mode ID]:###`` — Temperature variable at the ON aquastat
      setpoint.  ``###`` is the temperature (default ``default_on_temp``) that
      triggers the HP to turn ON.  Mode ID should be one of
      ``loadUp``, ``shed``, ``criticalPeak``, ``gridEmergency``,
      ``advLoadUp``, or ``normal`` (or left blank for normal mode).
    - ``SOOCHNG_OFF_[Mode ID]:###`` — Temperature variable at the OFF aquastat
      setpoint (may be the same as the ON variable).  ``###`` is the temperature
      (default ``default_off_temp``) that triggers the HP to turn OFF.  Same
      Mode ID options as above.

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_power_threshold : float, optional
        Default power threshold in kW for POW alarm codes when no custom bound
        is provided (default 1.0).  Heat pump is considered *on* when power
        exceeds this value.
    default_on_temp : float, optional
        Default ON temperature threshold in degrees F (default 115.0).  An alarm
        triggers when the HP turns on and the temperature is more than 5.0
        degrees from this value.
    default_off_temp : float, optional
        Default OFF temperature threshold in degrees F (default 140.0).  An alarm
        triggers when the HP turns off and the temperature is more than 5.0
        degrees from this value.

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    Detect load-shift mode inconsistencies.

    Triggers an alarm when a reported load-shift mode variable does not match
    its expected value during a scheduled load-shifting event.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``SOO_[mode]:###`` — Variable that must equal ``###`` during ``[mode]``
      load-shifting events.  ``[mode]`` can be ``normal``, ``loadUp``,
      ``shed``, ``criticalPeak``, ``gridEmergency``, or ``advLoadUp``.
      For example, ``SOO_loadUp:1`` means the variable should be 1 during
      loadUp events.

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame with a datetime index.
    config : ConfigManager
        Pipeline configuration object used to locate ``Variable_Names.csv``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    Detect temperatures outside an acceptable range.

    Triggers an alarm when a temperature variable stays above the high bound or
    below the low bound for ``fault_time`` consecutive minutes.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``TMPRNG_[OPTIONAL ID]:###-###`` — Temperature variable with acceptable
      range.  The ``###-###`` portion defines the low and high bounds
      (e.g., ``TMPRNG:110-130`` means temperature must stay between 110 and
      130 degrees).

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.  Used to determine which
        days to process.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_high_temp : float, optional
        Default upper temperature bound in degrees F when no custom range is
        provided in the alarm code (default 130).  Temperature above this value
        triggers an alarm.
    default_low_temp : float, optional
        Default lower temperature bound in degrees F when no custom range is
        provided in the alarm code (default 115).  Temperature below this value
        triggers an alarm.
    fault_time : int, optional
        Consecutive minutes temperature must be outside the acceptable range
        before an alarm is triggered (default 10).

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    Detect heat pump short-cycling events.

    Identifies when the heat pump turns on for fewer than ``short_cycle_time``
    consecutive minutes before turning off again.  Short cycling can indicate
    equipment issues or improper system sizing.

    VarNames syntax (``alarm_codes`` column in ``Variable_Names.csv``):

    - ``SHRTCYC_[OPTIONAL ID]:###`` — Heat pump power variable.  ``###`` is
      the power threshold (default ``default_power_threshold``) above which
      the heat pump is considered *on*.

    Parameters
    ----------
    df : pd.DataFrame
        Post-transformed minute-level data DataFrame.  Must contain consecutive,
        in-order minutes; out-of-order rows or gaps may produce erroneous alarms.
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    default_power_threshold : float, optional
        Default power threshold in kW when no custom bound is provided in the
        alarm code (default 1.0).  Heat pump is considered *on* when power
        exceeds this value.
    short_cycle_time : int, optional
        Minimum expected run time in minutes (default 15).  An alarm triggers
        when the heat pump runs for fewer than this many consecutive minutes
        before turning off.

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if ``df`` is
        empty or ``Variable_Names.csv`` cannot be found.
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
    """Convert a silent-alarm dictionary to a standardized alarm event DataFrame."""
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
    Detect power-ratio anomalies using daily energy data.

    Reads power-ratio alarm configuration from ``Variable_Names.csv`` via
    ``config`` and triggers alarms when the ratio of one power variable to
    another falls outside the defined bounds over a rolling window.

    Parameters
    ----------
    daily_df : pd.DataFrame
        Post-transformed daily-level data DataFrame.  Must contain consecutive,
        in-order days; out-of-order rows or gaps may produce erroneous alarms.
    config : ConfigManager
        Pipeline configuration object.  Points to a ``Variable_Names.csv`` file
        that must contain at least the columns ``variable_name`` and
        ``alarm_codes``.  Ratio alarm codes take the form
        ``PR_{Power Ratio Name}:{low percentage}-{high percentage}``.
    day_table_name : str
        Name of the daily database table used to fetch historical data for the
        rolling-period calculation.
    system : str, optional
        Name of the system to filter on when ``Variable_Names.csv`` contains
        multiple systems.  Pass an empty string (default) if not applicable.
    verbose : bool, optional
        If ``True``, emit additional print statements during processing
        (default ``False``).
    ratio_period_days : int, optional
        Number of days in the rolling power-ratio window (default 7).

    Returns
    -------
    pd.DataFrame
        DataFrame of alarm events.  Returns an empty DataFrame if no alarms
        are triggered or ``Variable_Names.csv`` cannot be found.
    """
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return pd.DataFrame()
    alarm = PowerRatio(bounds_df, day_table_name, ratio_period_days)
    return alarm.find_alarms(None, daily_df, config)
