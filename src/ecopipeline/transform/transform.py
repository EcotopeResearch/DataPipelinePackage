import pandas as pd
import numpy as np
import datetime as dt
import pickle
import os
from ecopipeline.utils.unit_convert import temp_c_to_f_non_noaa, volume_l_to_g, power_btuhr_to_kw, temp_f_to_c
from ecopipeline import ConfigManager

pd.set_option('display.max_columns', None)


def central_transform_function(config : ConfigManager, df : pd.DataFrame, weather_df : pd.DataFrame = None, tz_convert_from: str = 'America/Los_Angeles',
                               tz_convert_to: str = 'America/Los_Angeles', oat_column_name : str = "Temp_OAT",
                               complete_hour_threshold : float = 0.8, complete_day_threshold : float = 1.0, remove_partial : bool = True,
                               pre_aggregation_func=None, post_aggregation_func=None) -> [pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Run the full central transform pipeline on raw minute-level site data.

    Renames sensors, rounds timestamps, forward-fills missing values, optionally
    converts timezones, averages duplicate timestamps, aggregates to hourly and
    daily dataframes, and optionally merges weather data. Supports optional
    pre- and post-aggregation hooks for custom processing.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
    df : pd.DataFrame
        Dataframe with raw time-indexed (ideally minute-interval) site data.
        Important column names should be represented in the ``variable_alias``
        column in the Variable_Names.csv file.
    weather_df : pd.DataFrame, optional
        Dataframe with time-indexed (preferably hourly) weather data. Will be
        merged with the hourly dataframe.
    tz_convert_from : str, optional
        String value of the timezone the data is currently in.
    tz_convert_to : str, optional
        String value of the timezone the data should be converted to.
    oat_column_name : str, optional
        Name that the Outdoor Air Temperature column should have. Defaults to
        ``'Temp_OAT'``.
    complete_hour_threshold : float, optional
        Percent of minutes in an hour needed to count as a complete hour,
        expressed as a float (e.g. 80% = 0.8). Defaults to 0.8. Only
        applicable if ``remove_partial`` is ``True``.
    complete_day_threshold : float, optional
        Percent of hours in a day needed to count as a complete day, expressed
        as a float (e.g. 80% = 0.8). Defaults to 1.0. Only applicable if
        ``remove_partial`` is ``True``.
    remove_partial : bool, optional
        If ``True``, removes partial days and hours from aggregated dataframes.
        Defaults to ``True``.
    pre_aggregation_func : callable, optional
        A custom function called after minute-level processing and before
        aggregation. Signature:
        ``pre_aggregation_func(df: pd.DataFrame) -> pd.DataFrame``.
    post_aggregation_func : callable, optional
        A custom function called after weather merging and before returning.
        Signature:
        ``post_aggregation_func(df, hourly_df, daily_df) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]``.

    Returns
    -------
    tuple of pd.DataFrame
        A three-element tuple ``(df, hourly_df, daily_df)`` containing the
        processed minute-level, hourly, and daily dataframes respectively.

    Raises
    ------
    TypeError
        If ``pre_aggregation_func`` or ``post_aggregation_func`` is not
        callable, does not accept the expected parameters, or does not return
        the expected type.
    """
    print("++++++++++++ TRANSFORM ++++++++++++")
    df = rename_sensors(df, config)
    round_time(df)
    df = ffill_missing(df, config)
    if not tz_convert_to == tz_convert_from:
        df = convert_time_zone(df, tz_convert_from, tz_convert_to)
    df = avg_duplicate_times(df, None)

    if pre_aggregation_func is not None:
        if not callable(pre_aggregation_func):
            raise TypeError("pre_aggregation_func must be a callable that accepts (df: pd.DataFrame) and returns a pd.DataFrame.")
        try:
            result = pre_aggregation_func(df)
        except TypeError as e:
            raise TypeError(f"pre_aggregation_func failed — ensure it accepts exactly one parameter (df: pd.DataFrame). Original error: {e}")
        if not isinstance(result, pd.DataFrame):
            raise TypeError(f"pre_aggregation_func must return a pd.DataFrame, but returned {type(result).__name__}.")
        df = result

    hourly_df, daily_df = aggregate_df(df, config.get_ls_filename(), complete_hour_threshold, complete_day_threshold, remove_partial)
    # process ls
    # df, hourly_df, daily_df = process_ls_signal(df, hourly_df, daily_df)

    if not weather_df is None and not weather_df.empty:
        weather_df = weather_df.rename(columns = {'airTemp_F':oat_column_name})
        weather_df.index = weather_df.index.tz_localize(None)
        hourly_df = join_to_hourly(hourly_df, weather_df, oat_column_name)

        if len(daily_df.index) > 0 and oat_column_name in hourly_df.columns:
            daily_df[oat_column_name] = hourly_df[oat_column_name].resample('D').mean(numeric_only=True)

    if post_aggregation_func is not None:
        if not callable(post_aggregation_func):
            raise TypeError("post_aggregation_func must be a callable that accepts (df: pd.DataFrame, hourly_df: pd.DataFrame, daily_df: pd.DataFrame) and returns a tuple of three pd.DataFrames.")
        try:
            result = post_aggregation_func(df, hourly_df, daily_df)
        except TypeError as e:
            raise TypeError(f"post_aggregation_func failed — ensure it accepts exactly three parameters (df, hourly_df, daily_df: pd.DataFrame). Original error: {e}")
        if (not isinstance(result, tuple) or len(result) != 3
                or not all(isinstance(r, pd.DataFrame) for r in result)):
            raise TypeError(f"post_aggregation_func must return a tuple of three pd.DataFrames (df, hourly_df, daily_df), but returned {type(result).__name__}.")
        df, hourly_df, daily_df = result

    return df, hourly_df, daily_df

def concat_last_row(df: pd.DataFrame, last_row: pd.DataFrame) -> pd.DataFrame:
    """
    Concatenate the last database row onto a new-data dataframe to enable forward filling.

    Takes a dataframe with new data and a second dataframe representing the last
    row of the destination database, concatenates them so that subsequent forward
    filling can use information from the last row.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with new data that needs to be forward filled from data in the
        last row of a database.
    last_row : pd.DataFrame
        Last row of the database to forward fill from.

    Returns
    -------
    pd.DataFrame
        Dataframe with the last row concatenated and sorted by index.
    """
    df = pd.concat([last_row, df], join="inner")
    df = df.sort_index()
    return df


def round_time(df: pd.DataFrame):
    """
    Round a dataframe's DatetimeIndex down to the nearest minute, in place.

    Parameters
    ----------
    df : pd.DataFrame
        A dataframe indexed by datetimes. All timestamps will be floored to the
        nearest minute.

    Returns
    -------
    bool
        ``True`` if the index has been rounded down, ``False`` if the operation
        failed (e.g. if ``df`` was empty).
    """
    if (df.empty):
        return False
    if not df.index.tz is None:
        tz = df.index.tz
        df.index = df.index.tz_localize(None)
        df.index = df.index.floor('T')
        df.index = df.index.tz_localize(tz, ambiguous='infer')
    else:
        df.index = df.index.floor('T')
    return True


def rename_sensors(original_df: pd.DataFrame, config : ConfigManager, site: str = "", system: str = ""):
    """
    Rename sensor columns from their raw aliases to their true names.

    Reads the Variable_Names.csv file via ``config``, renames columns from
    ``variable_alias`` to ``variable_name``, drops columns with no matching
    true name, and optionally filters by site and/or system.

    Parameters
    ----------
    original_df : pd.DataFrame
        A dataframe containing data labeled by raw variable names to be renamed.
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
        Points to a file called Variable_Names.csv in the pipeline's input folder.
        The CSV must have at least two columns: ``variable_alias`` (the raw name
        to change from) and ``variable_name`` (the name to change to). Columns
        without a corresponding ``variable_name`` are dropped.
    site : str, optional
        Site name to filter by. If provided, only rows whose ``site`` column
        matches this value are retained. Leave as an empty string if not
        applicable.
    system : str, optional
        System name to filter by. If provided, only rows whose ``system`` column
        contains this string are retained. Leave as an empty string if not
        applicable.

    Returns
    -------
    pd.DataFrame
        Dataframe filtered by site and system (if applicable) with column names
        matching those specified in Variable_Names.csv.

    Raises
    ------
    Exception
        If the Variable_Names.csv file is not found at the path provided by
        ``config``.
    """
    variable_names_path = config.get_var_names_path()
    try:
        variable_data = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        raise Exception("File Not Found: "+ variable_names_path)
    
    if (site != ""):
        variable_data = variable_data.loc[variable_data['site'] == site]
    if (system != ""):
        variable_data = variable_data.loc[variable_data['system'].str.contains(system, na=False)]

    variable_data = variable_data.loc[:, ['variable_alias', 'variable_name']]
    variable_data.dropna(axis=0, inplace=True)
    variable_alias = list(variable_data["variable_alias"])
    variable_true = list(variable_data["variable_name"])
    variable_alias_true_dict = dict(zip(variable_alias, variable_true))
    # Create a copy of the original DataFrame
    df = original_df.copy()

    df.rename(columns=variable_alias_true_dict, inplace=True)

    # drop columns that do not have a corresponding true name
    df.drop(columns=[col for col in df if col in variable_alias and col not in variable_true], inplace=True)

    # drop columns that are not documented in variable names csv file at all
    df.drop(columns=[col for col in df if col not in variable_true], inplace=True)
    #drop null columns
    df = df.dropna(how='all')

    return df

def avg_duplicate_times(df: pd.DataFrame, timezone : str) -> pd.DataFrame:
    """
    Collapse duplicate timestamps by averaging numeric values and taking the first non-numeric value.

    Looks for duplicate timestamps (typically caused by daylight-saving time
    transitions or timestamp rounding) and reduces each group of duplicates to a
    single row, averaging numeric columns and keeping the first value for
    non-numeric columns.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe to be altered.
    timezone : str
        Timezone string to apply to the output index. Must be a string
        recognised by ``pandas.Series.tz_localize``. See
        https://pandas.pydata.org/docs/reference/api/pandas.Series.tz_localize.html.

    Returns
    -------
    pd.DataFrame
        Dataframe with all duplicate timestamps collapsed into one row,
        averaging numeric data values.
    """
    df.index = pd.DatetimeIndex(df.index).tz_localize(None)
    # get rid of time stamp 0 values
    if df.index.min() < pd.Timestamp('2000-01-01'):
        df = df[df.index > pd.Timestamp('2000-01-01')]

    # Get columns with non-numeric values
    non_numeric_cols = df.select_dtypes(exclude='number').columns

    # Group by index, taking only the first value in case of duplicates
    non_numeric_df = df.groupby(df.index)[non_numeric_cols].first()

    numeric_df = df.groupby(df.index).mean(numeric_only = True)
    df = pd.concat([non_numeric_df, numeric_df], axis=1)
    df.index = (df.index).tz_localize(timezone)
    return df

def _rm_cols(col, bounds_df):  # Helper function for remove_outliers
    """
    Set values outside the specified bounds to NaN for a single pandas Series.

    Parameters
    ----------
    col : pd.Series
        Pandas Series (dataframe column) from the data being processed.
    bounds_df : pd.DataFrame
        Dataframe indexed by column names from the parent dataframe. Must
        contain at least two columns: ``lower_bound`` and ``upper_bound``.
    """
    if (col.name in bounds_df.index):
        c_lower = bounds_df.loc[col.name]["lower_bound"]
        c_upper = bounds_df.loc[col.name]["upper_bound"]

        # Skip if both bounds are NaN
        if pd.isna(c_lower) and pd.isna(c_upper):
            return

        # Convert bounds to float, handling NaN values
        c_lower = float(c_lower) if not pd.isna(c_lower) else -np.inf
        c_upper = float(c_upper) if not pd.isna(c_upper) else np.inf

        col.mask((col > c_upper) | (col < c_lower), other=np.NaN, inplace=True)

# TODO: remove_outliers STRETCH GOAL: Functionality for alarms being raised based on bounds needs to happen here.
def remove_outliers(original_df: pd.DataFrame, config : ConfigManager, site: str = "") -> pd.DataFrame:
    """
    Remove outliers from a dataframe by replacing out-of-bounds values with NaN.

    Reads bound information from Variable_Names.csv via ``config`` and sets any
    values outside the defined ``lower_bound``/``upper_bound`` range to NaN.

    Parameters
    ----------
    original_df : pd.DataFrame
        Pandas dataframe for which outliers need to be removed.
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
        Points to a file called Variable_Names.csv in the pipeline's input folder.
        The CSV must have at least three columns: ``variable_name``,
        ``lower_bound``, and ``upper_bound``.
    site : str, optional
        Site name to filter bounds data by. Leave as an empty string if not
        applicable.

    Returns
    -------
    pd.DataFrame
        Dataframe with outliers replaced by NaN.
    """
    df = original_df.copy()
    variable_names_path = config.get_var_names_path()
    try:
        bounds_df = pd.read_csv(variable_names_path)
    except FileNotFoundError:
        print("File Not Found: ", variable_names_path)
        return df

    if (site != ""):
        bounds_df = bounds_df.loc[bounds_df['site'] == site]

    bounds_df = bounds_df.loc[:, [
        "variable_name", "lower_bound", "upper_bound"]]
    bounds_df.dropna(axis=0, thresh=2, inplace=True)
    bounds_df.set_index(['variable_name'], inplace=True)
    bounds_df = bounds_df[bounds_df.index.notnull()]

    df.apply(_rm_cols, args=(bounds_df,))
    return df


def _ffill(col, ffill_df, previous_fill: pd.DataFrame = None):  # Helper function for ffill_missing
    """
    Forward-fill a single pandas Series according to per-column rules in ``ffill_df``.

    Parameters
    ----------
    col : pd.Series
        Pandas Series to forward-fill.
    ffill_df : pd.DataFrame
        Dataframe indexed by variable name containing ``changepoint`` and
        ``ffill_length`` columns that control fill behaviour.
    previous_fill : pd.DataFrame, optional
        Dataframe used to seed the initial fill value for the first row of
        ``col`` when it is NaN.
    """
    if (col.name in ffill_df.index):
        #set initial fill value where needed for first row
        if previous_fill is not None and len(col) > 0 and pd.isna(col.iloc[0]):
            col.iloc[0] = previous_fill[col.name].iloc[0]
        cp = ffill_df.loc[col.name]["changepoint"]
        length = ffill_df.loc[col.name]["ffill_length"]
        if (length != length):  # check for nan, set to 0
            length = 0
        length = int(length)  # casting to int to avoid float errors
        if (cp == 1):  # ffill unconditionally
            col.fillna(method='ffill', inplace=True)
        elif (cp == 0):  # ffill only up to length
            col.fillna(method='ffill', inplace=True, limit=length)

def ffill_missing(original_df: pd.DataFrame, config : ConfigManager, previous_fill: pd.DataFrame = None) -> pd.DataFrame:
    """
    Forward-fill selected columns of a dataframe according to rules in Variable_Names.csv.

    Parameters
    ----------
    original_df : pd.DataFrame
        Pandas dataframe that needs to be forward-filled.
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
        Points to a file called Variable_Names.csv in the pipeline's input folder.
        The CSV must have at least three columns:

        - ``variable_name``: name of each variable to forward-fill.
        - ``changepoint``: ``1`` to forward-fill unconditionally until the next
          change point, ``0`` to forward-fill up to ``ffill_length`` rows, or
          null to skip forward-filling for that variable.
        - ``ffill_length``: number of rows to forward-fill when ``changepoint``
          is ``0``.
    previous_fill : pd.DataFrame, optional
        Dataframe with the same index type and at least some of the same columns
        as ``original_df`` (typically the last row from the destination database).
        Its values are used to seed forward-filling into the new data.

    Returns
    -------
    pd.DataFrame
        Dataframe that has been forward-filled per the specifications in the
        Variable_Names.csv file.
    """
    df = original_df.copy()
    df = df.sort_index()
    vars_filename = config.get_var_names_path()
    try:
        # ffill dataframe holds ffill length and changepoint bool
        ffill_df = pd.read_csv(vars_filename)
    except FileNotFoundError:
        print("File Not Found: ", vars_filename)
        return df

    ffill_df = ffill_df.loc[:, [
        "variable_name", "changepoint", "ffill_length"]]
    # drop data without changepoint AND ffill_length
    ffill_df.dropna(axis=0, thresh=2, inplace=True)
    ffill_df.set_index(['variable_name'], inplace=True)
    ffill_df = ffill_df[ffill_df.index.notnull()]  # drop data without names

    # add any columns in previous_fill that are missing from df and fill with nans
    if previous_fill is not None:
       # Get column names of df and previous_fill
        a_cols = set(df.columns)
        b_cols = set(previous_fill.columns)
        b_cols.discard('time_pt') # avoid duplicate column bug

        # Find missing columns in df and add them with NaN values
        missing_cols = list(b_cols - a_cols)
        if missing_cols:
            for col in missing_cols:
                df[col] = np.nan 

    df.apply(_ffill, args=(ffill_df,previous_fill))
    return df

def convert_temp_resistance_type(df : pd.DataFrame, column_name : str, sensor_model = 'veris') -> pd.DataFrame:
    """
    Convert temperature resistance readings using a 10k Type 2 thermistor model.

    Applies a two-stage pickle-model conversion (temperature-to-resistance, then
    resistance-to-temperature) to correct sensor readings in the specified column.

    Parameters
    ----------
    df : pd.DataFrame
        Timestamp-indexed Pandas dataframe of minute-by-minute values.
    column_name : str
        Name of the column containing resistance conversion Type 2 data.
    sensor_model : str, optional
        Sensor model to use. Supported values: ``'veris'``, ``'tasseron'``.
        Defaults to ``'veris'``.

    Returns
    -------
    pd.DataFrame
        Dataframe with the specified column corrected via the thermistor model.

    Raises
    ------
    Exception
        If ``sensor_model`` is not a supported value.
    """
    model_path_t_to_r = '../utils/pkls/'
    model_path_r_to_t = '../utils/pkls/'
    if sensor_model == 'veris':
        model_path_t_to_r = model_path_t_to_r + 'veris_temp_to_resistance_2.pkl'
        model_path_r_to_t = model_path_r_to_t + 'veris_resistance_to_temp_3.pkl'
    elif sensor_model == 'tasseron':
        model_path_t_to_r = model_path_t_to_r + 'tasseron_temp_to_resistance_2.pkl'
        model_path_r_to_t = model_path_r_to_t + 'tasseron_resistance_to_temp_3.pkl'
    else:
        raise Exception("unsupported sensor model")
    
    with open(os.path.join(os.path.dirname(__file__),model_path_t_to_r), 'rb') as f:
        model = pickle.load(f)
    df['resistance'] = df[column_name].apply(model)
    with open(os.path.join(os.path.dirname(__file__),model_path_r_to_t), 'rb') as f:
        model = pickle.load(f)
    df[column_name] = df['resistance'].apply(model)
    df.drop(columns='resistance')
    return df

def estimate_power(df : pd.DataFrame, new_power_column : str, current_a_column : str, current_b_column : str, current_c_column : str,
                 assumed_voltage : float = 208, power_factor : float = 1) -> pd.DataFrame:
    """
    Estimate three-phase power from per-phase current readings.

    Calculates power as the average phase current multiplied by the assumed
    voltage, power factor, and sqrt(3), then converts from watts to kilowatts.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe with minute-to-minute data.
    new_power_column : str
        Column name to store the estimated power. Units will be kW.
    current_a_column : str
        Column name of the Phase A current. Units should be amps.
    current_b_column : str
        Column name of the Phase B current. Units should be amps.
    current_c_column : str
        Column name of the Phase C current. Units should be amps.
    assumed_voltage : float, optional
        Assumed line voltage in volts. Defaults to 208.
    power_factor : float, optional
        Power factor to apply. Defaults to 1.

    Returns
    -------
    pd.DataFrame
        Dataframe with a new estimated power column of the specified name.
    """
    #average current * 208V * PF * sqrt(3)
    df[new_power_column] = (df[current_a_column] + df[current_b_column] + df[current_c_column]) / 3 * assumed_voltage * power_factor * np.sqrt(3) / 1000

    return df

def process_ls_signal(df: pd.DataFrame, hourly_df: pd.DataFrame, daily_df: pd.DataFrame, load_dict: dict = {1: "normal", 2: "loadUp", 3 : "shed"}, ls_column: str = 'ls',
                      drop_ls_from_df : bool = False):
    """
    Add load-shift signals to hourly and daily aggregated dataframes.

    Parameters
    ----------
    df : pd.DataFrame
        Timestamp-indexed Pandas dataframe of minute-by-minute values.
    hourly_df : pd.DataFrame
        Timestamp-indexed Pandas dataframe of hourly average values.
    daily_df : pd.DataFrame
        Timestamp-indexed Pandas dataframe of daily average values.
    load_dict : dict, optional
        Mapping from integer load-shift signal values to descriptive string
        labels. Defaults to ``{1: "normal", 2: "loadUp", 3: "shed"}``.
    ls_column : str, optional
        Name of the load-shift column in ``df``. Defaults to ``'ls'``.
    drop_ls_from_df : bool, optional
        If ``True``, drops ``ls_column`` from ``df`` after processing.
        Defaults to ``False``.

    Returns
    -------
    df : pd.DataFrame
        Minute-by-minute dataframe with ``ls_column`` removed if
        ``drop_ls_from_df`` is ``True``.
    hourly_df : pd.DataFrame
        Hourly dataframe with an added ``'system_state'`` column containing
        the load-shift command label from ``load_dict`` for each hour. Values
        are mapped from the rounded mean of ``ls_column`` within each hour;
        hours whose rounded mean is not a key in ``load_dict`` will be null.
    daily_df : pd.DataFrame
        Daily dataframe with an added boolean ``'load_shift_day'`` column that
        is ``True`` on days containing at least one non-normal load-shift
        command in ``hourly_df``.
    """
    # Make copies to avoid modifying original dataframes
    df_copy = df.copy()

    if ls_column in df_copy.columns:
        # print("1",df_copy[np.isfinite(df_copy[ls_column])])
        df_copy = df_copy[df_copy[ls_column].notna() & np.isfinite(df_copy[ls_column])]
        # print("2",df_copy[np.isfinite(df_copy[ls_column])])

    # Process hourly data - aggregate ls_column values by hour and map to system_state
    if ls_column in df_copy.columns:
        # Group by hour and calculate mean of ls_column, then round to nearest integer
        hourly_ls = df_copy[ls_column].resample('H').mean().round()
        
        # Convert to int only for non-NaN values
        hourly_ls = hourly_ls.apply(lambda x: int(x) if pd.notna(x) else x)
        
        # Map the rounded integer values to load_dict, using None for unmapped values
        hourly_df['system_state'] = hourly_ls.map(load_dict)
        
        # For hours not present in the minute data, system_state will be NaN
        hourly_df['system_state'] = hourly_df['system_state'].where(
            hourly_df.index.isin(hourly_ls.index)
        )
    else:
        # If ls_column doesn't exist, set all system_state to None
        hourly_df['system_state'] = None
    
    # Process daily data - determine if any non-normal loadshift commands occurred
    if 'system_state' in hourly_df.columns:
        # Group by date and check if any non-"normal" and non-null system_state exists
        daily_ls = hourly_df.groupby(hourly_df.index.date)['system_state'].apply(
            lambda x: any((state != "normal") and (state is not None) for state in x.dropna())
        )
        
        # Map the daily boolean results to the daily_df index
        daily_df['load_shift_day'] = daily_df.index.date
        daily_df['load_shift_day'] = daily_df['load_shift_day'].map(daily_ls).fillna(False)
    else:
        # If no system_state column, set all days to False
        daily_df['load_shift_day'] = False
    
    # Drop ls_column from df if requested
    if drop_ls_from_df and ls_column in df.columns:
        df = df.drop(columns=[ls_column])
    
    return df, hourly_df, daily_df

def delete_erroneous_from_time_pt(df: pd.DataFrame, time_point : pd.Timestamp, column_names : list, new_value = None) -> pd.DataFrame:
    """
    Replace erroneous values at a specific timestamp with a given replacement value.

    Parameters
    ----------
    df : pd.DataFrame
        Timestamp-indexed Pandas dataframe that contains the erroneous value.
    time_point : pd.Timestamp
        The index timestamp at which the erroneous values occur.
    column_names : list
        List of column name strings that contain erroneous values at this
        timestamp.
    new_value : any, optional
        Replacement value to write into the erroneous cells. If ``None``,
        the cells are replaced with NaN. Defaults to ``None``.

    Returns
    -------
    pd.DataFrame
        Dataframe with the erroneous values replaced by ``new_value``.
    """
    if new_value is None:
        new_value = float('NaN')  # Replace with NaN if new_value is not provided
    
    if time_point in df.index:
        for col in column_names:
            df.loc[time_point, col] = new_value

    return df

# TODO test this
def nullify_erroneous(original_df: pd.DataFrame, config : ConfigManager) -> pd.DataFrame:
    """
    Replace known error-sentinel values in a dataframe with NaN.

    Parameters
    ----------
    original_df : pd.DataFrame
        Pandas dataframe that needs to be filtered for error values.
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
        Points to a file called Variable_Names.csv in the pipeline's input folder.
        The CSV must have at least two columns:

        - ``variable_name``: names of columns that may contain error values.
        - ``error_value``: the sentinel error value for each variable, or null
          if no error value applies.

    Returns
    -------
    pd.DataFrame
        Dataframe with known error-sentinel values replaced by NaN.
    """
    df = original_df.copy()
    vars_filename = config.get_var_names_path()
    try:
        # ffill dataframe holds ffill length and changepoint bool
        error_df = pd.read_csv(vars_filename)
    except FileNotFoundError:
        print("File Not Found: ", vars_filename)
        return df

    error_df = error_df.loc[:, [
        "variable_name", "error_value"]]
    # drop data without changepoint AND ffill_length
    error_df.dropna(axis=0, thresh=2, inplace=True)
    error_df.set_index(['variable_name'], inplace=True)
    error_df = error_df[error_df.index.notnull()]  # drop data without names
    for col in error_df.index:
        if col in df.columns:
            error_value = error_df.loc[col, 'error_value']
            df.loc[df[col] == error_value, col] = np.nan

    return df

def column_name_change(df : pd.DataFrame, dt : pd.Timestamp, new_column : str, old_column : str, remove_old_column : bool = True) -> pd.DataFrame:
    """
    Back-fill ``new_column`` with values from ``old_column`` for rows before a name-change timestamp.

    Overwrites values in ``new_column`` with values from ``old_column`` for all
    rows with an index earlier than ``dt``, provided ``dt`` is within the index
    range. Optionally removes ``old_column`` afterwards.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe with minute-to-minute data.
    dt : pd.Timestamp
        Timestamp of the variable name change.
    new_column : str
        Name of the column to be overwritten for rows before ``dt``.
    old_column : str
        Name of the column to copy values from.
    remove_old_column : bool, optional
        If ``True``, drops ``old_column`` from the dataframe after the copy.
        Defaults to ``True``.

    Returns
    -------
    pd.DataFrame
        Dataframe with ``new_column`` updated for pre-change rows.
    """
    if old_column in df.columns:
        if df.index.min() < dt:
            mask = df.index < dt
            df.loc[mask, new_column] = df.loc[mask, old_column]
        if remove_old_column:
            df = df.drop(columns=[old_column])
    return df

def heat_output_calc(df: pd.DataFrame, flow_var : str, hot_temp : str, cold_temp : str, heat_out_col_name : str, return_as_kw : bool = True) -> pd.DataFrame:
    """
    Calculate heat output from flow rate and supply/return temperatures.

    Uses the formula ``Heat (BTU/hr) = 500 * flow (gal/min) * delta_T (°F)``
    and clips negative values to zero. Optionally converts the result to kW.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe with minute-to-minute data.
    flow_var : str
        Column name of the flow variable. Units must be gal/min.
    hot_temp : str
        Column name of the hot (supply) temperature variable. Units must be °F.
    cold_temp : str
        Column name of the cold (return) temperature variable. Units must be °F.
    heat_out_col_name : str
        Name for the new heat output column added to the dataframe.
    return_as_kw : bool, optional
        If ``True``, the new column will be in kW. If ``False``, it will be in
        BTU/hr. Defaults to ``True``.

    Returns
    -------
    pd.DataFrame
        Dataframe with the new heat output column of the specified name.
    """
    df[heat_out_col_name] = 500 * df[flow_var] * (df[hot_temp] - df[cold_temp]) #BTU/hr
    df[heat_out_col_name] = np.where(df[heat_out_col_name] > 0, df[heat_out_col_name], 0)
    if return_as_kw:
        df[heat_out_col_name] = df[heat_out_col_name]/3412 # convert to kW
    return df

#TODO investigate if this can be removed
def sensor_adjustment(df: pd.DataFrame, config : ConfigManager) -> pd.DataFrame:
    """
    Apply sensor adjustments from adjustments.csv to the dataframe.

    .. deprecated::
        This function is scheduled for removal. Use a more explicit adjustment
        approach instead.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to be adjusted.
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
        Points to a file called ``adjustments.csv`` in the pipeline's input
        folder (e.g. ``"full/path/to/pipeline/input/adjustments.csv"``).

    Returns
    -------
    pd.DataFrame
        Adjusted dataframe.
    """
    adjustments_csv_path = f"{config.input_directory}adjustments.csv"
    try:
        adjustments = pd.read_csv(adjustments_csv_path)
    except FileNotFoundError:
        print(f"File Not Found: {adjustments_csv_path}")
        return df
    if adjustments.empty:
        return df

    adjustments["datetime_applied"] = pd.to_datetime(
        adjustments["datetime_applied"])
    df = df.sort_values(by="datetime_applied")

    for adjustment in adjustments:
        adjustment_datetime = adjustment["datetime_applied"]
        # NOTE: To access time, df.index (this returns a list of DateTime objects in a full df)
        # To access time object if you have located a series, it's series.name (ex: df.iloc[0].name -- this prints the DateTime for the first row in a df)
        df_pre = df.loc[df.index < adjustment_datetime]
        df_post = df.loc[df.index >= adjustment_datetime]
        match adjustment["adjustment_type"]:
            case "add":
                continue
            case "remove":
                df_post[adjustment["sensor_1"]] = np.nan
            case "swap":
                df_post[[adjustment["sensor_1"], adjustment["sensor_2"]]] = df_post[[
                    adjustment["sensor_2"], adjustment["sensor_1"]]]
        df = pd.concat([df_pre, df_post], ignore_index=True)

    return df

def add_relative_humidity(df : pd.DataFrame, temp_col : str ='airTemp_F', dew_point_col : str ='dewPoint_F', degree_f : bool = True):
    """
    Add a ``'relative_humidity'`` column to the dataframe.

    Computes relative humidity from air temperature and dew-point temperature
    using the August-Roche-Magnus approximation. Clips the result to [0, 100].

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing air temperature and dew-point temperature columns.
    temp_col : str, optional
        Column name for air temperature. Defaults to ``'airTemp_F'``.
    dew_point_col : str, optional
        Column name for dew-point temperature. Defaults to ``'dewPoint_F'``.
    degree_f : bool, optional
        If ``True``, temperature columns are assumed to be in °F and are
        internally converted to °C for the calculation. If ``False``, columns
        are assumed to already be in °C. Defaults to ``True``.

    Returns
    -------
    pd.DataFrame
        Dataframe with an added ``'relative_humidity'`` column (percent, 0–100).
    """
    # Define constants
    A = 6.11
    B = 7.5
    C = 237.3
    try:
        if degree_f:
            df[f"{temp_col}_C"] = df[temp_col].apply(temp_f_to_c)
            df[f"{dew_point_col}_C"] = df[dew_point_col].apply(temp_f_to_c)
            temp_col_c = f"{temp_col}_C"
            dew_point_col_c = f"{dew_point_col}_C"
        else:
            temp_col_c = temp_col
            dew_point_col_c = dew_point_col

        # Calculate saturation vapor pressure (e_s) and actual vapor pressure (e)
        e_s = A * 10 ** ((B * df[temp_col_c]) / (df[temp_col_c] + C))
        e = A * 10 ** ((B * df[dew_point_col_c]) / (df[dew_point_col_c] + C))

        # Calculate relative humidity
        df['relative_humidity'] = (e / e_s) * 100.0

        # Handle cases where relative humidity exceeds 100% due to rounding
        df['relative_humidity'] = np.clip(df['relative_humidity'], 0.0, 100.0)

        if degree_f:
            df.drop(columns=[temp_col_c, dew_point_col_c])
    except:
       
        df['relative_humidity'] = None
        print("Unable to calculate relative humidity data for timeframe")

    return df

def cop_method_1(df: pd.DataFrame, recircLosses, heatout_primary_column : str = 'HeatOut_Primary', total_input_power_column : str = 'PowerIn_Total') -> pd.DataFrame:
    """
    Perform COP calculation method 1 (original AWS method).

    Computes ``COP_DHWSys_1 = (HeatOut_Primary + recircLosses) / PowerIn_Total``
    and adds the result as a new column to the dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of daily averaged values. Must already contain
        ``heatout_primary_column`` and ``total_input_power_column``.
    recircLosses : float or pd.Series
        Recirculation losses in kW. Pass a ``float`` for a fixed spot-measured
        value, or a ``pd.Series`` (aligned with ``df``) if measurements are
        available in the datastream.
    heatout_primary_column : str, optional
        Name of the column containing primary system output power in kW.
        Defaults to ``'HeatOut_Primary'``.
    total_input_power_column : str, optional
        Name of the column containing total system input power in kW.
        Defaults to ``'PowerIn_Total'``.

    Returns
    -------
    pd.DataFrame
        Dataframe with an added ``'COP_DHWSys_1'`` column.
    """
    columns_to_check = [heatout_primary_column, total_input_power_column]

    missing_columns = [col for col in columns_to_check if col not in df.columns]

    if missing_columns:
        print('Cannot calculate COP as the following columns are missing from the DataFrame:', missing_columns)
        return df
    
    df['COP_DHWSys_1'] = (df[heatout_primary_column] + recircLosses) / df[total_input_power_column]
    
    return df

def cop_method_2(df: pd.DataFrame, cop_tm, cop_primary_column_name) -> pd.DataFrame:
    """
    Perform COP calculation method 2.

    Formula:
    ``COP = COP_primary * (ELEC_primary / ELEC_total) + COP_tm * (ELEC_tm / ELEC_total)``

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe to add the COP column to. Must contain:

        - ``cop_primary_column_name``: primary system COP values.
        - ``'PowerIn_Total'``: total system power.
        - Columns prefixed with ``'PowerIn_HPWH'`` or equal to
          ``'PowerIn_SecLoopPump'`` (primary system power).
        - Columns prefixed with ``'PowerIn_SwingTank'`` or
          ``'PowerIn_ERTank'`` (temperature-maintenance system power).
    cop_tm : float
        Fixed COP value for the temperature-maintenance system.
    cop_primary_column_name : str
        Name of the column containing primary-system COP values.

    Returns
    -------
    pd.DataFrame
        Dataframe with an added ``'COP_DHWSys_2'`` column.
    """
    columns_to_check = [cop_primary_column_name, 'PowerIn_Total']

    missing_columns = [col for col in columns_to_check if col not in df.columns]

    if missing_columns:
        print('Cannot calculate COP as the following columns are missing from the DataFrame:', missing_columns)
        return df
    
    # Create list of column names to sum
    sum_primary_cols = [col for col in df.columns if col.startswith('PowerIn_HPWH') or col == 'PowerIn_SecLoopPump']
    sum_tm_cols = [col for col in df.columns if col.startswith('PowerIn_SwingTank') or col.startswith('PowerIn_ERTank')]

    if len(sum_primary_cols) == 0:
        print('Cannot calculate COP as the primary power columns (such as PowerIn_HPWH and PowerIn_SecLoopPump) are missing from the DataFrame')
        return df

    if len(sum_tm_cols) == 0:
        print('Cannot calculate COP as the temperature maintenance power columns (such as PowerIn_SwingTank) are missing from the DataFrame')
        return df
    
    # Create new DataFrame with one column called 'PowerIn_Primary' that contains the sum of the specified columns
    sum_power_in_df = pd.DataFrame({'PowerIn_Primary': df[sum_primary_cols].sum(axis=1),
                                    'PowerIn_TM': df[sum_tm_cols].sum(axis=1)
                                    })
    df['COP_DHWSys_2'] = (df[cop_primary_column_name] * (sum_power_in_df['PowerIn_Primary']/df['PowerIn_Total'])) + (cop_tm * (sum_power_in_df['PowerIn_TM']/df['PowerIn_Total']))
    # NULLify incomplete calculations
    sum_power_in_df.loc[df[sum_primary_cols].isna().any(axis=1), "PowerIn_Primary"] = np.nan
    sum_power_in_df.loc[df[sum_tm_cols].isna().any(axis=1), "PowerIn_TM"] = np.nan
    df.loc[df[sum_primary_cols+sum_tm_cols].isna().any(axis=1), "COP_DHWSys_2"] = np.nan
    
    return df

def convert_on_off_col_to_bool(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Convert "ON"/"OFF" string values to boolean ``True``/``False`` in specified columns.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of sensor data.
    column_names : list
        List of column names containing ``"ON"``/``"OFF"`` (or ``"On"``/``"Off"``)
        strings to be converted to boolean values.

    Returns
    -------
    pd.DataFrame
        Dataframe with the specified columns converted to boolean values.
    """
    
    mapping = {'ON': True, 'OFF': False, 'On': True, 'Off': False}
    
    for column_name in column_names: 
        df[column_name] = df[column_name].map(mapping).where(df[column_name].notna(), df[column_name])
    
    return df

def convert_c_to_f(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Convert specified columns from degrees Celsius to Fahrenheit.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of sensor data.
    column_names : list
        List of column names whose values are currently in Celsius and need to
        be converted to Fahrenheit.

    Returns
    -------
    pd.DataFrame
        Dataframe with the specified columns converted from Celsius to
        Fahrenheit.
    """
    for col in column_names:
        if col in df.columns.to_list():
            try:
                pd.to_numeric(df[col])
                df[col] = df[col].apply(temp_c_to_f_non_noaa)
            except ValueError:
                print(f"{col} is not a numeric value column and could not be converted.")
        else:
            print(f"{col} is not included in this data set.")
    return df

def convert_btuhr_to_kw(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Convert specified columns from BTU/hr to kilowatts.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of sensor data.
    column_names : list
        List of column names whose values are currently in BTU/hr and need to
        be converted to kW.

    Returns
    -------
    pd.DataFrame
        Dataframe with the specified columns converted from BTU/hr to kW.
    """
    for col in column_names:
        if col in df.columns.to_list():
            try:
                pd.to_numeric(df[col])
                df[col] = df[col].apply(power_btuhr_to_kw)
            except ValueError:
                print(f"{col} is not a numeric value column and could not be converted.")
        else:
            print(f"{col} is not included in this data set.")
    return df

def convert_l_to_g(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Convert specified columns from liters to gallons.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of sensor data.
    column_names : list
        List of column names whose values are currently in liters and need to
        be converted to gallons.

    Returns
    -------
    pd.DataFrame
        Dataframe with the specified columns converted from liters to gallons.
    """
    for col in column_names:
        if col in df.columns.to_list():
            try:
                pd.to_numeric(df[col])
                df[col] = df[col].apply(volume_l_to_g)
            except ValueError:
                print(f"{col} is not a numeric value column and could not be converted.")
        else:
            print(f"{col} is not included in this data set.")
    return df

def flag_dhw_outage(df: pd.DataFrame, daily_df : pd.DataFrame, dhw_outlet_column : str, supply_temp : int = 110, consecutive_minutes : int = 15) -> pd.DataFrame:
    """
    Detect DHW outage events and return an alarm event dataframe.

    Identifies periods where DHW outlet temperature falls below ``supply_temp``
    for at least ``consecutive_minutes`` consecutive minutes, then records an
    ALARM event for each affected day.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of sensor data on minute intervals.
    daily_df : pd.DataFrame
        Pandas dataframe of sensor data on daily intervals.
    dhw_outlet_column : str
        Name of the column in ``df`` that contains the DHW temperature supplied
        to building occupants.
    supply_temp : int, optional
        Minimum acceptable DHW supply temperature in °F. Defaults to 110.
    consecutive_minutes : int, optional
        Number of consecutive minutes below ``supply_temp`` required to qualify
        as a DHW outage. Defaults to 15.

    Returns
    -------
    pd.DataFrame
        Dataframe indexed by ``start_time_pt`` containing ``'ALARM'`` events
        for each day on which a DHW outage occurred.
    """
    # TODO edge case for outage that spans over a day
    events = {
        'start_time_pt' : [],
        'end_time_pt' : [],
        'event_type' : [],
        'event_detail' : [],
    }
    mask = df[dhw_outlet_column] < supply_temp
    for day in daily_df.index:
        print(day)
        next_day = day + pd.Timedelta(days=1)
        filtered_df = mask.loc[(mask.index >= day) & (mask.index < next_day)]

        consecutive_condition = filtered_df.rolling(window=consecutive_minutes).min() == 1
        if consecutive_condition.any():
            # first_true_index = consecutive_condition['supply_temp'].idxmax()
            first_true_index = consecutive_condition.idxmax()
            adjusted_time = first_true_index - pd.Timedelta(minutes=consecutive_minutes-1)
            events['start_time_pt'].append(day)
            events['end_time_pt'].append(next_day - pd.Timedelta(minutes=1))
            events['event_type'].append("ALARM")
            events['event_detail'].append(f"Hot Water Outage Occured (first one starting at {adjusted_time.strftime('%H:%M')})")
    event_df = pd.DataFrame(events)
    event_df.set_index('start_time_pt', inplace=True)
    return event_df

def generate_event_log_df(config : ConfigManager):
    """
    Create an event log dataframe from a user-submitted Event_log.csv file.

    Parameters
    ----------
    config : ConfigManager
        The ConfigManager object that holds configuration data for the pipeline.
        Points to the Event_log.csv file via ``config.get_event_log_path()``.

    Returns
    -------
    pd.DataFrame
        Dataframe indexed by ``start_time_pt`` and formatted from the events in
        Event_log.csv. Returns an empty dataframe with the expected columns if
        the file cannot be read.
    """
    event_filename = config.get_event_log_path()
    try:
        event_df = pd.read_csv(event_filename)
        event_df['start_time_pt'] = pd.to_datetime(event_df['start_time_pt'])
        event_df['end_time_pt'] = pd.to_datetime(event_df['end_time_pt'])
        event_df.set_index('start_time_pt', inplace=True)
        return event_df
    except Exception as e:
        print(f"Error processing file {event_filename}: {e}")
        return pd.DataFrame({
            'start_time_pt' : [],
            'end_time_pt' : [],
            'event_type' : [],
            'event_detail' : [],
        })

def aggregate_df(df: pd.DataFrame, ls_filename: str = "", complete_hour_threshold : float = 0.8, complete_day_threshold : float = 1.0, remove_partial : bool = True) -> (pd.DataFrame, pd.DataFrame):
    """
    Aggregate minute-level data into hourly and daily dataframes.

    Energy columns (matching ``.*Energy.*`` but not ``EnergyRate`` or BTU
    suffixes) are summed; all other numeric columns are averaged. Optionally
    appends load-shift schedule data and removes partial hours/days.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of minute-by-minute sensor data.
    ls_filename : str, optional
        Path to the load-shift schedule CSV file (e.g.
        ``"full/path/to/pipeline/input/loadshift_matrix.csv"``). The CSV must
        have at least four columns: ``date``, ``startTime``, ``endTime``, and
        ``event``. Defaults to ``""``.
    complete_hour_threshold : float, optional
        Fraction of minutes in an hour required to count as a complete hour,
        expressed as a float (e.g. 80% = 0.8). Defaults to 0.8. Only
        applicable when ``remove_partial`` is ``True``.
    complete_day_threshold : float, optional
        Fraction of hours in a day required to count as a complete day,
        expressed as a float (e.g. 80% = 0.8). Defaults to 1.0. Only
        applicable when ``remove_partial`` is ``True``.
    remove_partial : bool, optional
        If ``True``, removes partial hours and days from the aggregated
        dataframes. Defaults to ``True``.

    Returns
    -------
    hourly_df : pd.DataFrame
        Aggregated hourly dataframe, including a ``'system_state'`` column if
        a valid load-shift file was provided.
    daily_df : pd.DataFrame
        Aggregated daily dataframe, including a ``'load_shift_day'`` column if
        a valid load-shift file was provided.
    """
    # If df passed in empty, we just return empty dfs for hourly_df and daily_df
    if (df.empty):
        return pd.DataFrame(), pd.DataFrame()

    # Start by splitting the dataframe into sum, which has all energy related vars, and mean, which has everything else. Time is calc'd differently because it's the index
    sum_df = (df.filter(regex=".*Energy.*")).filter(regex="^(?!.*EnergyRate).*(?<!BTU)$")
    # NEEDS TO INCLUDE: EnergyOut_PrimaryPlant_BTU
    mean_df = df.filter(regex="^((?!Energy)(?!EnergyOut_PrimaryPlant_BTU).)*$")

    # Resample downsamples the columns of the df into 1 hour bins and sums/means the values of the timestamps falling within that bin
    hourly_sum = sum_df.resample('H').sum()
    hourly_mean = mean_df.resample('H').mean(numeric_only=True)
    # Same thing as for hours, but for a whole day
    daily_sum = sum_df.resample("D").sum()
    daily_mean = mean_df.resample('D').mean(numeric_only=True)

    # combine sum_df and mean_df into one hourly_df, then try and print that and see if it breaks
    hourly_df = pd.concat([hourly_sum, hourly_mean], axis=1)
    daily_df = pd.concat([daily_sum, daily_mean], axis=1)

    partial_day_removal_exclusion = []

    # appending loadshift data
    if ls_filename != "" and os.path.exists(ls_filename):
        ls_df = pd.read_csv(ls_filename)
        # Parse 'date' and 'startTime' columns to create 'startDateTime'
        ls_df['startDateTime'] = pd.to_datetime(ls_df['date'] + ' ' + ls_df['startTime'])
        # Parse 'date' and 'endTime' columns to create 'endDateTime'
        ls_df['endDateTime'] = pd.to_datetime(ls_df['date'] + ' ' + ls_df['endTime'])
        daily_df["load_shift_day"] = False
        hourly_df["system_state"] = 'normal'
        partial_day_removal_exclusion = ["load_shift_day","system_state"]
        for index, row in ls_df.iterrows():
            startDateTime = row['startDateTime']
            endDateTime = row['endDateTime']
            event = row['event']

            # Update 'system_state' in 'hourly_df' and 'load_shift_day' in 'daily_df' based on conditions
            hourly_df.loc[(hourly_df.index >= startDateTime) & (hourly_df.index < endDateTime), 'system_state'] = event
            daily_df.loc[daily_df.index.date == startDateTime.date(), 'load_shift_day'] = True
            daily_df.loc[daily_df.index.date == endDateTime.date(), 'load_shift_day'] = True
    else:
        print(f"The loadshift file '{ls_filename}' does not exist. Thus loadshifting will not be added to daily dataframe.")
    
    # if any day in hourly table is incomplete, we should delete that day from the daily table as the averaged data it contains will be from an incomplete day.
    if remove_partial:
        hourly_df, daily_df = remove_partial_days(df, hourly_df, daily_df, complete_hour_threshold, complete_day_threshold, partial_day_removal_exclusion = partial_day_removal_exclusion)
    return hourly_df, daily_df

def convert_time_zone(df: pd.DataFrame, tz_convert_from: str = 'UTC', tz_convert_to: str = 'America/Los_Angeles') -> pd.DataFrame:
    """
    Convert a dataframe's DatetimeIndex from one timezone to another.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of sensor data whose index should be timezone-converted.
    tz_convert_from : str, optional
        Timezone string the index is currently in. Defaults to ``'UTC'``.
    tz_convert_to : str, optional
        Timezone string the index should be converted to. Defaults to
        ``'America/Los_Angeles'``.

    Returns
    -------
    pd.DataFrame
        Dataframe with its index converted to the target timezone (stored
        without timezone info as a naive datetime index).
    """
    time_UTC = df.index.tz_localize(tz_convert_from)
    time_PST = time_UTC.tz_convert(tz_convert_to)
    df['time_pt'] = time_PST.tz_localize(None)
    df.set_index('time_pt', inplace=True)
    return df

def shift_accumulative_columns(df : pd.DataFrame, column_names : list = []):
    """
    Convert accumulative columns to period-difference (non-cumulative) values.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of sensor data.
    column_names : list, optional
        Names of columns to convert from cumulative-sum data to
        non-cumulative difference data. If an empty list is provided, all
        columns are converted. Defaults to ``[]``.

    Returns
    -------
    pd.DataFrame
        Dataframe with the specified columns (or all columns) converted from
        cumulative to period-difference values.
    """
    df.sort_index(inplace = True)
    df_diff = df - df.shift(1)
    df_diff[df.shift(1).isna()] = np.nan
    df_diff.iloc[0] = np.nan
    if len(column_names) == 0:
        return df_diff
    for column_name in column_names:
        if column_name in df.columns:
            df[column_name] = df_diff[column_name]
    return df

def create_summary_tables(df: pd.DataFrame):
    """
    Create hourly and daily summary tables from minute-by-minute data.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of minute-by-minute sensor data.

    Returns
    -------
    hourly_df : pd.DataFrame
        Hourly mean aggregation of the input data, with partial hours removed.
    daily_df : pd.DataFrame
        Daily mean aggregation of the input data, with partial days removed.
    """
    # If df passed in empty, we just return empty dfs for hourly_df and daily_df
    if (df.empty):
        return pd.DataFrame(), pd.DataFrame()
    
    hourly_df = df.resample('H').mean()
    daily_df = df.resample('D').mean()

    hourly_df, daily_df = remove_partial_days(df, hourly_df, daily_df)
    return hourly_df, daily_df

def remove_partial_days(df, hourly_df, daily_df, complete_hour_threshold : float = 0.8, complete_day_threshold : float = 1.0, partial_day_removal_exclusion : list = []):
    """
    Remove hourly and daily rows that are derived from insufficient minute-level data.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas dataframe of minute-by-minute sensor data.
    hourly_df : pd.DataFrame
        Aggregated hourly dataframe.
    daily_df : pd.DataFrame
        Aggregated daily dataframe.
    complete_hour_threshold : float, optional
        Fraction of minutes in an hour required to count as a complete hour,
        expressed as a float (e.g. 80% = 0.8). Defaults to 0.8.
    complete_day_threshold : float, optional
        Fraction of hours in a day required to count as a complete day,
        expressed as a float (e.g. 80% = 0.8). Defaults to 1.0.
    partial_day_removal_exclusion : list, optional
        Column names to skip when evaluating completeness. Defaults to ``[]``.

    Returns
    -------
    hourly_df : pd.DataFrame
        Hourly dataframe with incomplete hours removed and sparse columns
        nullified.
    daily_df : pd.DataFrame
        Daily dataframe with incomplete days removed and sparse columns
        nullified.

    Raises
    ------
    Exception
        If ``complete_hour_threshold`` or ``complete_day_threshold`` is not
        between 0 and 1.
    """
    if complete_hour_threshold < 0.0 or complete_hour_threshold > 1.0:
        raise Exception("complete_hour_threshold must be a float between 0 and 1 to represent a percent (e.g. 80% = 0.8)")
    if complete_day_threshold < 0.0 or complete_day_threshold > 1.0:
        raise Exception("complete_day_threshold must be a float between 0 and 1 to represent a percent (e.g. 80% = 0.8)")
    
    num_minutes_required = 60.0 * complete_hour_threshold
    incomplete_hours = []
    for hour in hourly_df.index:
        next_hour = hour + pd.Timedelta(hours=1)
        filtered_df = df.loc[(df.index >= hour) & (df.index < next_hour)]
        if len(filtered_df.index) < num_minutes_required:
            incomplete_hours.append(hour)
        else:
            for column in hourly_df.columns.to_list():
                if column not in partial_day_removal_exclusion:
                    not_null_count = filtered_df[column].notna().sum()
                    if not_null_count < num_minutes_required:
                        hourly_df.loc[hour, column] = np.nan

    hourly_df = hourly_df.drop(incomplete_hours)
    
    num_complete_hours_required = 24.0 * complete_day_threshold
    incomplete_days = []
    for day in daily_df.index:
        next_day = day + pd.Timedelta(days=1)
        filtered_df = hourly_df.loc[(hourly_df.index >= day) & (hourly_df.index < next_day)]
        if len(filtered_df.index) < num_complete_hours_required:
            incomplete_days.append(day)
        else:
            for column in daily_df.columns.to_list():
                if column not in partial_day_removal_exclusion:
                    not_null_count = filtered_df[column].notna().sum()
                    if not_null_count < num_complete_hours_required:
                        daily_df.loc[day, column] = np.nan
    daily_df = daily_df.drop(incomplete_days)

    return hourly_df, daily_df


def join_to_hourly(hourly_data: pd.DataFrame, noaa_data: pd.DataFrame, oat_column_name : str = 'OAT_NOAA') -> pd.DataFrame:
    """
    Left-join weather data onto the hourly dataframe.

    Parameters
    ----------
    hourly_data : pd.DataFrame
        Hourly sensor dataframe.
    noaa_data : pd.DataFrame
        Weather (e.g. NOAA) dataframe to join.
    oat_column_name : str, optional
        Name of the outdoor air temperature column in ``noaa_data``. Defaults
        to ``'OAT_NOAA'``.

    Returns
    -------
    pd.DataFrame
        Hourly dataframe left-joined with the weather dataframe. Returns
        ``hourly_data`` unchanged if the OAT column in ``noaa_data`` contains
        no non-null values.
    """
    #fixing pipelines for new years
    if oat_column_name in noaa_data.columns and not noaa_data[oat_column_name].notnull().any():
        return hourly_data
    out_df = hourly_data.join(noaa_data)
    return out_df


def join_to_daily(daily_data: pd.DataFrame, cop_data: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join COP data onto the daily dataframe.

    Parameters
    ----------
    daily_data : pd.DataFrame
        Daily sensor dataframe.
    cop_data : pd.DataFrame
        COP values dataframe to join.

    Returns
    -------
    pd.DataFrame
        Daily dataframe left-joined with the COP dataframe.
    """
    out_df = daily_data.join(cop_data)
    return out_df

def apply_equipment_cop_derate(df: pd.DataFrame, equip_cop_col: str, r_val : int = 16) -> pd.DataFrame:
    """
    Derate equipment-method system COP based on building R-value.

    Derate percentages applied:

    - R12–R16: 12%
    - R16–R20: 10%
    - R20–R24: 8%
    - R24–R28: 6%
    - R28–R32: 4%
    - > R32: 2%

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing the equipment COP column to derate.
    equip_cop_col : str
        Name of the COP column to derate.
    r_val : int, optional
        Building R-value used to determine the derate factor. Defaults to 16.

    Returns
    -------
    pd.DataFrame
        Dataframe with ``equip_cop_col`` multiplied by the appropriate derate
        factor.

    Raises
    ------
    Exception
        If ``r_val`` is less than 12.
    """
    derate = 1 # R12-R16
    if r_val >= 12:
        if r_val < 16:
            derate = 0.88
        elif r_val < 20:
            derate = 0.9
        elif r_val < 24:
            derate = .92
        elif r_val < 28:
            derate = .94
        elif r_val < 32:
            derate = .96
        else:
            derate = .98
    else:
        raise Exception("R value for Equipment COP derate must be at least 12")
    
    df[equip_cop_col] =  df[equip_cop_col] * derate
    return df

def create_data_statistics_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-column data-gap statistics aggregated by day.

    Must be called on the raw minute-level dataframe after ``rename_sensors()``
    and before ``ffill_missing()``. Each original column is expanded into three
    derived columns:

    - ``<col>_missing_mins``: number of minutes in the day with no reported
      value.
    - ``<col>_avg_gap``: average consecutive gap length (in minutes) for that
      day.
    - ``<col>_max_gap``: maximum consecutive gap length (in minutes) for that
      day.

    Parameters
    ----------
    df : pd.DataFrame
        Minute-level dataframe after ``rename_sensors()`` and before
        ``ffill_missing()`` has been called.

    Returns
    -------
    pd.DataFrame
        Day-indexed dataframe containing the three gap-statistic columns for
        each original column.
    """
    min_time = df.index.min()
    start_day = min_time.floor('D')

    # If min_time is not exactly at the start of the day, move to the next day
    if min_time != start_day:
        start_day = start_day + pd.tseries.offsets.Day(1)

    # Build a complete minutely timestamp index over the full date range
    full_index = pd.date_range(start=start_day,
                               end=df.index.max().floor('D') - pd.Timedelta(minutes=1),
                               freq='T')
    
    # Reindex to include any completely missing minutes
    df_full = df.reindex(full_index)
    # df_full = df_full.select_dtypes(include='number')
    # print("1",df_full)
    # Resample daily to count missing values per column
    total_missing = df_full.isna().resample('D').sum().astype(int)
    # Function to calculate max consecutive missing values
    def max_consecutive_nans(x):
        is_na = pd.Series(x).isna().reset_index(drop=True)
        groups = (is_na != is_na.shift()).cumsum()
        return is_na.groupby(groups).sum().max() or 0

    # Function to calculate average consecutive missing values
    def avg_consecutive_nans(x):
        is_na = pd.Series(x).isna().reset_index(drop=True)
        groups = (is_na != is_na.shift()).cumsum()
        gap_lengths = is_na.groupby(groups).sum()
        gap_lengths = gap_lengths[gap_lengths > 0]
        if len(gap_lengths) == 0:
            return 0
        return gap_lengths.mean()

    # Apply daily, per column
    # print("hello?",type(df_full.index))
    max_consec_missing = df_full.resample('D').agg(max_consecutive_nans)
    avg_consec_missing = df_full.resample('D').agg(avg_consecutive_nans)

    # Rename columns to include a suffix
    total_missing = total_missing.add_suffix('_missing_mins')
    max_consec_missing = max_consec_missing.add_suffix('_max_gap')
    avg_consec_missing = avg_consec_missing.add_suffix('_avg_gap')

    # Concatenate along columns (axis=1)
    combined_df = pd.concat([total_missing, max_consec_missing, avg_consec_missing], axis=1)

    return combined_df
