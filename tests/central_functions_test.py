"""
Unit tests for the four central pipeline functions:
  - central_extract_function
  - central_transform_function
  - central_alarm_df_creator
  - central_load_function
"""

import pytest
from unittest.mock import MagicMock, patch, call
import pandas as pd
from pandas.testing import assert_frame_equal
from datetime import datetime
import numpy as np

from ecopipeline.extract.extract import central_extract_function
from ecopipeline.transform.transform import central_transform_function
from ecopipeline.load.load import central_load_function
from ecopipeline.event_tracking.event_tracking import central_alarm_df_creator


# ─────────────────────────── shared helpers ──────────────────────────────────

def _make_config(
    db_name="test_db",
    minute_table="minute_table",
    hour_table="hour_table",
    day_table="day_table",
    var_names_path="path/to/Variable_Names.csv",
):
    config = MagicMock()
    config.get_db_name.return_value = db_name
    config.get_table_name.side_effect = lambda key: {
        "minute": minute_table,
        "hour": hour_table,
        "day": day_table,
    }.get(key, f"{key}_table")
    config.get_var_names_path.return_value = var_names_path
    return config


def _make_minute_df(n=120):
    idx = pd.date_range("2023-01-01", periods=n, freq="min")
    return pd.DataFrame({"sensor_a": np.ones(n), "sensor_b": np.zeros(n)}, index=idx)


def _make_hourly_df():
    idx = pd.date_range("2023-01-01", periods=2, freq="h")
    return pd.DataFrame({"sensor_a": [1.0, 2.0]}, index=idx)


def _make_daily_df():
    idx = pd.date_range("2023-01-01", periods=1, freq="D")
    return pd.DataFrame({"sensor_a": [1.5]}, index=idx)


def _make_alarm_df():
    idx = pd.DatetimeIndex([datetime(2023, 1, 1, 1)], name="start_time_pt")
    return pd.DataFrame(
        {
            "end_time_pt": [datetime(2023, 1, 1, 2)],
            "alarm_type": ["boundary"],
            "alarm_detail": ["sensor_a out of range"],
            "variable_name": ["sensor_a"],
        },
        index=idx,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  central_extract_function
# ══════════════════════════════════════════════════════════════════════════════

@patch("ecopipeline.extract.extract._get_time_indicator_defaults",
       return_value=("DateTime", "%Y/%m/%d %H:%M:%S"))
@patch("ecopipeline.extract.extract._get_lat_and_long", return_value=(47.6, -122.3))
@patch("ecopipeline.extract.extract.get_OAT_open_meteo")
@patch("ecopipeline.extract.extract.CSVProcessor")
def test_central_extract_csv_returns_raw_and_weather(
    mock_csv_cls, mock_oat, mock_latlong, mock_defaults
):
    """Happy path: CSV process type should return raw data and weather."""
    config = _make_config()
    raw_data = _make_minute_df()
    weather_data = pd.DataFrame(
        {"airTemp_F": [50.0]},
        index=pd.date_range("2023-01-01", periods=1, freq="h"),
    )
    mock_csv_cls.return_value.get_raw_data.return_value = raw_data
    mock_oat.return_value = weather_data

    result_raw, result_weather = central_extract_function(
        config, "csv", start_time=datetime(2023, 1, 1)
    )

    assert not result_raw.empty
    assert not result_weather.empty
    mock_csv_cls.assert_called_once()
    mock_oat.assert_called_once()


@patch("ecopipeline.extract.extract._get_time_indicator_defaults",
       return_value=("DateTime", "%Y/%m/%d %H:%M:%S"))
@patch("ecopipeline.extract.extract.CSVProcessor")
def test_central_extract_empty_raw_produces_empty_weather(mock_csv_cls, mock_defaults):
    """When the file processor returns no data, weather should also be empty."""
    config = _make_config()
    mock_csv_cls.return_value.get_raw_data.return_value = pd.DataFrame()

    result_raw, result_weather = central_extract_function(
        config, "csv", start_time=datetime(2023, 1, 1)
    )

    assert result_raw.empty
    assert result_weather.empty


@patch("ecopipeline.extract.extract._get_time_indicator_defaults",
       return_value=("DateTime", "%Y/%m/%d %H:%M:%S"))
@patch("ecopipeline.extract.extract._get_lat_and_long", return_value=(47.6, -122.3))
@patch("ecopipeline.extract.extract.get_OAT_open_meteo")
@patch("ecopipeline.extract.extract.CSVProcessor")
def test_central_extract_pull_weather_false_skips_weather_call(
    mock_csv_cls, mock_oat, mock_latlong, mock_defaults
):
    """pull_weather_data=False must not call get_OAT_open_meteo."""
    config = _make_config()
    mock_csv_cls.return_value.get_raw_data.return_value = _make_minute_df()

    _, result_weather = central_extract_function(
        config, "csv", start_time=datetime(2023, 1, 1), pull_weather_data=False
    )

    mock_oat.assert_not_called()
    assert result_weather.empty


def test_central_extract_unknown_process_type_raises():
    """An unrecognised process_type should raise an Exception."""
    config = _make_config()
    with pytest.raises(Exception):
        central_extract_function(
            config, "not_a_real_type", start_time=datetime(2023, 1, 1)
        )


@patch("ecopipeline.extract.extract._get_time_indicator_defaults",
       return_value=("DateTime", "%Y/%m/%d %H:%M:%S"))
@patch("ecopipeline.extract.extract.get_last_full_day_from_db")
@patch("ecopipeline.extract.extract.CSVProcessor")
def test_central_extract_no_start_time_queries_db(
    mock_csv_cls, mock_last_day, mock_defaults
):
    """When start_time is None the function must query the DB for the last full day."""
    config = _make_config()
    mock_last_day.return_value = datetime(2023, 1, 1)
    mock_csv_cls.return_value.get_raw_data.return_value = pd.DataFrame()

    central_extract_function(config, "csv")  # start_time=None

    mock_last_day.assert_called_once_with(config, tz_aware = False)


# ══════════════════════════════════════════════════════════════════════════════
#  central_transform_function
# ══════════════════════════════════════════════════════════════════════════════

@patch("ecopipeline.transform.transform.join_to_hourly")
@patch("ecopipeline.transform.transform.aggregate_df")
@patch("ecopipeline.transform.transform.avg_duplicate_times")
@patch("ecopipeline.transform.transform.ffill_missing")
@patch("ecopipeline.transform.transform.round_time")
@patch("ecopipeline.transform.transform.rename_sensors")
def test_central_transform_returns_three_dataframes(
    mock_rename, mock_round, mock_ffill, mock_avg, mock_agg, mock_join
):
    """The function must always return a 3-tuple of DataFrames."""
    config = _make_config()
    df = _make_minute_df()
    hourly, daily = _make_hourly_df(), _make_daily_df()

    mock_rename.return_value = df
    mock_ffill.return_value = df
    mock_avg.return_value = df
    mock_agg.return_value = (hourly, daily)

    result = central_transform_function(config, df)

    assert isinstance(result, tuple) and len(result) == 3
    assert all(isinstance(r, pd.DataFrame) for r in result)


@patch("ecopipeline.transform.transform.aggregate_df")
@patch("ecopipeline.transform.transform.avg_duplicate_times")
@patch("ecopipeline.transform.transform.ffill_missing")
@patch("ecopipeline.transform.transform.round_time")
@patch("ecopipeline.transform.transform.rename_sensors")
def test_central_transform_pre_aggregation_bad_return_raises_type_error(
    mock_rename, mock_round, mock_ffill, mock_avg, mock_agg
):
    """pre_aggregation_func that returns non-DataFrame must raise TypeError."""
    config = _make_config()
    df = _make_minute_df()
    mock_rename.return_value = df
    mock_ffill.return_value = df
    mock_avg.return_value = df

    def bad_pre(df):
        return "not a dataframe"

    with pytest.raises(TypeError):
        central_transform_function(config, df, pre_aggregation_func=bad_pre)


@patch("ecopipeline.transform.transform.aggregate_df")
@patch("ecopipeline.transform.transform.avg_duplicate_times")
@patch("ecopipeline.transform.transform.ffill_missing")
@patch("ecopipeline.transform.transform.round_time")
@patch("ecopipeline.transform.transform.rename_sensors")
def test_central_transform_post_aggregation_bad_return_raises_type_error(
    mock_rename, mock_round, mock_ffill, mock_avg, mock_agg
):
    """post_aggregation_func that returns non-tuple must raise TypeError."""
    config = _make_config()
    df = _make_minute_df()
    hourly, daily = _make_hourly_df(), _make_daily_df()
    mock_rename.return_value = df
    mock_ffill.return_value = df
    mock_avg.return_value = df
    mock_agg.return_value = (hourly, daily)

    def bad_post(df, hourly_df, daily_df):
        return "not a tuple"

    with pytest.raises(TypeError):
        central_transform_function(config, df, post_aggregation_func=bad_post)


@patch("ecopipeline.transform.transform.join_to_hourly")
@patch("ecopipeline.transform.transform.aggregate_df")
@patch("ecopipeline.transform.transform.avg_duplicate_times")
@patch("ecopipeline.transform.transform.ffill_missing")
@patch("ecopipeline.transform.transform.round_time")
@patch("ecopipeline.transform.transform.rename_sensors")
def test_central_transform_pre_aggregation_func_output_passed_to_aggregator(
    mock_rename, mock_round, mock_ffill, mock_avg, mock_agg, mock_join
):
    """aggregate_df must receive the DataFrame returned by pre_aggregation_func."""
    config = _make_config()
    df = _make_minute_df()
    enriched_df = df.copy()
    enriched_df["extra_col"] = 999.0
    hourly, daily = _make_hourly_df(), _make_daily_df()

    mock_rename.return_value = df
    mock_ffill.return_value = df
    mock_avg.return_value = df
    mock_agg.return_value = (hourly, daily)

    def pre_func(d):
        return enriched_df

    central_transform_function(config, df, pre_aggregation_func=pre_func)

    # First positional argument to aggregate_df should be the enriched df
    passed_df = mock_agg.call_args[0][0]
    assert "extra_col" in passed_df.columns


@patch("ecopipeline.transform.transform.join_to_hourly")
@patch("ecopipeline.transform.transform.aggregate_df")
@patch("ecopipeline.transform.transform.avg_duplicate_times")
@patch("ecopipeline.transform.transform.ffill_missing")
@patch("ecopipeline.transform.transform.round_time")
@patch("ecopipeline.transform.transform.rename_sensors")
def test_central_transform_weather_merged_into_hourly(
    mock_rename, mock_round, mock_ffill, mock_avg, mock_agg, mock_join
):
    """When weather_df is provided, join_to_hourly should be called."""
    config = _make_config()
    df = _make_minute_df()
    hourly, daily = _make_hourly_df(), _make_daily_df()
    weather_df = pd.DataFrame(
        {"airTemp_F": [50.0, 52.0]},
        index=pd.date_range("2023-01-01", periods=2, freq="h"),
    )

    mock_rename.return_value = df
    mock_ffill.return_value = df
    mock_avg.return_value = df
    mock_agg.return_value = (hourly, daily)
    mock_join.return_value = hourly

    central_transform_function(config, df, weather_df=weather_df)

    mock_join.assert_called_once()


# ══════════════════════════════════════════════════════════════════════════════
#  central_alarm_df_creator
# ══════════════════════════════════════════════════════════════════════════════

def test_central_alarm_empty_df_returns_empty_dataframe():
    """Empty input df must short-circuit and return an empty DataFrame."""
    config = _make_config()
    result = central_alarm_df_creator(pd.DataFrame(), pd.DataFrame(), config)
    assert isinstance(result, pd.DataFrame)
    assert result.empty


@patch("pandas.read_csv", side_effect=FileNotFoundError)
def test_central_alarm_missing_variable_names_returns_empty(mock_read_csv):
    """FileNotFoundError on Variable_Names.csv must return an empty DataFrame."""
    config = _make_config()
    result = central_alarm_df_creator(_make_minute_df(), pd.DataFrame(), config)
    assert isinstance(result, pd.DataFrame)
    assert result.empty


@patch("pandas.read_csv")
def test_central_alarm_system_column_missing_raises(mock_read_csv):
    """Passing a non-empty system when the 'system' column is absent must raise."""
    config = _make_config()
    mock_read_csv.return_value = pd.DataFrame(
        {"variable_alias": ["sensor_a"], "variable_name": ["Sensor_A"]}
    )

    with pytest.raises(Exception):
        central_alarm_df_creator(
            _make_minute_df(), pd.DataFrame(), config, system="sys1"
        )


def _mock_alarm_cls(alarm_df=None):
    """Return a mock alarm detector whose find_alarms() returns alarm_df."""
    if alarm_df is None:
        alarm_df = pd.DataFrame()
    instance = MagicMock()
    instance.find_alarms.return_value = alarm_df
    cls = MagicMock(return_value=instance)
    return cls


@patch("pandas.read_csv")
def test_central_alarm_all_detectors_called_and_results_concatenated(mock_read_csv):
    """All 14 alarm detectors must have find_alarms called; detected events are combined."""
    config = _make_config()
    config.get_table_name.return_value = "day_table"
    mock_read_csv.return_value = pd.DataFrame(
        {"variable_alias": ["sensor_a"], "variable_name": ["Sensor_A"]}
    )

    # Build one alarm event to be "detected" by the Boundary detector
    alarm_event = pd.DataFrame(
        {
            "end_time_pt": [datetime(2023, 1, 1, 2)],
            "alarm_type": ["boundary"],
            "alarm_detail": ["out of range"],
            "variable_name": ["sensor_a"],
        },
        index=pd.DatetimeIndex([datetime(2023, 1, 1, 1)], name="start_time_pt"),
    )

    boundary_cls = _mock_alarm_cls(alarm_event)
    empty_cls = _mock_alarm_cls()

    patches = {
        "Boundary": boundary_cls,
        "PowerRatio": empty_cls,
        "AbnormalCOP": empty_cls,
        "TMSetpoint": empty_cls,
        "BalancingValve": empty_cls,
        "HPWHInlet": empty_cls,
        "HPWHOutlet": empty_cls,
        "BackupUse": empty_cls,
        "HPWHOutage": empty_cls,
        "BlownFuse": empty_cls,
        "SOOChange": empty_cls,
        "ShortCycle": empty_cls,
        "TempRange": empty_cls,
        "LSInconsist": empty_cls,
    }

    with patch.multiple("ecopipeline.event_tracking.event_tracking", **patches):
        result = central_alarm_df_creator(
            _make_minute_df(), pd.DataFrame(), config
        )

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert "alarm_type" in result.columns
    assert result["alarm_type"].iloc[0] == "boundary"


# ══════════════════════════════════════════════════════════════════════════════
#  central_load_function
# ══════════════════════════════════════════════════════════════════════════════

@patch("ecopipeline.load.load.Loader")
@patch("ecopipeline.load.load.AlarmLoader")
def test_central_load_calls_all_loaders_when_dfs_nonempty(mock_alarm_cls, mock_loader_cls):
    """All four loaders (alarm, minute, hourly, daily) must be called when dfs are present."""
    config = _make_config()

    central_load_function(
        config,
        _make_minute_df(),
        _make_hourly_df(),
        _make_daily_df(),
        _make_alarm_df(),
    )

    mock_alarm_cls.return_value.load_database.assert_called_once()
    assert mock_loader_cls.return_value.load_database.call_count == 3


@patch("ecopipeline.load.load.Loader")
@patch("ecopipeline.load.load.AlarmLoader")
def test_central_load_skips_loaders_for_empty_dfs(mock_alarm_cls, mock_loader_cls):
    """No loader must be invoked when all DataFrames are empty."""
    config = _make_config()

    central_load_function(
        config,
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
    )

    mock_alarm_cls.return_value.load_database.assert_not_called()
    mock_loader_cls.return_value.load_database.assert_not_called()


@patch("ecopipeline.load.load.Loader")
@patch("ecopipeline.load.load.AlarmLoader")
def test_central_load_skips_loaders_for_none_dfs(mock_alarm_cls, mock_loader_cls):
    """No loader must be invoked when all DataFrames are None."""
    config = _make_config()

    central_load_function(config, None, None, None, None)

    mock_alarm_cls.return_value.load_database.assert_not_called()
    mock_loader_cls.return_value.load_database.assert_not_called()


@patch("ecopipeline.load.load.Loader")
@patch("ecopipeline.load.load.AlarmLoader")
def test_central_load_uses_correct_table_names_from_config(mock_alarm_cls, mock_loader_cls):
    """Loader.load_database must be called with the table names returned by config."""
    config = _make_config()

    central_load_function(
        config,
        _make_minute_df(),
        _make_hourly_df(),
        _make_daily_df(),
        None,
    )

    load_calls = mock_loader_cls.return_value.load_database.call_args_list
    # Third positional arg (index 2) is the table name
    tables_used = [c[0][2] for c in load_calls]
    assert "minute_table" in tables_used
    assert "hour_table" in tables_used
    assert "day_table" in tables_used
