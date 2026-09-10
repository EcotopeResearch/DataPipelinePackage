# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

**EcoPipeline** (`ecopipeline`, v2.4.1) is a Python library for processing sensor data from heat pump water heater (HPWH) systems. It implements a 3-stage ETL pipeline: Extract → Transform → Load, with an optional Event Tracking stage for anomaly detection. Developed by Ecotope Inc.

## Common Commands

This project uses [uv](https://docs.astral.sh/uv/). The interpreter is pinned in
`.python-version` and the dependency set is locked in `uv.lock`.

```bash
# Create/refresh the environment from the lockfile
uv sync

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/transform_test.py

# Run a single test by name
uv run pytest tests/event_tracking_test.py -k "test_boundary_alarm"

# Bump the version (updates pyproject.toml and uv.lock together)
uv version --bump patch

# Build sdist + wheel
uv build
```

## Pipeline Architecture

Data flows through four stages via central entry-point functions:

```python
from ecopipeline import ConfigManager
from ecopipeline import extract, transform, event_tracking, load

config = ConfigManager("path/to/config.ini")
raw_df, weather_df     = extract.central_extract_function(config, process_type="csv")
df, hourly_df, daily_df = transform.central_transform_function(config, raw_df, weather_df)
alarm_df               = event_tracking.central_alarm_df_creator(df, daily_df, config)
load.central_load_function(config, df, hourly_df, daily_df, alarm_df)
```

All stages produce and consume pandas DataFrames with datetime indexes. The package is a library—there is no CLI.

### Stage 1 — Extract (`src/ecopipeline/extract/`)

`central_extract_function()` returns `(raw_df, weather_df)`.

Supported `process_type` values map to processor classes:

| `process_type`     | Class / Source                   |
|--------------------|----------------------------------|
| `csv`              | `CSVProcessor`                   |
| `csv_long`         | `LongCSVProcessor` (long-form)   |
| `csv_mb`           | `ModbusCSVProcessor`             |
| `csv_dent`         | `DentCSVProcessor`               |
| `csv_flow`         | `FlowCSVProcessor`               |
| `csv_msa`          | `MSACSVProcessor`                |
| `csv_egauge`       | `EGaugeCSVProcessor`             |
| `csv_small_planet` | `SmallPlanetCSVProcessor`        |
| `json`             | `JSONProcessor`                  |
| `api_tb`           | ThingsBoard cloud IoT            |
| `api_skycentrics`  | Skycentrics cloud API            |
| `api_fm`           | Field Manager API                |
| `api_licor`        | LI-COR Cloud API                 |
| `api_bluedot`      | Bluedot API                      |

Weather data (outdoor air temperature) is fetched automatically from Open Meteo for the same time window. When reprocessing, previously pulled API data is read from cached CSV files.

**Timestamp parsing.** `raw_time_column` and `time_column_format` are only honoured when
`use_defaults=False`; otherwise `_get_time_indicator_defaults()` replaces them with the
per-`process_type` defaults. `time_column_format` is passed straight to `pd.to_datetime(format=...)`,
so it takes `strftime` directives. Downstream code assumes a **timezone-naive** index:
`convert_time_zone()` calls `index.tz_localize()`, which raises `TypeError: Already tz-aware`
on an index parsed with `%z`. Timestamps carrying a UTC offset (`2026-09-03 16:00:00-07:00`)
therefore need the offset stripped before parsing, or the index made naive before transform.
A file whose offsets change across a DST boundary also fails outright under pandas 3 with
`ValueError: Mixed timezones detected` -- the processors do not pass `utc=True`.

### Stage 2 — Transform (`src/ecopipeline/transform/`)

`central_transform_function()` returns `(df_minute, df_hourly, df_daily)`.

Processing order:
1. Rename sensors via `Variable_Names.csv` mapping
2. Round timestamps to nearest minute
3. Forward-fill missing values using per-sensor `changepoint` / `ffill_length` rules
4. Convert timezones — skipped unless `tz_convert_from != tz_convert_to`; both default to `America/Los_Angeles`, so no conversion happens by default
5. Average duplicate timestamps
6. Run optional **pre-aggregation hook** for site-specific logic
7. Aggregate: energy columns (`.*Energy.*` but not `EnergyRate` or `*BTU`) are **summed**; all other numeric columns are **averaged**. Partial hours/days below the completeness thresholds are dropped here, inside `aggregate_df()`
8. Merge weather data onto the hourly DataFrame, then resample it to a daily OAT column
9. Run optional **post-aggregation hook**

Key utility functions available for custom hooks: `heat_output_calc()`, `estimate_power()`, `cop_method_1()`, `cop_method_2()`, `flag_dhw_outage()`, `remove_outliers()`, `nullify_erroneous()`.

### Stage 3 — Event Tracking (`src/ecopipeline/event_tracking/`)

`central_alarm_df_creator()` detects anomalies using 14 alarm types, each in `alarms/`:

`Boundary`, `PowerRatio` (7-day rolling window), `AbnormalCOP` (default bounds 0–4.5), `TMSetpoint`, `BalancingValve`, `HPWHInlet`, `HPWHOutlet`, `BackupUse`, `HPWHOutage`, `BlownFuse`, `SOOChange`, `ShortCycle`, `TempRange`, `LSInconsist`

All alarm classes extend the `Alarm` base class (`event_tracking/Alarm.py`, one level above `alarms/`). Alarms carry certainty levels (high=3, medium=2, low=1) and overlap resolution logic. Unless `upload_alarm_set=False`, this stage also writes the accumulated alarm-set DataFrame via `AlarmSetLoader`.

### Stage 4 — Load (`src/ecopipeline/load/`)

`central_load_function()` UPSERTs data into MySQL. Key behaviors:
- NULL values do **not** overwrite non-NULL values in the database
- Tables and columns are created automatically as needed
- `Loader` writes the minute/hourly/daily frames; `AlarmLoader` writes alarms and handles overlap resolution between instances
- Each DataFrame is written only when it is non-`None` and non-empty

## ConfigManager

All configuration and DB access flows through `ConfigManager` (from `utils/ConfigManager.py`):

```python
config = ConfigManager("path/to/config.ini")
conn   = config.connect_db()
```

`config.ini` required sections: `[database]` (user/password/host/database), `[minute]`/`[hour]`/`[day]` (table_name), `[input]` (directory), `[output]` (directory), `[data]` (directory + optional API credentials). On Ecotope's server, Windows drive letters (`R:`, `F:`) are automatically remapped to POSIX mount points.

## Input Metadata Files

Located in the `input/` directory configured in `config.ini`:

- **`Variable_Names.csv`** — maps raw column names to canonical sensor names; drives renaming, forward-fill parameters, bounds, and alarm codes
- **`Event_log.csv`** — user-submitted maintenance/commissioning events
- **`loadshift_matrix.csv`** — optional demand-response load-shift schedules

## Deprecated Code

Many older functions are deprecated and should not be used in new code. Prefer the class-based processors and central functions over the legacy standalone functions (`extract_new()`, `extract_files()`, `csv_to_df()`, `dent_csv_to_df()`, `get_noaa_data()`, `fm_api_to_df()`, etc.).

## Dependencies

Core: `pandas`, `numpy`, `mysql-connector-python`, `scikit-learn`, `scipy`,
`openmeteo_requests`, `requests`, `pytz`. Each is declared in `pyproject.toml` with a floor at the current
release and a ceiling below the next major; the exact resolved set lives in `uv.lock`.
Current ranges: `numpy>=2.4,<3.0`, `pandas>=3.0,<4.0`, `scikit-learn>=1.9,<2.0`,
`scipy>=1.15,<2.0`, `mysql-connector-python>=26.7,<27.0`, `openmeteo_requests>=1.7,<2.0`,
`requests>=2.34,<3.0`, `pytz>=2026.3`.

`scipy` is used directly by `transform.convert_temp_resistance_type()`, which interpolates
the knot tables in `utils/thermistor_curves/*.csv` with `make_interp_spline(..., k=3)`. Those
curves were previously pickled scipy `interp1d` objects under `utils/pkls/`, which has been
deleted. Do not reintroduce pickled third-party objects as a storage format -- they bind the
file to a private scipy module path and to whatever version wrote them.

Python is pinned to 3.11 only (`requires-python = ">=3.11,<3.12"`, with `.python-version`
holding `3.11`).

`src/ecopipeline/__init__.py` checks the installed numpy version before importing
pandas. numpy outside `>=2.4,<3.0` raises an `ImportError` naming the fix, instead
of surfacing as a `numpy.dtype size changed` ABI error from inside pandas.
