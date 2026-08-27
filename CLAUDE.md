# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

**EcoPipeline** (`ecopipeline`, v2.2.3) is a Python library for processing sensor data from heat pump water heater (HPWH) systems. It implements a 3-stage ETL pipeline: Extract → Transform → Load, with an optional Event Tracking stage for anomaly detection. Developed by Ecotope Inc.

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

Weather data (outdoor air temperature) is fetched automatically from Open Meteo for the same time window. When reprocessing, previously pulled API data is read from cached CSV files.

### Stage 2 — Transform (`src/ecopipeline/transform/`)

`central_transform_function()` returns `(df_minute, df_hourly, df_daily)`.

Processing order:
1. Rename sensors via `Variable_Names.csv` mapping
2. Round timestamps to nearest minute; average duplicate timestamps
3. Forward-fill missing values using per-sensor `changepoint` / `ffill_length` rules
4. Convert timezones (UTC → configured local timezone)
5. Run optional **pre-aggregation hook** for site-specific logic
6. Aggregate: energy columns (`.*Energy.*` but not `EnergyRate` or `*BTU`) are **summed**; all other numeric columns are **averaged**
7. Run optional **post-aggregation hook**
8. Remove partial hours/days below completeness thresholds
9. Merge weather data onto hourly DataFrame

Key utility functions available for custom hooks: `heat_output_calc()`, `estimate_power()`, `cop_method_1()`, `cop_method_2()`, `flag_dhw_outage()`, `remove_outliers()`, `nullify_erroneous()`.

### Stage 3 — Event Tracking (`src/ecopipeline/event_tracking/`)

`central_alarm_df_creator()` detects anomalies using 12 alarm types, each in `alarms/`:

`Boundary`, `PowerRatio` (7-day rolling window), `AbnormalCOP` (default bounds 0–4.5), `TMSetpoint`, `BalancingValve`, `HPWHInlet`, `HPWHOutlet`, `BackupUse`, `HPWHOutage`, `BlownFuse`, `SOOChange`, `ShortCycle`

All alarm classes extend `Alarm` base class (`alarms/Alarm.py`). Alarms carry certainty levels (high=3, medium=2, low=1) and overlap resolution logic.

### Stage 4 — Load (`src/ecopipeline/load/`)

`central_load_function()` UPSERTs data into MySQL. Key behaviors:
- NULL values do **not** overwrite non-NULL values in the database
- Tables and columns are created automatically as needed
- `AlarmLoader` handles overlap resolution between alarm instances

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

Core: `pandas`, `numpy`, `mysql-connector-python`, `scikit-learn`, `openmeteo_requests`,
`requests`, `pytz`. Declared with upper bounds in `pyproject.toml`; the exact resolved
set lives in `uv.lock`. Python 3.11 only (`>=3.11,<3.12`) -- pandas 1.5.x, numpy 1.24.x,
and scikit-learn 1.2.x publish no cp312 wheels.

`src/ecopipeline/__init__.py` checks the installed numpy version before importing
pandas. numpy outside `>=1.24.1,<1.25` raises an `ImportError` naming the fix, instead
of surfacing as a `numpy.dtype size changed` ABI error from inside pandas.
