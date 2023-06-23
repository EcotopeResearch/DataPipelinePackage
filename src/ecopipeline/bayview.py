import pandas as pd
import numpy as np
import os 
from ecopipeline.config import _input_directory, _output_directory
from ecopipeline.unit_convert import energy_btu_to_kwh, energy_kwh_to_kbtu, energy_to_power



def _set_zone_vol(location: pd.Series, gals: int, total: int, zones: pd.Series) -> pd.DataFrame:
    """
    Function that initializes the dataframe that holds the volumes of each zone.

    Args:
        location (pd.Series)
        gals (int) 
        total (int) 
        zones (pd.Series)
    Returns: 
        pd.DataFrame: Pandas dataframe
    """
    relative_loc = location
    tank_frxn = relative_loc.subtract(relative_loc.shift(-1))
    gal_per_tank = gals
    tot_storage = total
    zone_gals = tank_frxn * tot_storage
    zone_gals = pd.Series.dropna(zone_gals)  # remove NA from leading math
    zone_list = zones
    gals_per_zone = pd.DataFrame({'Zone': zone_list, 'Zone_vol_g': zone_gals})
    return gals_per_zone


def _largest_less_than(df_row: pd.Series, target: int) -> str:
    """
    Function takes a sensor row and a target temperature and determines
    the zone with the highest temperature < 120 degrees.

    Args: 
        df_row (pd.DataFrame): A single row of a sensor Pandas Dataframe in a series 
        target (int): integer target
    Output: 
        str: A string of the name of the zone.
    """
    count = 0
    largest_less_than_120_tmp = ""
    for val in df_row:
        if val < target:
            largest_less_than_120_tmp = df_row.index[count]
            break
        count = count + 1

    return largest_less_than_120_tmp


def _get_vol_equivalent_to_120(df_row: pd.Series, location: pd.Series, gals: int, total: int, zones: pd.Series) -> float:
    """
    Function takes a row of sensor data and finds the total volume of water > 120 degrees.

    Args: 
        df_row (pd.Series) 
        location (pd.Series)
        gals (int)
        total (int)
        zones (pd.Series)
    Returns: 
        float: A float of the total volume of water > 120 degrees
    """
    try:
        if df_row.get('Temp_PrimaryStorageOutTop') < 120:
            return 0
        tvadder = 0
        vadder = 0
        gals_per_zone = _set_zone_vol(location, gals, total, zones)
        dfcheck = df_row.filter(regex='top|mid|bottom')
        # An empty or invalid dataframe would have Vol120 and ZoneTemp120 as columns with
        # values of 0, so we check if the size is 0 without those columns if the dataframe has no data.
        # Temp_CityWater_atSkid
        if (dfcheck.size == 0):
            return 0
        dftemp = df_row[['Temp_PrimaryStorageOutTop', 'Temp_top', 'Temp_midtop', 'Temp_mid', 'Temp_midbottom', 'Temp_bottom']]
        count = 0
        for val in dftemp:
            if dftemp.index[count] == "Temp_bottom":
                vadder += gals_per_zone[gals_per_zone.columns[1]][count]
                tvadder += dftemp[dftemp.index[count]] * gals_per_zone[gals_per_zone.columns[1]][count]
                break
            elif dftemp[dftemp.index[count + 1]] >= 120:
                vadder += gals_per_zone[gals_per_zone.columns[1]][count]
                tvadder += (dftemp[dftemp.index[count + 1]] + dftemp[dftemp.index[count]]) / \
                    2 * gals_per_zone[gals_per_zone.columns[1]][count]
            elif dftemp[dftemp.index[count + 1]] < 120:
                vadder += df_row.get('Vol120')
                tvadder += df_row.get('Vol120') * df_row.get('ZoneTemp120')
                break
            count += 1
        avg_temp_above_120 = tvadder / vadder
        temp_ratio = (avg_temp_above_120 - df_row.get('Temp_CityWater_atSkid')) / (120 - df_row.get('Temp_CityWater_atSkid'))
        return (temp_ratio * vadder)
    except ZeroDivisionError:
        print("DIVIDED BY ZERO ERROR")
        return 0


def _get_V120(df_row: pd.Series, location: pd.Series, gals: int, total: int, zones: pd.Series):
    """
    Function takes a row of sensor data and determines the volume of water > 120 degrees
    in the zone that has the highest sensor < 120 degrees.

    Args: 
        df_row (pd.Series): A single row of a sensor Pandas Dataframe in a series
        location (pd.Series)
        gals (int)
        total (int)
        zones (pd.Series)
    Returns: 
        float: A float of the total volume of water > 120 degrees     
    """
    try:
        gals_per_zone = _set_zone_vol(location, gals, total, zones)
        temp_cols = df_row[['Temp_PrimaryStorageOutTop', 'Temp_top', 'Temp_midtop', 'Temp_mid', 'Temp_midbottom', 'Temp_bottom']]
        if (temp_cols.size <= 3):
            return 0
        name_cols = ""
        name_cols = _largest_less_than(temp_cols, 120)
        count = 0
        name_col_index = 0
        for index in temp_cols.index:
            if index == name_cols:
                name_col_index = count
                break
            count += 1
        if name_col_index <= 0 or name_col_index >= len(temp_cols.index):
            # top of tank is less than 120 degrees or entire tank > 120 
            return 0
        # subtract one to get index of first zone larger than 120
        name_col_index -= 1
        dV = gals_per_zone['Zone_vol_g'][name_col_index]
        V120 = ((temp_cols[temp_cols.index[name_col_index]] - 120) / (
            temp_cols[temp_cols.index[name_col_index]] - temp_cols[temp_cols.index[name_col_index+1]])) * dV
        return V120
    except ZeroDivisionError:
        print("DIVIDED BY ZERO ERROR")
        return 0


def _get_zone_Temp120(df_row: pd.Series) -> float:
    """
    Function takes a row of sensor data and determines average temperature of the temperature of the greater than 120 portion of the lowest zone that contains water at 120 degrees.

    Args: 
        df_row (pd.Series): A single row of a sensor Pandas Dataframe in a series
    Returns: 
        float: A float of the average temperature of the greater than 120 portion of the lowest zone that contains water at 120 degrees.
    """
    # if df_row["Temp_120"] != 120:
    #    return 0
    temp_cols = df_row[['Temp_PrimaryStorageOutTop', 'Temp_top', 'Temp_midtop', 'Temp_mid', 'Temp_midbottom', 'Temp_bottom']]
    if (temp_cols.size <= 3):
        return 0
    name_cols = _largest_less_than(temp_cols, 120)
    count = 0
    name_col_index = 0
    for index in temp_cols.index:
        if index == name_cols:
            name_col_index = count
            break
        count += 1
    
    if name_col_index <= 0 or name_col_index >= len(temp_cols.index):
        # if top of tank is < 120 or entire tank > 120, return nan
        return np.nan

    zone_Temp_120 = (120 + temp_cols[temp_cols.index[name_col_index - 1]]) / 2
    return zone_Temp_120


def get_storage_gals120(df: pd.DataFrame, location: pd.Series, gals: int, total: int, zones: pd.Series) -> pd.DataFrame:
    """
    Function that creates and appends the Gals120 data onto the Dataframe

    Args: 
        df (pd.Series): A Pandas Dataframe
        location (pd.Series)
        gals (int)
        total (int)
        zones (pd.Series)
    Returns: 
        pd.DataFrame: a Pandas Dataframe
    """
    if (len(df) > 0):
        df['Vol120'] = df.apply(_get_V120, axis=1, args=(
            location, gals, total, zones))
        df['ZoneTemp120'] = df.apply(_get_zone_Temp120, axis=1)
        df['Vol_Equivalent_to_120'] = df.apply(
            _get_vol_equivalent_to_120, axis=1, args=(location, gals, total, zones))

    return df


def _calculate_average_zone_temp(df: pd.DataFrame, substring: str):
    """
    Function that calculates the average temperature of the inputted zone.

    Args: 
        df (pd.Series): A Pandas Dataframe
        substring (str)
    Returns: 
        pd.DataFrame: a Pandas Dataframe
    """
    try:
        df_subset = df[[x for x in df if substring in x]]
        result = df_subset.sum(axis=1, skipna=True) / df_subset.count(axis=1)
        return result
    except ZeroDivisionError:
        print("DIVIDED BY ZERO ERROR")
        return 0


def get_temp_zones120(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function that keeps track of the average temperature of each zone.
    for this function to work, naming conventions for each parrallel tank must include 'Temp1' as the tempature at the top of the tank, 'Temp5' as that at the bottom of the tank, and 'Temp2'-'Temp4' as the tempatures in between.

    Args: 
        df (pd.Series): A Pandas Dataframe
    Returns: 
        pd.DataFrame: a Pandas Dataframe
    """
    df['Temp_top'] = _calculate_average_zone_temp(df, "Temp1")
    df['Temp_midtop'] = _calculate_average_zone_temp(df, "Temp2")
    df['Temp_mid'] = _calculate_average_zone_temp(df, "Temp3")
    df['Temp_midbottom'] = _calculate_average_zone_temp(df, "Temp4")
    df['Temp_bottom'] = _calculate_average_zone_temp(df, "Temp5")
    return df

def get_energy_by_min(df: pd.DataFrame) -> pd.DataFrame:
    """
    Energy is recorded cummulatively. Function takes the lagged differences in 
    order to get a per/minute value for each of the energy variables.

    Args: 
        df (pd.DataFrame): Pandas dataframe
    Returns: 
        pd.DataFrame: Pandas dataframe
    """
    energy_vars = df.filter(regex=".*Energy.*")
    energy_vars = energy_vars.filter(regex=".*[^BTU]$")
    for var in energy_vars:
        df[var] = df[var] - df[var].shift(1)
    return df

def verify_power_energy(df: pd.DataFrame):
    """
    Verifies that for each timestamp, corresponding power and energy variables are consistent
    with one another. Power ~= energy * 60. Margin of error TBD. Outputs to a csv file any
    rows with conflicting power and energy variables.

    Prereq: 
        Input dataframe MUST have had get_energy_by_min() called on it previously
    Args: 
        df (pd.DataFrame): Pandas dataframe
    Returns:
        None
    """

    out_df = pd.DataFrame(columns=['time_pt', 'power_variable', 'energy_variable',
                          'energy_value', 'power_value', 'expected_power', 'difference_from_expected'])
    energy_vars = (df.filter(regex=".*Energy.*")).filter(regex=".*[^BTU]$")
    power_vars = (df.filter(regex=".*Power.*")
                  ).filter(regex="^((?!Energy).)*$")
    df['time_pt'] = df.index
    power_energy_df = df[df.columns.intersection(
        ['time_pt'] + list(energy_vars) + list(power_vars))]
    del df['time_pt']

    margin_error = 5.0          # margin of error still TBD, 5.0 for testing purposes
    for pvar in power_vars:
        if (pvar != 'PowerMeter_SkidAux_Power'):
            corres_energy = pvar.replace('Power', 'Energy')
        if (pvar == 'PowerMeter_SkidAux_Power'):
            corres_energy = 'PowerMeter_SkidAux_Energty'
        if (corres_energy in energy_vars):
            temp_df = power_energy_df[power_energy_df.columns.intersection(['time_pt'] + list(energy_vars) + list(power_vars))]
            for i, row in temp_df.iterrows():
                expected = energy_to_power(row[corres_energy])
                low_bound = expected - margin_error
                high_bound = expected + margin_error
                if (row[pvar] != expected):
                    out_df.loc[len(df.index)] = [row['time_pt'], pvar, corres_energy,
                                                 row[corres_energy], row[pvar], expected, abs(expected - row[pvar])]
                    path_to_output = f'{_output_directory}power_energy_conflicts.csv'
                    if not os.path.isfile(path_to_output):
                        out_df.to_csv(path_to_output, index=False, header=out_df.columns)
                    else:
                        out_df.to_csv(path_to_output, index=False, mode='a', header=False)


def aggregate_values(df: pd.DataFrame, thermo_slice: str) -> pd.DataFrame:
    """
    Gets daily average of data for all relevant varibles. 

    Args:
        df (pd.DataFrame): Pandas DataFrame of minute by minute data
        thermo_slice (str): indicates the time at which slicing begins. If none no slicing is performed. The format of the thermo_slice string is "HH:MM AM/PM".

    Returns: 
        pd.DataFrame: Pandas DataFrame which contains the aggregated hourly data.
    """
    
    avg_sd = df[['Flow_CityWater', 'Flow_CityWater_atSkid', 'Temp_PrimaryStorageOutTop']].resample('D').mean()

    if thermo_slice is not None:
        avg_sd_6 = df.between_time(thermo_slice, "11:59PM")[
            ['Temp_CityWater_atSkid', 'Temp_CityWater']].resample('D').mean()
    else:
        avg_sd_6 = df[['Temp_CityWater_atSkid',
                       'Temp_CityWater']].resample('D').mean()

    #cop_inter = pd.DataFrame(index=avg_sd.index)
    df['Temp_RecircSupply_avg'] = ( df['Temp_RecircSupply_MXV1'] + df['Temp_RecircSupply_MXV2']) / 2
    df['HeatOut_PrimaryPlant'] = energy_kwh_to_kbtu(df['Flow_CityWater_atSkid'], df['Temp_PrimaryStorageOutTop'] - df['Temp_CityWater_atSkid'])
    df['HeatOut_SecLoop'] = energy_kwh_to_kbtu(df['Flow_SecLoop'], df['Temp_SecLoopHexOutlet'] - df['Temp_SecLoopHexInlet'])
    df['HeatOut_HW'] = energy_kwh_to_kbtu(df['Flow_CityWater'], df['Temp_RecircSupply_avg'] -  df['Temp_CityWater'])
    df['HeatLoss_TempMaint_MXV1'] = energy_kwh_to_kbtu(df['Flow_RecircReturn_MXV1'], df['Temp_RecircSupply_MXV1'] - df['Temp_RecircReturn_MXV1'])
    df['HeatLoss_TempMaint_MXV2'] = energy_kwh_to_kbtu(df['Flow_RecircReturn_MXV2'], df['Temp_RecircSupply_MXV2'] - df['Temp_RecircReturn_MXV2'])
    df['EnergyIn_SecLoopPump'] = df['PowerIn_SecLoopPump'] * (1/60)
    df['EnergyIn_HPWH'] = df['EnergyIn_HPWH']

    cop_inter = df [['Temp_RecircSupply_avg', 'HeatOut_PrimaryPlant', 'HeatOut_SecLoop', 'HeatOut_HW', 'HeatLoss_TempMaint_MXV1', 'HeatLoss_TempMaint_MXV2', 'EnergyIn_SecLoopPump', 'EnergyIn_HPWH']].resample('D').mean()

    cop_inter['HeatOut_HW_dyavg'] = energy_kwh_to_kbtu(avg_sd['Flow_CityWater'], cop_inter['Temp_RecircSupply_avg'] -
                                                       avg_sd_6['Temp_CityWater'])
    cop_inter['HeatOut_PrimaryPlant_dyavg'] = energy_kwh_to_kbtu(avg_sd['Flow_CityWater_atSkid'],
                                                                 avg_sd['Temp_PrimaryStorageOutTop'] -
                                                                 avg_sd_6['Temp_CityWater_atSkid'])

    return cop_inter


def calculate_cop_values(df: pd.DataFrame, heatLoss_fixed: int, thermo_slice: str) -> pd.DataFrame:
    """
    Performs COP calculations using the daily aggregated data. 

    Args: 
        df (pd.DataFrame): Pandas DataFrame to add COP columns to
        heatloss_fixed (float): fixed heatloss value 
        thermo_slice (str): the time at which slicing begins if we would like to thermo slice. 

    Returns: 
        pd.DataFrame: Pandas DataFrame with the added COP columns. 
    """
    cop_inter = pd.DataFrame()
    if (len(df) != 0):
        cop_inter = aggregate_values(df, thermo_slice)

    cop_values = pd.DataFrame(index=cop_inter.index, columns=[
                              "COP_DHWSys", "COP_DHWSys_dyavg", "COP_DHWSys_fixTMloss", "COP_PrimaryPlant", "COP_PrimaryPlant_dyavg"])

    try:
        cop_values['COP_DHWSys'] = (energy_btu_to_kwh(cop_inter['HeatOut_HW']) + (
            energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV1'])) + (
            energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV2']))) / (
                cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

        if thermo_slice is not None:
            cop_values['COP_DHWSys_dyavg'] = (energy_btu_to_kwh(cop_inter['HeatOut_HW_dyavg']) + (
                energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV1'])) + (
                energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV2']))) / (
                    cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

        cop_values['COP_DHWSys_fixTMloss'] = ((energy_btu_to_kwh(cop_inter['HeatOut_HW'])) + (
            energy_btu_to_kwh(heatLoss_fixed))) / ((cop_inter['EnergyIn_HPWH'] +
                                                    cop_inter['EnergyIn_SecLoopPump']))

        cop_values['COP_PrimaryPlant'] = (energy_btu_to_kwh(cop_inter['HeatOut_PrimaryPlant'])) / \
            (cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

        if thermo_slice is not None:
            cop_values['COP_PrimaryPlant_dyavg'] = (energy_btu_to_kwh(cop_inter['HeatOut_PrimaryPlant_dyavg'])) / \
                (cop_inter['EnergyIn_HPWH'] +
                 cop_inter['EnergyIn_SecLoopPump'])

    except ZeroDivisionError:
        print("DIVIDED BY ZERO ERROR")
        return df

    return cop_values