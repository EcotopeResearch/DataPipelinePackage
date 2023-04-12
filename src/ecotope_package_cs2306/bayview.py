import pandas as pd
import os 
from ecotope_package_cs2306.config import _input_directory, _output_directory
from ecotope_package_cs2306.unit_convert import energy_btu_to_kwh, energy_kwh_to_kbtu, energy_to_power

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
    avg_sd = df[['Temp_RecircSupply_MXV1', 'Temp_RecircSupply_MXV2', 'Flow_CityWater_atSkid', 'Temp_PrimaryStorageOutTop',
                 'Temp_CityWater_atSkid', 'Flow_SecLoop', 'Temp_SecLoopHexOutlet', 'Temp_SecLoopHexInlet', 'Flow_CityWater', 'Temp_CityWater',
                 'Flow_RecircReturn_MXV1', 'Temp_RecircReturn_MXV1', 'Flow_RecircReturn_MXV2', 'Temp_RecircReturn_MXV2', 'PowerIn_SecLoopPump',
                 'EnergyIn_HPWH']].resample('D').mean()

    if thermo_slice is not None:
        avg_sd_6 = df.between_time(thermo_slice, "11:59PM")[
            ['Temp_CityWater_atSkid', 'Temp_CityWater']].resample('D').mean()
    else:
        avg_sd_6 = df[['Temp_CityWater_atSkid',
                       'Temp_CityWater']].resample('D').mean()

    cop_inter = pd.DataFrame(index=avg_sd.index)
    cop_inter['Temp_RecircSupply_avg'] = (
        avg_sd['Temp_RecircSupply_MXV1'] + avg_sd['Temp_RecircSupply_MXV2']) / 2
    cop_inter['HeatOut_PrimaryPlant'] = energy_kwh_to_kbtu(avg_sd['Flow_CityWater_atSkid'],
                                                           avg_sd['Temp_PrimaryStorageOutTop'] -
                                                           avg_sd['Temp_CityWater_atSkid'])
    cop_inter['HeatOut_PrimaryPlant_dyavg'] = energy_kwh_to_kbtu(avg_sd['Flow_CityWater_atSkid'],
                                                                 avg_sd['Temp_PrimaryStorageOutTop'] -
                                                                 avg_sd_6['Temp_CityWater_atSkid'])
    cop_inter['HeatOut_SecLoop'] = energy_kwh_to_kbtu(avg_sd['Flow_SecLoop'], avg_sd['Temp_SecLoopHexOutlet'] -
                                                      avg_sd['Temp_SecLoopHexInlet'])
    cop_inter['HeatOut_HW'] = energy_kwh_to_kbtu(avg_sd['Flow_CityWater'], cop_inter['Temp_RecircSupply_avg'] -
                                                 avg_sd['Temp_CityWater'])
    cop_inter['HeatOut_HW_dyavg'] = energy_kwh_to_kbtu(avg_sd['Flow_CityWater'], cop_inter['Temp_RecircSupply_avg'] -
                                                       avg_sd_6['Temp_CityWater'])
    cop_inter['HeatLoss_TempMaint_MXV1'] = energy_kwh_to_kbtu(avg_sd['Flow_RecircReturn_MXV1'],
                                                              avg_sd['Temp_RecircSupply_MXV1'] -
                                                              avg_sd['Temp_RecircReturn_MXV1'])
    cop_inter['HeatLoss_TempMaint_MXV2'] = energy_kwh_to_kbtu(avg_sd['Flow_RecircReturn_MXV2'],
                                                              avg_sd['Temp_RecircSupply_MXV2'] -
                                                              avg_sd['Temp_RecircReturn_MXV2'])
    cop_inter['EnergyIn_SecLoopPump'] = avg_sd['PowerIn_SecLoopPump'] * \
        (1/60) * (1/1000)
    cop_inter['EnergyIn_HPWH'] = avg_sd['EnergyIn_HPWH']

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