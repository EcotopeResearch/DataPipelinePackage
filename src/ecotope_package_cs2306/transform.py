import pandas as pd
import numpy as np
import os
from dateutil.parser import parse
from ecotope_package_cs2306.unit_convert import energy_to_power, energy_btu_to_kwh, energy_kwh_to_kbtu
from ecotope_package_cs2306.extract import _input_directory, _output_directory

pd.set_option('display.max_columns', None)

#from .transform remove_outliers, ffill_missing, sensor_adjustment, get_energy_by_min, verify_power_energy, calculate_intermediate_values, calculate_cop_values 


def rename_sensors(df, variable_names_path):
    variable_data = pd.read_csv(variable_names_path)
    variable_data = variable_data[1:86]
    variable_data = variable_data[['variable_alias', 'variable_name']]
    variable_data.dropna(axis=0, inplace=True)
    variable_alias = list(variable_data["variable_alias"])
    variable_true = list(variable_data["variable_name"])
    variable_alias_true_dict = dict(zip(variable_alias, variable_true))

    df.rename(columns=variable_alias_true_dict, inplace=True)


def avg_duplicate_times(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function will take in a dataframe and looks for duplicate timestamps due to daylight savings.
    Takes the average values between the duplicate timestamps.
    Input: Pandas dataframe
    Output: Pandas dataframe 
    """
    df = df.groupby('time').mean()
    return df


#Helper functions for remove_outliers and ffill_missing because I am too stupid to write a lambda
def _rm_cols(col, bounds_df):
    if(col.name in bounds_df.index):
        c_lower = float(bounds_df.loc[col.name]["lower_bound"])
        c_upper = float(bounds_df.loc[col.name]["upper_bound"])
        #for this to be one line, it could be the following:
        #col.mask((col > float(bounds_df.loc[col.name]["upper_bound"])) | (col < float(bounds_df.loc[col.name]["lower_bound"])), other = np.NaN, inplace = True)
        col.mask((col > c_upper) | (col < c_lower), other = np.NaN, inplace = True)

def _ffill(col, ffill_df):
    if(col.name in ffill_df.index):
        cp = ffill_df.loc[col.name]["changepoint"]
        length = ffill_df.loc[col.name]["ffill_length"]
        if(length != length): #check for nan, set to 0
            length = 0
        length = int(length) #casting to int to avoid float errors
        if(cp == 1): #ffill unconditionally
            col.fillna(method='ffill', inplace = True)
        elif(cp == 0): #ffill only up to length
            col.fillna(method='ffill', inplace = True, limit = length)


#TODO: remove_outliers STRETCH GOAL
#Functionality for alarms being raised based on bounds needs to happen here. 
def remove_outliers(df : pd.DataFrame, vars_filename) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and location of bounds information in a csv,
    store the bounds data in a dataframe, then remove outliers above or below bounds as 
    designated by the csv. Function then returns the resulting dataframe. 
    Input: Pandas dataframe and file location of variable processing information
    Output: Pandas dataframe 
    """
    bounds_df = pd.read_csv(vars_filename) # bounds dataframe holds acceptable ranges
    bounds_df = bounds_df.loc[:, ["variable_name", "lower_bound", "upper_bound"]]
    bounds_df.dropna(axis=0, thresh=2, inplace=True)
    bounds_df.set_index(['variable_name'], inplace=True)
    bounds_df = bounds_df[bounds_df.index.notnull()]

    df.apply(_rm_cols, args=(bounds_df,))

    return df


#TODO: Improve function effiency with vectorization
def ffill_missing(df : pd.DataFrame, vars_filename) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and forward fill select variables with no entry. 
    Input: Pandas dataframe
    Output: Pandas dataframe
    """
    ffill_df = pd.read_csv(vars_filename)  #ffill dataframe holds ffill length and changepoint bool
    ffill_df = ffill_df.loc[:, ["variable_name", "changepoint", "ffill_length"]]
    ffill_df.dropna(axis=0, thresh=2, inplace=True) #drop data without changepoint AND ffill_length
    ffill_df.set_index(['variable_name'], inplace=True)
    ffill_df = ffill_df[ffill_df.index.notnull()] #drop data without names

    #improved .apply setup
    df.apply(_ffill, args=(ffill_df,))
                
    return df


def sensor_adjustment(df : pd.DataFrame) -> pd.DataFrame:
    """
    Reads in input/adjustments.csv and applies necessary adjustments to the dataframe
    Input: DataFrame to be adjusted
    Output: Adjusted Dataframe
    """
    adjustments = pd.read_csv(f"{_input_directory}adjustments.csv")
    if adjustments.empty:
        return df
    adjustments["datetime_applied"] = pd.to_datetime(adjustments["datetime_applied"])
    df = df.sort_values(by = "datetime_applied")
    
    for adjustment in adjustments:
        adjustment_datetime = adjustment["datetime_applied"]
        df_pre = df.loc[df['time'] < adjustment_datetime]
        df_post = df.loc[df['time'] >= adjustment_datetime]
        match adjustment["adjustment_type"]:
            case "add":
                continue
            case "remove":
                df_post[adjustment["sensor_1"]] = np.nan
            case "swap":
                df_post[[adjustment["sensor_1"],adjustment["sensor_2"]]] = df_post[[adjustment["sensor_2"],adjustment["sensor_1"]]]
        df = pd.concat([df_pre, df_post], ignore_index=True)
    return df


def get_energy_by_min(df : pd.DataFrame) -> pd.DataFrame:
    """
    Energy is recorded cummulatively. Function takes the lagged differences in 
    order to get a per/minute value for each of the energy variables.
    
    Input: Pandas dataframe
    Output: Pandas dataframe
    """
    energy_vars = df.filter(regex=".*Energy.*")
    energy_vars = energy_vars.filter(regex=".*[^BTU]$")
    for var in energy_vars:
        df[var] = df[var] - df[var].shift(1)
    return df


def verify_power_energy(df : pd.DataFrame):
    """
    Verifies that for each timestamp, corresponding power and energy variables are consistent
    with one another. Power ~= energy * 60. Margin of error TBD. Outputs to a csv file any
    rows with conflicting power and energy variables.

    Prereq: Input df MUST have had get_energy_by_min() called on it previously
    Input: Pandas dataframe
    Output: Creates or appends to a csv file
    """
    
    out_df = pd.DataFrame(columns=['time', 'power_variable', 'energy_variable', 'energy_value' ,'power_value', 'expected_power', 'difference_from_expected'])
    energy_vars = (df.filter(regex=".*Energy.*")).filter(regex=".*[^BTU]$")
    power_vars = (df.filter(regex=".*Power.*")).filter(regex="^((?!Energy).)*$")
    df['time'] = df.index
    power_energy_df = df[df.columns.intersection(['time'] + list(energy_vars) + list(power_vars))]
    del df['time']

    margin_error = 5.0          # margin of error still TBD, 5.0 for testing purposes
    for pvar in power_vars:
        if (pvar != 'PowerMeter_SkidAux_Power'):
            corres_energy = pvar.replace('Power', 'Energy')
        if (pvar == 'PowerMeter_SkidAux_Power'):
            corres_energy = 'PowerMeter_SkidAux_Energty'
        if (corres_energy in energy_vars):
            temp_df = power_energy_df[power_energy_df.columns.intersection(['time'] + list(energy_vars) + list(power_vars))]
            for i, row in temp_df.iterrows():
                expected = energy_to_power(row[corres_energy])
                low_bound = expected - margin_error
                high_bound = expected + margin_error
                if(row[pvar] != expected):
                    out_df.loc[len(df.index)] = [row['time'], pvar, corres_energy, row[corres_energy], row[pvar], expected, abs(expected - row[pvar])] 
                    path_to_output = f'{_output_directory}power_energy_conflicts.csv'
                    if not os.path.isfile(path_to_output):
                      out_df.to_csv(path_to_output, index=False, header=out_df.columns)
                    else:
                      out_df.to_csv(path_to_output, index=False, mode='a', header=False)


def aggregate_values(df: pd.DataFrame) -> dict:
    print(df)
    after_6pm = df.index[0].replace(hour=6, minute=0)

    avg_sd = df[['Temp_RecircSupply_MXV1', 'Temp_RecircSupply_MXV2', 'Flow_CityWater_atSkid', 'Temp_PrimaryStorageOutTop', 'Temp_CityWater_atSkid', 'Flow_SecLoop', 'Temp_SecLoopHexOutlet', 'Temp_SecLoopHexInlet', 'Flow_CityWater', 'Temp_CityWater', 'Flow_RecircReturn_MXV1', 'Temp_RecircReturn_MXV1', 'Flow_RecircReturn_MXV2', 'Temp_RecircReturn_MXV2', 'PowerIn_SecLoopPump', 'EnergyIn_HPWH']].mean(axis=0, skipna=True)

    avg_sd_6 = df[after_6pm:][['Temp_CityWater_atSkid', 'Temp_CityWater']].mean(axis=0, skipna=True)

    cop_inter = dict()
    cop_inter['Temp_RecircSupply_avg'] = (avg_sd['Temp_RecircSupply_MXV1'] + avg_sd['Temp_RecircSupply_MXV2']) / 2
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
    cop_inter['EnergyIn_SecLoopPump'] = avg_sd['PowerIn_SecLoopPump'] * (1/60) * (1/1000)
    cop_inter['EnergyIn_HPWH'] = avg_sd['EnergyIn_HPWH'] * (1/60) * (1/1000)

    return cop_inter


def calculate_cop_values(df: pd.DataFrame) -> dict:
    heatLoss_fixed = 27.296
    ENERGYIN_SWINGTANK1 = 0
    ENERGYIN_SWINGTANK2 = 0

    cop_inter = aggregate_values(df)

    cop_values = dict()
    cop_values['COP_DHWSys'] = (energy_btu_to_kwh(cop_inter['HeatOut_HW']) + (
        energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV1'])) + (
        energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV2']))) / (
            cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'] + ENERGYIN_SWINGTANK1 +
            ENERGYIN_SWINGTANK2)

    cop_values['COP_DHWSys_dyavg'] = (energy_btu_to_kwh(cop_inter['HeatOut_HW_dyavg']) + (
        energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV1'])) + (
        energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV2']))) / (
            cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'] + ENERGYIN_SWINGTANK1 +
            ENERGYIN_SWINGTANK2)

    cop_values['COP_DHWSys_fixTMloss'] = ((energy_btu_to_kwh(cop_inter['HeatOut_HW'])) + (
        energy_btu_to_kwh(heatLoss_fixed))) / ((cop_inter['EnergyIn_HPWH'] +
                                             cop_inter['EnergyIn_SecLoopPump'] + ENERGYIN_SWINGTANK1 +
                                             ENERGYIN_SWINGTANK2))

    cop_values['COP_PrimaryPlant'] = (energy_btu_to_kwh(cop_inter['HeatOut_PrimaryPlant'])) / \
                                     (cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

    cop_values['COP_PrimaryPlant_dyavg'] = (energy_btu_to_kwh(cop_inter['HeatOut_PrimaryPlant_dyavg'])) / \
                                     (cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

    return cop_values

 
def aggregate_df(df: pd.DataFrame):
    """
    Input: Single pandas dataframe of minute-by-minute sensor data.
    Output: Two pandas dataframes of by the hour and by the day aggregated sensor data.
    """
    #Start by splitting the dataframe into sum, which has all energy related vars, and mean, which has everything else. Time is calc'd differently because it's the index
    sum_df = df.filter(regex=".*Energy.*")
    mean_df = df.filter(regex=".*[^Energy].*")

    #Resample downsamples the columns of the df into 1 hour bins and sums/means the values of the timestamps falling within that bin
    hourly_sum = sum_df.resample('H').sum()
    hourly_mean = mean_df.resample('H').mean()
    #Same thing as for hours, but for a whole day
    daily_sum = sum_df.resample("D").sum()
    daily_mean = mean_df.resample('D').mean()

    #combine sum_df and mean_df into one hourly_df, then try and print that and see if it breaks
    hourly_df = pd.concat([hourly_sum, hourly_mean], axis=1)
    daily_df = pd.concat([daily_sum, daily_mean], axis=1)

    return hourly_df, daily_df

""""
# #Test function
# def outlierTest():
#     testdf_filename = "input/ecotope_wide_data.csv"
#     df = pd.read_csv(testdf_filename)

#     print(df)
#     print("\nTesting _removeOutliers...\n")
#     print(remove_outliers(df, vars_filename))
#     print("\nFinished testing _removeOutliers\n")

# #Test function
# def ffillTest():
#     testdf_filename = "input/ecotope_wide_data.csv"
#     df = pd.read_csv(testdf_filename)
#     df = remove_outliers(df, vars_filename)

#     print("\nTesting _fillMissing...\n")
#     print(ffill_missing(df, vars_filename))
#     print("\nFinished testing _fillMissing\n")

# #Test function
# def testCopCalc():
#     df_path = "input/ecotope_wide_data.csv"
#     ecotope_data = pd.read_csv(df_path)
#     ecotope_data.set_index("time", inplace=True)

# # Test function
# def testPEV():
#     testdf_filename = "input/ecotope_wide_data.csv"
#     df = pd.read_csv(testdf_filename)
#     df = get_energy_by_min(df)

#     verify_power_energy(df)
"""


#Test main, will be removed once transform.py is complete
def __main__():
    file_path = "input/df.pkl"
    vars_filename = "input/Variable_Names.csv"

    df = pd.read_pickle(file_path)
    rename_sensors(df, vars_filename)
    df = avg_duplicate_times(df)
    df = remove_outliers(df, vars_filename)
    df = ffill_missing(df, vars_filename)
    df = sensor_adjustment(df)
    df = get_energy_by_min(df)
    verify_power_energy(df)
    cop_values = calculate_cop_values(df)
    hourly_df, daily_df = aggregate_df(df)

    print(df.head(10))
    print(hourly_df.head(10))
    print(daily_df)

    pass

if __name__ == '__main__':
    __main__()

