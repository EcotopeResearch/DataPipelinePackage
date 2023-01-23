import pandas as pd
import numpy as np
import os
from dateutil.parser import parse

#from .transform remove_outliers, ffill_missing, sensor_adjustment, get_energy_by_min, verify_power_energy, calculate_intermediate_values, calculate_cop_values 

#input files for tests, will come from parameters come deployment
vars_filename = "input/Variable_Names.csv" #currently set to a test until real csv is completed

def rename_sensors(df, variable_names_path):
    variable_data = pd.read_csv(variable_names_path)
    variable_data = variable_data[1:87]
    variable_alias = list(variable_data["variable_alias"])
    variable_true = list(variable_data["variable_name"])
    variable_alias_true_dict = dict(zip(variable_alias, variable_true))

    df.rename(columns=variable_alias_true_dict, inplace=True)

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
    adjustments = pd.read_csv("input/adjustments.csv")
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
    # margin of error still TBD, 5.0 for testing purposes 
    margin_error = 5.0              
    energy_vars = (df.filter(regex=".*Energy.*")).filter(regex=".*[^BTU]$")
    power_vars = (df.filter(regex=".*Power.*")).filter(regex="^((?!Energy).)*$")
    for pvar in power_vars:
        if (pvar != 'PowerMeter_SkidAux_Power'):
            corres_energy = pvar.replace('Power', 'Energy')
        if (pvar == 'PowerMeter_SkidAux_Power'):
            corres_energy = 'PowerMeter_SkidAux_Energy'
        if (corres_energy in energy_vars):
            temp = df[[pvar, corres_energy]]
            for i, row in temp.iterrows():
                low_bound = row[corres_energy] * 60 - margin_error
                high_bound = row[corres_energy] * 60 + margin_error
                if (row[pvar] < low_bound or row[pvar] > high_bound):
                    out_df = df[(df[pvar] == row[pvar]) & (df[corres_energy] == row[corres_energy])]
                    path_to_output = 'output/power_energy_conflicts.csv'
                    if not os.path.isfile(path_to_output):
                      out_df.to_csv(path_to_output, header=df.columns)
                    else:
                      out_df.to_csv(path_to_output, index=False, mode='a', header=False)


def convert_to_kwh(sensor_readings):
    return sensor_readings / (60 * 3.412)


def get_kbtu_value(gpm, delta_t):
    return 60 * 8.33 * gpm * delta_t / 1000


def aggregate_values(df: pd.DataFrame) -> dict:
    after_6pm = str(parse(df.index[0]).replace(hour=6, minute=0))

    avg_sd = df[['Temp_RecircSupply_MXV1', 'Temp_RecircSupply_MXV2', 'Flow_CityWater_atSkid',
                 'Temp_PrimaryStorageOutTop', 'Temp_CityWater_atSkid',
                 'Flow_SecLoop', 'Temp_SecLoopHexOutlet', 'Temp_SecLoopHexInlet', 'Flow_CityWater', 'Temp_CityWater',
                 'Flow_RecircReturn_MXV1', 'Temp_RecircReturn_MXV1', 'Flow_RecircReturn_MXV2', 'Temp_RecircReturn_MXV2',
                 'PowerIn_SecLoopPump', 'EnergyIn_HPWH']].mean(axis=0, skipna=True)

    avg_sd_6 = df[after_6pm:][['Temp_CityWater_atSkid', 'Temp_CityWater']].mean(axis=0, skipna=True)

    cop_inter = dict()
    cop_inter['Temp_RecircSupply_avg'] = (avg_sd['Temp_RecircSupply_MXV1'] + avg_sd['Temp_RecircSupply_MXV2']) / 2
    cop_inter['HeatOut_PrimaryPlant'] = get_kbtu_value(avg_sd['Flow_CityWater_atSkid'],
                                                       avg_sd['Temp_PrimaryStorageOutTop'] -
                                                       avg_sd['Temp_CityWater_atSkid'])
    cop_inter['HeatOut_PrimaryPlant_dyavg'] = get_kbtu_value(avg_sd['Flow_CityWater_atSkid'],
                                                             avg_sd['Temp_PrimaryStorageOutTop'] -
                                                             avg_sd_6['Temp_CityWater_atSkid'])
    cop_inter['HeatOut_SecLoop'] = get_kbtu_value(avg_sd['Flow_SecLoop'], avg_sd['Temp_SecLoopHexOutlet'] -
                                                  avg_sd['Temp_SecLoopHexInlet'])
    cop_inter['HeatOut_HW'] = get_kbtu_value(avg_sd['Flow_CityWater'], cop_inter['Temp_RecircSupply_avg'] -
                                             avg_sd['Temp_CityWater'])
    cop_inter['HeatOut_HW_dyavg'] = get_kbtu_value(avg_sd['Flow_CityWater'], cop_inter['Temp_RecircSupply_avg'] -
                                                   avg_sd_6['Temp_CityWater'])
    cop_inter['HeatLoss_TempMaint_MXV1'] = get_kbtu_value(avg_sd['Flow_RecircReturn_MXV1'],
                                                          avg_sd['Temp_RecircSupply_MXV1'] -
                                                          avg_sd['Temp_RecircReturn_MXV1'])
    cop_inter['HeatLoss_TempMaint_MXV2'] = get_kbtu_value(avg_sd['Flow_RecircReturn_MXV2'],
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
    cop_values['COP_DHWSys'] = (convert_to_kwh(cop_inter['HeatOut_HW']) + (
        convert_to_kwh(cop_inter['HeatLoss_TempMaint_MXV1'])) + (
        convert_to_kwh(cop_inter['HeatLoss_TempMaint_MXV2']))) / (
            cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'] + ENERGYIN_SWINGTANK1 +
            ENERGYIN_SWINGTANK2)

    cop_values['COP_DHWSys_dyavg'] = (convert_to_kwh(cop_inter['HeatOut_HW_dyavg']) + (
        convert_to_kwh(cop_inter['HeatLoss_TempMaint_MXV1'])) + (
        convert_to_kwh(cop_inter['HeatLoss_TempMaint_MXV2']))) / (
            cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'] + ENERGYIN_SWINGTANK1 +
            ENERGYIN_SWINGTANK2)

    cop_values['COP_DHWSys_fixTMloss'] = ((convert_to_kwh(cop_inter['HeatOut_HW'])) + (
        convert_to_kwh(heatLoss_fixed))) / ((cop_inter['EnergyIn_HPWH'] +
                                             cop_inter['EnergyIn_SecLoopPump'] + ENERGYIN_SWINGTANK1 +
                                             ENERGYIN_SWINGTANK2))

    cop_values['COP_PrimaryPlant'] = (convert_to_kwh(cop_inter['HeatOut_PrimaryPlant'])) / \
                                     (cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

    cop_values['COP_PrimaryPlant_dyavg'] = (convert_to_kwh(cop_inter['HeatOut_PrimaryPlant_dyavg'])) / \
                                     (cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

    return cop_values


#Test function
def outlierTest():
    testdf_filename = "input/ecotope_wide_data.csv"
    df = pd.read_csv(testdf_filename)

    print(df)
    print("\nTesting _removeOutliers...\n")
    print(remove_outliers(df, vars_filename))
    print("\nFinished testing _removeOutliers\n")

#Test function
def ffillTest():
    testdf_filename = "input/ecotope_wide_data.csv"
    df = pd.read_csv(testdf_filename)
    df = remove_outliers(df, vars_filename)

    print("\nTesting _fillMissing...\n")
    print(ffill_missing(df, vars_filename))
    print("\nFinished testing _fillMissing\n")

#Test function
def testCopCalc():
    df_path = "input/ecotope_wide_data.csv"
    ecotope_data = pd.read_csv(df_path)
    ecotope_data.set_index("time", inplace=True)

#Test main, will be removed once transform.py is complete

def __main__():
    outlierTest()
    ffillTest()
    pass

if __name__ == '__main__':
    __main__()

