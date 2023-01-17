import pandas as pd
import numpy as np
import os

#input files for tests, will come from parameters come deployment
vars_filename = "input/Variable_Names.csv" #currently set to a test until real csv is completed


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
    bounds_df = pd.read_csv(vars_filename) #Bounds dataframe holds acceptable ranges
    bounds_df = bounds_df.loc[:, ["variable_name", "lower_bound", "upper_bound"]]
    bounds_df.dropna(axis=0, thresh=2, inplace=True)
    bounds_df.set_index(['variable_name'], inplace=True)
    bounds_df = bounds_df[bounds_df.index.notnull()]
    for column_var in df:  #bad data removal loop
        if(column_var in bounds_df.index):
            c_lower = bounds_df.loc[column_var]["lower_bound"]
            c_upper = bounds_df.loc[column_var]["upper_bound"]
            for index in df.index:
                value = df.loc[(index, column_var)]
                if(value < c_lower or value > c_upper):
                    df.replace(to_replace = df.loc[(index, column_var)], value = np.NaN, inplace = True)
    return df


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
    for column_var in df:  #ffill loop
        if(column_var in ffill_df.index):
            cp = ffill_df.loc[column_var]["changepoint"]
            length = ffill_df.loc[column_var]["ffill_length"]
            if(length != length): #check for nan, set to 0
                length = 0
            length = int(length)
            if(cp == 1): #ffill unconditionally
                df.loc[:, [column_var]] = df.loc[:, [column_var]].fillna(method='ffill')
            elif(cp == 0): #ffill using length, PARTIALLY FILLS
                df.loc[:, [column_var]] = df.loc[:, [column_var]].fillna(method = 'ffill', limit = length)
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


def getEnergyByMinute(df : pd.DataFrame) -> pd.DataFrame:
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


def verifyPowerEnergy(df : pd.DataFrame):
    """
    Verifies that for each timestamp, corresponding power and energy variables are consistent
    with one another. Power ~= energy * 60. Margin of error TBD. Outputs to a csv file any
    rows with conflicting power and energy variables.
    Prereq: Input df must have had getEnergyByMinute() called on it previously
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


#Test function for simple main, will be removed once transform.py is complete
def outlierTest():
    testdf_filename = "input/ecotope_wide_data.csv"
    df = pd.read_csv(testdf_filename)

    print("\nTesting _removeOutliers...\n")
    print(remove_outliers(df, vars_filename))
    print("\nFinished testing _removeOutliers\n")

#Test function for simple main, will be removed once transform.py is complete
def ffillTest():
    testdf_filename = "input/ecotope_wide_data.csv"
    df = pd.read_csv(testdf_filename)
    df = remove_outliers(df, vars_filename)

    print("\nTesting _fillMissing...\n")
    print(ffill_missing(df, vars_filename))
    print("\nFinished testing _fillMissing\n")


#Test main, will be removed once transform.py is complete
def __main__():
    testdf_filename = "input/ecotope_wide_data.csv"
    df = pd.read_csv(testdf_filename)
    df = getEnergyByMinute(df)
    verifyPowerEnergy(df)
    pass



if __name__ == '__main__':
    __main__()