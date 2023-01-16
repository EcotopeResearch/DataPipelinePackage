import pandas as pd
import numpy as np

#input files for tests, will come from parameters come deployment
vars_filename = "input/Variable_Names.csv" #currently set to a test until real csv is completed


#TODO: _removeOutliers STRETCH GOAL
#Functionality for alarms being raised based on bounds needs to happen here. 
def _removeOutliers(df : pd.DataFrame, vars_filename) -> pd.DataFrame:
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
    for columnVar in df:  #bad data removal loop
        if(columnVar in bounds_df.index):
            cLower = bounds_df.loc[columnVar]["lower_bound"]
            cUpper = bounds_df.loc[columnVar]["upper_bound"]
            for index in df.index:
                value = df.loc[(index, columnVar)]
                if(value < cLower or value > cUpper):
                    df.replace(to_replace = df.loc[(index, columnVar)], value = np.NaN, inplace = True)
    return df


def _fillMissing(df : pd.DataFrame, vars_filename) -> pd.DataFrame:
    """
    Function will take a pandas dataframe and forward fill select variables with no entry. 
    Input: Pandas dataframe
    Output: Pandas dataframe
    """
    ffill_df = pd.read_csv(vars_filename)  #ffill dataframe holds ffill length and changepoint bool
    ffill_df = ffill_df.loc[:, ["variable_name", "changepoint", "ffill_length"]]
    ffill_df.dropna(axis=0, thresh=2, inplace=True) #drop data without changepoint AND ffill_length
    ffill_df.set_index(['variable_name'], inplace=True)
    ffill_df = ffill_df[ffill_df.index.notnull()]
    #TODO: Conditonal ffill, right now this does cumsum and ffills perfectly BUT it needs to not 
    #ffill if the gap is greater than ffill length. 
    df.ffill(inplace = True)

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
    Energy is recorded cummulatively. Function takes the lagged difference in 
    order to get a per/minute value for each of the energy variables.
    Input: Pandas dataframe
    Output: Pandas dataframe
    """
    energy_vars = df.filter(regex=".*Energy.*")
    energy_vars = energy_vars.filter(regex=".*[^BTU]$")
    for var in energy_vars:
        df[var] = df[var] - df[var].shift(1)
        df[var][0] = 0.0
    return df


def verifyPowerEnergy(df : pd.DataFrame) -> pd.DataFrame:
    """
    Verifies that for each timestamp, corresponding power and energy variables are consistent
    with one another. Power ~= energy * 60. 
    Input: Pandas dataframe
    Output: Pandas dataframe
    """


#Test function for simple main, will be removed once transform.py is complete
def outlierTest():
    testdf_filename = "input/ecotope_wide_data.csv"
    df = pd.read_csv(testdf_filename)

    print("\nTesting _removeOutliers...\n")
    print(_removeOutliers(df, vars_filename))
    print("\nFinished testing _removeOutliers\n")


#Test function for simple main, will be removed once transform.py is complete
def ffillTest():
    testdf_filename = "input/ecotope_wide_data.csv"
    df = pd.read_csv(testdf_filename)
    df = _removeOutliers(df, vars_filename)

    print("\nTesting _fillMissing...\n")
    print(_fillMissing(df, vars_filename))
    print("\nFinished testing _fillMissing\n")


#Test main, will be removed once transform.py is complete
def __main__():
    #outlierTest()
    ffillTest()
    pass




if __name__ == '__main__':
    __main__()