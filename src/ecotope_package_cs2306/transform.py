import pandas as pd
import numpy as np

#required input files
vars_filename = "input/Variable_Names.csv" #currently set to a test until real csv is completed

#STRETCH GOAL
#Functionality for alarms being raised based on bounds needs to happen here. 
#Try and keep things clean so that can be added later. 
def _removeOutliers(df, vars_filename):
    """
    Function will take the location of extracted data in a json and bounds information in a csv,
    store the json data in a pandas dataframe, then remove outliers above or below bounds as 
    designated by the csv. Function then returns the resulting dataframe. 
    Input: Pandas dataframe and file location of variable processing information
    Output: Pandas dataframe 
    """
    #Bounds dataframe holds acceptable ranges
    bounds_df = pd.read_csv(vars_filename)
    bounds_df = bounds_df.loc[:, ["variable_name", "lower_bound", "upper_bound"]]
    bounds_df.dropna(axis=0, thresh=2, inplace=True)
    bounds_df.set_index(['variable_name'], inplace=True)
    bounds_df = bounds_df[bounds_df.index.notnull()]
    
    #bad data removal loop
    for columnVar in df:
        if(columnVar in bounds_df.index):
            cLower = bounds_df.loc[columnVar]["lower_bound"]
            cUpper = bounds_df.loc[columnVar]["upper_bound"]
            for index in df.index:
                value = df.loc[(index, columnVar)]
                """
                #BUG: 
                Currently, below if replaces some var properly, some seemingly randomly. I checked, 
                cLower and cUpper are perfect, but value isn't what it's supposed to be? If you look 
                at the test prints, there are ONLY issues with Duty_QAHVPump
                """
                if(value < cLower or value > cUpper):
                    df.replace(to_replace = df.loc[(index, columnVar)], value = np.NaN, inplace = True)
    return df


#What are the select variables? Just variables with numeric inputs?
def _fillMissing(df):
    """
    Function will take a pandas dataframe and forward fill select variables with no entry. 
    Input: Pandas dataframe
    Output: Pandas dataframe
    """

    #ONLY forward fill if cumulative sum of var is not zero, e.g. don't do it until you find at least one valid entry first.
    #ffill_length column specifies time in minutes non-changepoint vars may be forward filled if missing. 
    #vars with changepoint column set to 1, forward fill without restriction. obv cumulative sum still applies.

    #forward fill being take previous var value and insert it. we need to do this column by column.
    #probably use pd.ffill

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



#Test function for simple main, will be removed once transform.py is complete
def outlier_fillTest():
    #Sample df, this should come from extract in actual running
    testdf_filename = "input/ecotope_wide_data.csv"
    df = pd.read_csv(testdf_filename)

    print("\nTesting _removeOutliers...\n")
    print(_removeOutliers(df, vars_filename))
    print("\nFinished testing _removeOutliers\n")

    #print("\nTesting _fillMissing...\n")


#Test main, will be removed once transform.py is complete
def __main__():
    outlier_fillTest()


if __name__ == '__main__':
    __main__()