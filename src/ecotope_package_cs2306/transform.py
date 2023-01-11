import pandas as pd

#I'm just going to write generic functions that hopefully can be 
#copy pasted into whatever our transform.py ends up being. 

#There is no csv_filename, this SHOULD get passed a df
vars_filename = "input/vars_test.csv"


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
    print("Entered _removeOutliers")

    #data is brought in directly as a df, no additional work

    #Bounds setup df
    #Variable_Names.csv, I only need columns 1 (variable_name), 11 (lower_bound), and 12 (upper_bound).
    #Read it in, load into another pandas dataframe. ONLY load the row if it has at least one bound. 
    bounds_df = pd.read_csv(vars_filename)
    #keeping only variable_name (1), lower_bound (11), and upper_bound (12)
    bounds_df = bounds_df.loc[:, ["variable_name", "lower_bound", "upper_bound"]]
    #TODO: remove row if there is NaN in both lower_bound and upper_bound column. Use pd.apply lambda function to implement
    print(bounds_df)

    #Removal
    #Compare the two dataframes. For each row of the data, locate column names with names in the rows
    #of the bounds df. If matched, go all the way down that column, and remove the data IF it is not
    #within the set bounds indicated by bounds df col 2 and 3. 

    return df


#What are the select variables? Just variables with numeric inputs?
def _fillMissing(df):
    """
    Function will take a pandas dataframe and forward fill select variables with no entry. 

    Input: Pandas dataframe
    Output: Pandas dataframe
    """

    #ONLY forward fill if cumulative sum of var is not zero. 
    #ffill_length column specifies time in minutes non-changepoint vars may be forward filled if missing. 
    #vars with changepoint column set to 1, forward fill without restriction.

    #forward fill being take previous var value and insert it. we need to do this column by column.

    return df


def __main__():
    #Sample df, this should come from extract in actual running
    df = pd.DataFrame()
    #use csv_test to make dataframe

    print("\nTesting _removeOutliers...\n")
    _removeOutliers(df, vars_filename)
    print("\nFinished testing _removeOutliers\n")

    #print("\nTesting _fillMissing...\n")

if __name__ == '__main__':
    __main__()