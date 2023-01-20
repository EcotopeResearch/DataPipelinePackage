import extract
import load
import transform
import pandas as pd

vars_filename = "input/Variable_Names.csv"
testdata_filename = "output/1_11_23.csv"

def __main__():
    df = pd.read_csv(testdata_filename)
    df["time"] = pd.to_datetime(df["time"])
    df.set_index(df["time"], inplace=True)

    df = transform.rename_sensors(df, vars_filename)
    df = transform.remove_outliers(df, vars_filename)
    #df = transform.ffill_missing(df, vars_filename)

    print(df)
    pass
    #transform testing
    #outliersDF = transform.remove_outliers(vars_filename)
    

if __name__ == '__main__':
    __main__()