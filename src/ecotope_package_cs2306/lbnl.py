import pandas as pd
import re

def site_specific(df : pd.DataFrame, site : str) -> pd.DataFrame:
    """
    Does Site Specific Calculations for LBNL. The site name is searched using RegEx
    Input: dataframe of data and site name as a string
    Output: 
    """
    # Bob's site notes says add 55 Pa to the Pressure
    if re.search("MO2_", site):
        df["Pressure_staticP"] += 55

    # Calculate Power vars
    if re.search("(AZ2_01|AZ2_02|MO2_|IL2_|NW2_01)", site):  # All MO & IL sites.
        # Calculation goes negative to -0.001 sometimes.
       df["Power_OD_compressor1"] = (
        df["Power_OD_total1"] - df["Power_OD_fan1"]).apply(lambda x: max(0, x))
       df["Power_system1"] = df["Power_OD_total1"] + df["Power_AH1"]

    elif re.search("(AZ2_03)", site):
        df["Power_OD_total1"] = df["Power_OD_compressor1"] + df["Power_OD_fan1"]
        df["Power_AH1"] = df["Power_system1"] - df["Power_OD_total1"]

    elif re.search("(AZ2_04|AZ2_05)", site):
        df["Power_system1"] = df["Power_OD_total1"] + df["Power_AH1"]
    
    # Extra site specific calculations can be added with an extra elif statement and RegEx
    
    return df