import pandas as pd
import re
from ecotope_package_cs2306.config import configure

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

def lbnl_sat_calculations(df: pd.DataFrame) -> pd.DataFrame:
    df_temp = df.filter(regex=r'.*Temp_SAT.*')
    df["Temp_SATAvg"] = df.mean(axis=1)

    return df

def lbnl_pressure_conversions(df: pd.DataFrame) -> pd.DataFrame:
    if ("Pressure_staticInWC" in df.columns) and ("Pressure_staticPa" in df.columns):
        inWC_2_Pa = 248.84
        df["Pressure_staticP"] = df["Pressure_staticPa"] + (inWC_2_Pa * df["Pressure_staticInWC"])
        return df

    return df

def lbnl_temperature_conversions(df: pd.DataFrame) -> pd.DataFrame:
    if "Temp_LL_C" in df.columns:
        df["Temp_LL_F"] = (9/5)*df["Temp_LL_C"] + 32
    
    if "Temp_SL_C" in df.columns:
        df["Temp_SL_F"] = (9/5)*df["Temp_SL_C"] + 32

    return df

def condensate_calculations(df: pd.DataFrame, site: str) -> pd.DataFrame:
    site_info_directory = configure.get('site_info', 'directory')
    site_info = pd.read_csv(site_info_directory)
    oz_2_m3 = 1 / 33810  # [m3/oz]
    water_density = 997  # [kg/m³]
    water_latent_vaporization = 2264.705  # [kJ/kg]

    # Condensate calculations
    if "Condensate_ontime" in df.columns:
        cycle_length = site_info.loc[site_info["site"]
                                     == site, "condensate_cycle_length"].iloc[0]
        oz_per_tip = site_info.loc[site_info["site"]
                                   == site, "condensate_oz_per_tip"].iloc[0]

        df["Condensate_oz"] = df["Condensate_ontime"].diff().shift(-1).apply(
            lambda x: x / cycle_length * oz_per_tip if x else x)
    elif "Condensate_pulse_avg" in df.columns:
        oz_per_tip = site_info.loc[site_info["site"]
                                   == site, "condensate_oz_per_tip"].iloc[0]

        df["Condensate_oz"] = df["Condensate_pulse_avg"].apply(
            lambda x: x * oz_per_tip)

    # Get instantaneous energy from condensation
    if "Condensate_oz" in df.columns:
        df["Condensate_kJ"] = df["Condensate_oz"].apply(
            lambda x: x * oz_2_m3 * water_density * water_latent_vaporization / 1000)
        df = df.drop(columns=["Condensate_oz"])

    return df

def gas_valve_diff(df : pd.DataFrame, site : str, site_info_path : str) -> pd.DataFrame:
    """
    Function takes in the site df, the site name, and path to the site_info file. If the site has
    gas heating, take the lagged difference to get per minute values. 
    Input: Dataframe for site, site name as string, path to site_info.csv as string
    Output: Pandas Dataframe 
    """
    try:
        site_info = pd.read_csv(site_info_path)
    except FileNotFoundError:
        print("File Not Found: ", site_info_path)
        return df
    
    specific_site_info = site_info.loc[site_info["site"] == site]
    if specific_site_info["heating_type"] == "gas":
        if ("gasvalve" in df.columns):
            df["gasvalve"] = df["gasvalve"] - df["gasvalve"].shift(1)
        elif (("gasvalve_lowstage" in df.columns) and ("gasvalve_highstage" in df.columns)):
            df["gasvalve_lowstage"] = df["gasvalve_lowstage"] - df["gasvalve_lowstage"].shift(1)
            df["gasvalve_highstage"] = df["gasvalve_highstage"] - df["gasvalve_highstage"].shift(1)
        
    return df