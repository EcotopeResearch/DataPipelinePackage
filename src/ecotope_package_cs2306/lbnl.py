import pandas as pd
import re
from typing import List
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
    Function takes in the site dataframe, the site name, and path to the site_info file. If the site has
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

#NOTE: This function needs a THREE external csv files, do I really want them all in the parameter?
def get_refrig_charge(df : pd.DataFrame, site : str, site_info_path : str) -> pd.DataFrame:
    """
    Function takes in a site dataframe, its site name as a string, the path to site_info.csv as a string, 
    the path to superheat.csv as a string, and the path to 410a_pt.csv, and calculates the refrigerant 
    charge per minute? 
    Input: Pandas Dataframe, site name as a string, path to site_info.csv as a string, path to superheat.csv 
    as a string, and the path to 410a_pt.csv as a string. 
    Output: Pandas Dataframe
    """
    #Step 1: Extract 'metering_device' from site_info_path, making sure to grab from the row matching
    #the site name! Assuming site name is in format "AZ2_01", or this will break. 
    site_df = pd.read_csv(site_info_path)
    metering_device = site_df.loc[site, "metering_device"]

    #NOTE: loop through every minute once metering_device is filtered. it seems oddly done in R,
    #but you need to do this calculation for every row. bruh.
    if(metering_device == "txv"):
        #calculate the refrigerant charge w/the subcooling method (the easy way)

        #NOTE: Potentially just call a helper on the whole df and have it apply by row?


        #start by calculating sat_temp_f by linear interpolation w/410a_pt.csv
        #grab 'pressure' and 'temp' from 410a_pt.csv at the current index, df.loc['Pressure_LL_psi']
        sat_temp_f = ""

        #we need some index to replace 0? this is gonna be weird w/apply
        refrig_charge = sat_temp_f - df.loc[0, "Temp_LL_F"]
        #add refrig_charge to the df at the proper index
        pass
    else:
        #calculate the refrigerant charge w/the superheat method (uh oh)

        #NOTE: Potentially just call a helper on the whole df and have it apply by row?
        pass

    return df

def gather_outdoor_conditions(df : pd.DataFrame, site : str) -> pd.DataFrame:
    """
    Function takes in a site dataframe and site name as a string. Returns a new dataframe
    that contains time_utc, <site>_ODT, and <site>_ODRH for the site.
    Input: Pandas Dataframe, site name as string
    Output: Pandas Dataframe
    """
    if ("Power_OD_total1" in df.columns):
        odc_df = df[["time_utc", "Temp_ODT", "Humidity_ODRH", "Power_OD_total1"]].copy()
        odc_df.rename(columns={"Power_OD_total1":"Power_OD"}, inplace=True)
    else:
        odc_df = df[["time_utc", "Temp_ODT", "Humidity_ODRH", "Power_DHP"]].copy()
        odc_df.rename(columns={"Power_DHP":"Power_OD"}, inplace=True)
    
    odc_df = odc_df[odc_df["Power_OD"] > 0.01] 
    odc_df.drop("Power_OD", axis=1, inplace=True)
    odc_df.rename(columns={"Temp_ODT": site + "_ODT", "Humidity_ODRH": site + "_ODRH"}, inplace=True)
    return odc_df


def lbnl_extract_new(last_date: str, filenames: List[str]) -> List[str]:
    """
    Function filters the filenames to only those newer than the last date.
    Input: Latest date, List of filenames to be filtered
    Output: Filtered list of filenames
    """
    time_int = int(last_date.strptime("%Y-%m-%d"))
    return list(filter(lambda filename: int(filename[7:-8]) >= time_int, filenames))