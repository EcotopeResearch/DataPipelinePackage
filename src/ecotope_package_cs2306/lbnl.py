import pandas as pd
import numpy as np
import pytz
import re
from typing import List
import datetime as dt
from sklearn.linear_model import LinearRegression
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

#.apply helper function for get_refrig_charge, calculates w/superheat method when metering = orifice
#NOTE: Needs various CSV files
def _superheat(row):
    #reference superheat.R in this construction, it's very complicated
    pass

#.apply helper function for get_refrig_charge, calculates w/subcooling method when metering = txv
def _subcooling(row, lr_model):
    #linear regression model gets passed in, we use it to calculate sat_temp_f, then take difference

    #NOTE: linear regression model is just m and b, right? so I'm calcing y, we do this,
    #assuming that lr_model is a tuple w/m at 0 and b at 1. 
    sat_temp_f = lr_model(0)*row.loc["Pressure_LL_psi"] + lr_model(1)

    difference = sat_temp_f - row.loc["Temp_LL_F"]
    row.loc["Refrig_charge"] = difference

#NOTE: This function needs a THREE external csv files, do I really want them all in the parameter?
def get_refrig_charge(df : pd.DataFrame, site : str, site_info_path : str, four_path : str) -> pd.DataFrame:
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

    #NOTE: .apply on every row once the metering device has been determined. different calcs for each!
    if(metering_device == "txv"):
        #calculate the refrigerant charge w/the subcooling method

        four_df = pd.read_csv(four_path)
        #store pressure column in a list, and temp column in a list
        pressure_list = four_df["pressure"].values.tolist()
        temp_list = four_df["temp"].values.tolist()

        #NOTE: Train a linear regression model HERE w/pressure and temp lists from 410a_pt.csv, pass into .apply
        lr_model = ("m", "b") #m and b as in mx + b

        df.apply(_subcooling, args=(lr_model,))
    else:
        #calculate the refrigerant charge w/the superheat method
        #NOTE: Think about what needs to be done OUTSIDE of the loops. A model maybe?

        #according to Madison, you convert RAT (return air temp) to celsius, calculate wet bulb, and assign xrange 
        #BEFORE you start looping through everything. I think you do all that in superheat!

        #helper call here
        df.apply(_superheat)
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

# TODO: update this function from using a passed in date to using date from last row
def aqsuite_filter_new(last_date: str, filenames: List[str]) -> List[str]:
    """
    Function filters the filenames list to only those newer than the last date.
    Input: Latest date, List of filenames to be filtered
    Output: Filtered list of filenames
    """
    last_date = dt.datetime.strptime(last_date, '%Y-%m-%d')
    return list(filter(lambda filename: dt.datetime.strptime(filename[-18:-8], '%Y-%m-%d') >= last_date, filenames))

def aqsuite_csv_to_df(csv_filenames: List[str]) -> pd.DataFrame:
    """
    Function takes a list of csv filenames containing aqsuite data and reads all files into a singular dataframe.
    Input: List of filenames 
    Output: Pandas Dataframe containing data from all files
    """
    temp_dfs = []
    for filename in csv_filenames:
        try:
            data = pd.read_csv(filename)
        except FileNotFoundError:
            print("File Not Found: ", filename)
            return
        
        if len(data) != 0:
            data = add_date(data, filename)
            temp_dfs.append(data)
    df = pd.concat(temp_dfs, ignore_index=False)
    return df

def add_date(df : pd.DataFrame, filename : str) -> pd.DataFrame:
    """
    Some LBNL data files do not contain the date in the time column. This function extracts the date
    from the filename and adds it to the data.
    Input: Dataframe, filename as string
    Output: Modified dataframe
    """
    date = filename[-18:-8]
    df['time'] = df.apply(lambda row : date + " " + str(row['time']), axis = 1)
    return df

def replace_humidity(df: pd.DataFrame, od_conditions: pd.DataFrame, date_forward, site_name: str) -> pd.DataFrame:
    df.loc[df.index > date_forward, "Humidity_ODRH"] = np.nan
    data_old = df["Humidity_ODRH"]

    data_new = od_conditions.loc[od_conditions.index > date_forward]  # .astimezone(pytz.timezone('UTC'))]
    data_new = data_new[f"{site_name}_ODRH"]

    df["Humidity_ODRH"] = data_old.fillna(value=data_new)

    return df

def create_fan_curves(cfm_info, site_info):
    # Make a copy of the dataframes to avoid modifying the original data
    cfm_info = cfm_info.copy()
    site_info = site_info.copy()

    # Convert furnace power from kW to W
    site_info['furn_misc_power'] *= 1000

    # Calculate furnace power to remove for each row
    def calculate_watts_to_remove(row):
        if np.isnan(row['ID_blower_rms_watts']) or 'heat' not in row['mode']:
            return 0
        site_row = site_info.loc[site_info['site'] == row['site']]
        return site_row['furn_misc_power'].values[0]

    cfm_info['watts_to_remove'] = cfm_info.apply(calculate_watts_to_remove, axis=1)

    # Subtract furnace power from blower power
    mask = cfm_info['watts_to_remove'] != 0
    cfm_info.loc[mask, 'ID_blower_rms_watts'] -= cfm_info['watts_to_remove']

    # Group by site and estimate coefficients
    by_site = cfm_info.groupby('site')
    def estimate_coefficients(group):
        X = group[['ID_blower_rms_watts']].values ** 0.3333 - 1
        y = group['ID_blower_cfm'].values
        return pd.Series(LinearRegression().fit(X, y).coef_)

    fan_coeffs = by_site.apply(estimate_coefficients)
    fan_coeffs.columns = ['a', 'b']

    return fan_coeffs

def get_cfm_values(df, site):  
    site_cfm = pd.read_csv("sitecfminfo.csv", encoding='unicode_escape')
    site_cfm = site_cfm[site_cfm["site"] == site]
    site_cfm = site_cfm.set_index(["site"])

    cfm_info = dict()
    cfm_info["circ"] = [site_cfm["ID_blower_cfm"].iloc[i] for i in range(len(site_cfm.index)) if bool(re.search(".*circ.*", site_cfm["mode"].iloc[i]))][0]
    cfm_info["heat"] = [site_cfm["ID_blower_cfm"].iloc[i] for i in range(len(site_cfm.index)) if bool(re.search(".*heat.*", site_cfm["mode"].iloc[i]))][0]
    cfm_info["cool"] = [site_cfm["ID_blower_cfm"].iloc[i] for i in range(len(site_cfm.index)) if bool(re.search(".*cool.*", site_cfm["mode"].iloc[i]))][0]

    df["Cfm_Calc"] = [cfm_info[state] if state in cfm_info.keys() else 0.0 for state in df["HVAC"]]

    return df

def get_cop_values(df: pd.DataFrame, site_air_corr: pd.DataFrame, site: str):
    w_to_btuh = 3.412
    btuh_to_w = 1 / w_to_btuh
    air_density = 1.08

    air_corr = site_air_corr.loc[site_air_corr['site'] == site, 'air_corr']

    df = df.assign(Power_Output_BTUh=np.select([df['HVAC_state'] == "heat", df['HVAC_state'] == "circ"], [0, 0],
    default=(df['Temp_SATAvg'] - df['Temp_RAT']) * df['Cfm_Calc'] * air_density * air_corr)).assign(Power_Output_kW = df['Power_Output_BTUh'] * btuh_to_w / 1000)
                                                                                                                               
    df["cop"] = abs(df['Power_Output_kW'] / df["Power_system1"])
    df = df.drop(columns=['Power_Output_BTUh'], axis=1)

    return df
