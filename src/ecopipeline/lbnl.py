import pandas as pd
import numpy as np
import math
import pytz
import re
from typing import List
import datetime as dt
from sklearn.linear_model import LinearRegression
from statsmodels.formula.api import ols
from ecopipeline.config import configure
import os


def site_specific(df: pd.DataFrame, site: str) -> pd.DataFrame:
    """
    Does Site Specific Calculations for LBNL. The site name is searched using RegEx
    Args: 
        df (pd.DataFrame): dataframe of data 
        site (str): site name as a string
    Output: 
        pd.DataFrame: modified dataframe
    """
    # Bob's site notes says add 55 Pa to the Pressure
    if re.search("MO2_", site):
        df["Pressure_staticP"] += 55

    # Calculate Power vars
    # All MO & IL sites.
    if re.search("(AZ2_01|AZ2_02|MO2_|IL2_|NW2_01)", site):
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
        df["Pressure_staticP"] = df["Pressure_staticPa"] + \
            (inWC_2_Pa * df["Pressure_staticInWC"])
        return df

    return df


def lbnl_temperature_conversions(df: pd.DataFrame) -> pd.DataFrame:
    if "Temp_LL_C" in df.columns:
        df["Temp_LL_F"] = (9/5)*df["Temp_LL_C"] + 32

    if "Temp_SL_C" in df.columns:
        df["Temp_SL_F"] = (9/5)*df["Temp_SL_C"] + 32

    return df


def condensate_calculations(df: pd.DataFrame, site: str) -> pd.DataFrame:
    """
    Calculates condensate values for the given dataframe

    Args:
        df (pd.DataFrame): dataframe to be modified
        site (str): name of site
    Returns:
        pd.DataFrame: modified dataframe
    """
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


def gas_valve_diff(df: pd.DataFrame, site: str, site_info_path: str) -> pd.DataFrame:
    """
    Function takes in the site dataframe, the site name, and path to the site_info file. If 
    the site has gas heating, take the lagged difference to get per minute values. 
    
    Args: 
        df (pd.DataFrame): Dataframe for site
        site (str): site name as string
        site_info_path (str): path to site_info.csv as string
    Returns: 
        pd.DataFrame: new Pandas Dataframe 
    """
    try:
        site_info = pd.read_csv(site_info_path)
    except FileNotFoundError:
        print("File Not Found: ", site_info_path)
        return df

    specific_site_info = site_info.loc[site_info["site"] == site]
    if (specific_site_info["heating_type"] == "gas").all():
        if ("gasvalve" in df.columns):
            df["gasvalve"] = df["gasvalve"] - df["gasvalve"].shift(1)
        elif (("gasvalve_lowstage" in df.columns) and ("gasvalve_highstage" in df.columns)):
            df["gasvalve_lowstage"] = df["gasvalve_lowstage"] - \
                df["gasvalve_lowstage"].shift(1)
            df["gasvalve_highstage"] = df["gasvalve_highstage"] - \
                df["gasvalve_highstage"].shift(1)

    return df


# .apply helper function for get_refrig_charge, calculates w/subcooling method when metering = txv
def _subcooling(row, lr_model):
    """
    Function takes in a Pandas series and a linear regression model, calculates 
    Refrig_charge for the Pandas series with that model, then inserts it into the series and returns it. 
    
    Args: 
        row (pd.Series): Pandas series
        lr_model (sklearn.linear_model.Fit): Linear regression model
    Returns: 
        row (pd.Series): Pandas series (Refrig_charge added!)
    """
    # linear regression model gets passed in, we use it to calculate sat_temp_f, then take difference
    x = row.loc["Pressure_LL_psi"]
    m = lr_model.coef_
    b = lr_model.intercept_
    sat_temp_f = m*x+b
    #convert old temp to F
    temp_f = (row.loc["Temp_LL_C"]*(9/5)) + 32

    r_charge = sat_temp_f - temp_f
    row.loc["Refrig_charge"] = r_charge[0]
    return row

# .apply helper function for get_refrig_charge, calculates w/superheat method when metering = orifice
def _superheat(row, x_range, row_range, superchart, lr_model):
    """
    Function takes in a Pandas series, ranges from a csv, and a linear regression model 
    in order to calculate Refrig_charge for the given row through linear interpolation. 

    Args: 
        row (pd.Series): Pandas series
        x_range (<class 'list'>): List of ints
        row_range (<class 'list'>): List of ints
        superchart (pd.Dataframe): Pandas dataframe, big grid of ints
        lr_model (sklearn.linear_model.Fit): Linear regression model
    Returns: 
        row (pd.Series): Pandas series (Refrig_charge added!)
    """
    superheat_target = None

    #TODO: IF Temp_ODT, Temp_RAT, Humidity_RARH, Pressure_LL_psi, or Temp_SL_C
    # is null, just return the row early (refrig_charge will be none!)
    if(row.loc["Temp_ODT"] == np.NaN or row.loc["Temp_RAT"] == np.NaN or row.loc["Humidity_RARH"] == np.NaN or row.loc["Pressure_LL_psi"] == np.NaN or row.loc["Temp_SL_C"] == np.NaN):
        return row

    #Convert F to C return air temperature
    RAT_C = (row.loc["Temp_RAT"] - 32) * (5/9)
    rh = row.loc["Humidity_RARH"]

    #calculate wet bulb temp w/humidity and air temp, then convert back to F
    Temp_wb_C = RAT_C * math.atan(0.151977(rh + 8.31659)**(1/2)) + math.atan(RAT_C + rh) - math.atan(rh - 1.676331) + 0.00391838(rh)**(3/2) * math.atan(0.023101*rh) - 4.686035
    Temp_wb_F = (Temp_wb_C * (9/5)) + 32
    Temp_ODT = row.loc['Temp_ODT']

    #NA checks, elif bound check, else interpolations
    if math.isnan(Temp_ODT or math.isnan(Temp_wb_F)):
        #filtering out na's in recorded data
        superheat_target = None
    elif(Temp_ODT > max(row_range) or Temp_ODT < min(row_range) or Temp_wb_F > max(x_range) or Temp_wb_F < min(x_range)):
        superheat_target = None
    else:
        #row_range exists so this can have yrange
        y_max = math.ceil(Temp_ODT/5) * 5
        y_min = math.floor(Temp_ODT/5) * 5
        y_range = [y_min, y_max]

        table_v1 = np.interp(Temp_wb_F, x_range, superchart.loc[str(y_min)])
        if(y_max == y_min):
            superheat_target = table_v1 
        else: 
            table_v2 = np.interp(Temp_wb_F, x_range, superchart.loc[str(max)])
            xvalue_range3 = [table_v1, table_v2]
            if(any(np.isnan(xvalue_range3))):
                superheat_target = None
            else:
                superheat_target = np.interp(Temp_ODT, y_range, xvalue_range3)

    #finding superheat_calc
    sat_temp_f = lr_model.coef_*row.loc["Pressure_LL_psi"]+lr_model.intercept_
    Temp_SL_F = (row.loc["Temp_SL_C"])*(9/5) + 32
    superheat_calc = Temp_SL_F - sat_temp_f

    #now that we have superheat_calc and superheat_target, we calc
    #refrigerant charge and add it back to the series.
    r_charge = superheat_calc - superheat_target 
    row.loc["Refrig_charge"] = r_charge[0]
    return row

# NOTE: This function needs a THREE external csv files, do I really want them all in the parameter?
def get_refrig_charge(df: pd.DataFrame, site: str) -> pd.DataFrame:
    """
    Function takes in a site dataframe, its site name as a string, the path to site_info.csv as a string, 
    the path to superheat.csv as a string, and the path to 410a_pt.csv, and calculates the refrigerant 
    charge per minute? 
    
    Args: 
        df (pd.DataFrame): Pandas Dataframe
        site (str): site name as a string 
        site_info_path (str): path to site_info.csv as a string
        four_path (str) path to 410a_pt.csv as a string
        superheat_path (str): path to superheat.csv as a string
    Returns: 
        pd.DataFrame: modified Pandas Dataframe
    """
    #configs
    config = configure.get('input')
    site_info_directory = f"{config['directory']}{config['site_info']}"
    four_directory = f"{config['directory']}{config['410a_info']}"
    superheat_directory = f"{config['directory']}{config['superheat_info']}"


    #if DF empty, return the df as is
    if(df.empty):
        return df

    site_df = pd.read_csv(site_info_directory, index_col=0)
    metering_device = site_df.at[site, "metering_device"]

    #NOTE: this specific lr_model is needed for both superheat AND subcooling!
    four_df = pd.read_csv(four_directory)
    X = np.array(four_df["pressure"].values.tolist()).reshape((-1, 1))
    y = np.array(four_df["temp"].values.tolist())
    lr_model = LinearRegression().fit(X, y)

    #Creating Refrig_charge column populated w/None
    df["Refrig_charge"] = None

    # .apply on every row once the metering device has been determined. different calcs for each!
    if (metering_device == "txv"):
        #calculate the refrigerant charge w/the subcooling method
        df = df.apply(_subcooling, axis=1, args=(lr_model,))
    else:
        # calculate the refrigerant charge w/the superheat method
        superchart = pd.read_csv(superheat_directory)
        x_range = superchart.columns.values.tolist()
        row_range = superchart.iloc[:,0].tolist()
        #ignore first element and we have our range from the col names
        x_range.pop(0) 

        df = df.apply(_superheat, axis=1, args=(x_range, row_range, superchart, lr_model))

    return df


def gather_outdoor_conditions(df: pd.DataFrame, site: str) -> pd.DataFrame:
    """
    Function takes in a site dataframe and site name as a string. Returns a new dataframe
    that contains time_utc, <site>_ODT, and <site>_ODRH for the site.
    
    Args: 
        df (pd.DataFrame): Pandas Dataframe
        site (str): site name as string
    Returns: 
        pd.DataFrame: new Pandas Dataframe
    """
    if (not df.empty):
      if ("Power_OD_total1" in df.columns):
        odc_df = df[["time_utc", "Temp_ODT", "Humidity_ODRH", "Power_OD_total1"]].copy()
        odc_df.rename(columns={"Power_OD_total1": "Power_OD"}, inplace=True)
      else:
        odc_df = df[["time_utc", "Temp_ODT", "Humidity_ODRH", "Power_DHP"]].copy()
        odc_df.rename(columns={"Power_DHP": "Power_OD"}, inplace=True)

      odc_df = odc_df.loc[odc_df["Power_OD"] > 0.01] 
      odc_df.drop("Power_OD", axis=1, inplace=True)
      odc_df.rename(columns={"Temp_ODT": site + "_ODT", "Humidity_ODRH": site + "_ODRH"}, inplace=True)
      return odc_df
    else:
        return df
    

def change_ID_to_HVAC(df: pd.DataFrame, site: str) -> pd.DataFrame:
    """
    Function takes in a site dataframe along with the name and path of the site and assigns
    a unique event_ID value whenever the system changes state.
    
    Args: 
        df (pd.DataFrame): Pandas Dataframe
        site (str): site name as a string 
        site_info_path (str): path to site_info.csv as a string
    Returns: 
        pd.DataFrame: modified Pandas Dataframe
    """
    
    if ("Power_FURN1" in list(df.columns)):
            df.rename(columns={"Power_FURN1": "Power_AH1"})
    site_info_directory = configure.get('site_info', 'directory')
    site_info = pd.read_csv(site_info_directory)
    site_section = site_info[site_info["site"] == site]
    statePowerAHThreshold = pd.to_numeric(site_section["AH_standby_power"]) * 1.5
    df["event_ID"] = np.NAN
    df["event_ID"] = df["event_ID"].mask(df["Power_AH1"] > statePowerAHThreshold, 1)
    event_ID = 1

    for i in range(1,len(df.index)):
        if((math.isnan(df["event_ID"][i]) == False) and (df["event_ID"][i] == 1.0)):
            time_diff = (df["time"][i] - df["time"][i-1])
            diff_minutes = time_diff.total_seconds() / 60
            if(diff_minutes > 10):
                event_ID += 1
        elif (math.isnan(df["event_ID"][i])):
            if(math.isnan(df["event_ID"][i - 1]) == False):
                event_ID += 1
        df["event_ID"][i] = event_ID
    return df

# TODO: update this function from using a passed in date to using date from last row
def nclarity_filter_new(last_date: str, filenames: List[str]) -> List[str]:
    """
    Function filters the filenames list to only those newer than the last date.
    
    Args: 
        last_date (str): latest date loaded prior to current runtime
        filenames (List[str]): List of filenames to be filtered
    Returns: 
        List[str]: Filtered list of filenames
    """
    last_date = dt.datetime.strptime(last_date, '%Y-%m-%d')
    return list(filter(lambda filename: dt.datetime.strptime(filename[-18:-8], '%Y-%m-%d') >= last_date, filenames))


def nclarity_csv_to_df(csv_filenames: List[str]) -> pd.DataFrame:
    """
    Function takes a list of csv filenames containing nclarity data and reads all files into a singular dataframe.
    Args: 
        csv_filenames (List[str]): List of filenames 
    Returns: 
        pd.DataFrame: Pandas Dataframe containing data from all files
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


def aqsuite_filter_new(last_date: str, filenames: List[str], site: str, prev_opened: str = 'input/previous.pkl') -> List[str]:
    """
    Function filters the filenames list to only those newer than the last date.
    Args: 
        last_date (str): latest date loaded prior to current runtime
        filenames (List[str]): List of filenames to be filtered
        site (str): site name
        prev_opened (str): pkl directory for previously opened files
    Returns: 
        List[str]: Filtered list of filenames
    """
    # Opens the df that contains the dictionary of what each file is
    if os.path.exists(prev_opened):
        # Columns are site, filename, start datetime, end datetime
        prev_df = pd.read_pickle(prev_opened)
    else:
        prev_df = pd.DataFrame(
            columns=['site', 'filename', 'start_datetime', 'end_datetime'])
        prev_df[['start_datetime', 'end_datetime']] = prev_df[[
            'start_datetime', 'end_datetime']].apply(pd.to_datetime)

    # Filter files by what has not been opened
    prev_filename_set = set(prev_df['filename'])
    new_filenames = [
        filename for filename in filenames if filename not in prev_filename_set]

    # Add files to prev_df
    for filename in new_filenames:
        data = pd.read_csv(filename)
        data["time(UTC)"] = pd.to_datetime(data["time(UTC)"])
        max_date = data["time(UTC)"].max()
        min_date = data["time(UTC)"].min()
        new_entry = {'site': site, 'filename': filename,
                     'start_datetime': min_date, 'end_datetime': max_date}
        prev_df = pd.concat([prev_df, pd.DataFrame(
            new_entry, index=[0])], ignore_index=True)

    # Save new prev_df
    prev_df.to_pickle(prev_opened)

    # List all files with the date equal or newer than last_date
    last_date = dt.datetime.strptime(last_date, '%Y-%m-%d')
    filtered_prev_df = prev_df[(['site'] == site) & (
        prev_df['start_datetime'] >= last_date)]
    filtered_filenames = filtered_prev_df['filename'].tolist()

    return filtered_filenames



def add_date(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    LBNL's nclarity files do not contain the date in the time column. This
    function extracts the date from the filename and adds it to the data.

    Args: 
        df (pd.DataFrame): Dataframe
        filename (str): filename as string
    Returns:
        pd.DataFrame: Modified dataframe
    """
    date = filename[-18:-8]
    df['time'] = df.apply(lambda row: date + " " + str(row['time']), axis=1)
    return df


def elev_correction(site_info_file : str, site_name : str) -> pd.DataFrame:
    """
    Function creates a dataframe for a given site that contains site name, elevation, 
    and the corrected elevation.

    Args: 
        site_info_file (str): Path to site_info.csv file 
        site_name (str): site's name
    Returns: 
        pd.DataFrame: new Pandas dataframe
    """
    try:
        site_info_df = pd.read_csv(site_info_file)
    except FileNotFoundError:
        print("File Not Found: ", site_info_file)
        return
    
    site_info_df = site_info_df.loc[site_info_df['site'] == site_name]

    elev_ft = [0,1000,2000,3000,4000,5000,6000,7000,8000,9000,10000,11000,12000]
    alt_corr_fact = [1,0.97,0.93,0.89,0.87,0.84,0.80,0.77,0.75,0.72,0.69,0.66,0.63]
    cf_df = pd.DataFrame({'elev_ft': elev_ft, 'alt_corr_fact': alt_corr_fact})

    lin_model = ols(y = cf_df['alt_corr_fact'], x = cf_df['elev_ft'])

    elv_df = site_info_df[['elev']].rename(columns={'elev': 'elev_ft'}).fillna(0)
    air_corr = {'air_corr': lin_model.predict(exog=elv_df)}

    site_air_corr = site_info_df[['site', 'elev']].assign(air_corr=lambda df: np.where(
                            df['elev'].isna() | df['elev'] < 1000, 1, air_corr['air_corr']))
  
    return site_air_corr


def replace_humidity(df: pd.DataFrame, od_conditions: pd.DataFrame, date_forward: dt.datetime, site_name: str) -> pd.DataFrame:
    """
    Function replaces all humidity readings for a given site after a given datetime. 

    Args:
        df (pd.DataFrame): Dataframe containing the raw sensor data.
        od_conditions (pd.DataFrame): DataFrame containing outdoor confitions measured by field sensors.
        date_forward (dt.datetime): Datetime containing the time after which all humidity readings should be replaced.
        site_name (str): String containing the name of the site for which humidity values are to be replaced.
    Returns:
        pd.DataFrame: Modified DataFrame where the Humidity_ODRH column contains the field readings after the given datetime. 
    """
    df.loc[df.index > date_forward, "Humidity_ODRH"] = np.nan
    data_old = df["Humidity_ODRH"]

    # .astimezone(pytz.timezone('UTC'))]
    data_new = od_conditions.loc[od_conditions.index > date_forward]
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

    cfm_info['watts_to_remove'] = cfm_info.apply(
        calculate_watts_to_remove, axis=1)

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


def get_cfm_values(df: pd.DataFrame, site_cfm: pd.DataFrame, site_info: pd.DataFrame, fan_coefficients: pd.DataFrame, site: str):
    """
    Function calculates the volume of air that moves through a space per minute measures in 
    cubic feet per minute (CFM). 

    Args:
        df (pd.DataFrame): Dataframe containing the raw sensor data.
        site_cfm (pd.DataFrame): Configuration file containing site-specific cfm information.
        site_info (pd.DataFrame): Configuration file containing site-specific information.
        fan_coefficients (pd.DataFrame): DataFrame containing fan coefficient values. 
        site (str): String containing the name of the site for which cfm values are to be calculated. 
    Returns: 
        pd.DataFrame: Modified DataFrame with a column named Cfm_Calc.
    """
    site_cfm = site_cfm[site_cfm.index == site]
    fan_curve = True if site_cfm.iloc[0]["use_fan_curve"] == "TRUE" else False

    if not fan_curve:
        cfm_info = dict()
        cfm_info["circ"] = [site_cfm["ID_blower_cfm"].iloc[i] for i in range(
            len(site_cfm.index)) if bool(re.search(".*circ.*", site_cfm["mode"].iloc[i]))][0]
        cfm_info["heat"] = [site_cfm["ID_blower_cfm"].iloc[i] for i in range(
            len(site_cfm.index)) if bool(re.search(".*heat.*", site_cfm["mode"].iloc[i]))][0]
        cfm_info["cool"] = [site_cfm["ID_blower_cfm"].iloc[i] for i in range(
            len(site_cfm.index)) if bool(re.search(".*cool.*", site_cfm["mode"].iloc[i]))][0]

        df["Cfm_Calc"] = [cfm_info[state] if state in cfm_info.keys() else 0.0 for state in df["HVAC"]]

    else:
        heat_in_HVAC = "heat" in list(df["HVAC"])

        cfm_temp = df["Power_AH1"]
        if heat_in_HVAC:
            furn_misc_power = site_info.loc[site, "furn_misc_power"]
            furn_misc_power = 0.0 if furn_misc_power == np.nan else furn_misc_power
            cfm_temp = cfm_temp - np.full(len(df["Power_AH1"]), furn_misc_power)

        cfm_temp = (cfm_temp * 1000) ** (1/3)
        df["Cfm_Calc"] = cfm_temp

    return df


def get_cop_values(df: pd.DataFrame, site_air_corr: pd.DataFrame, site: str):
    w_to_btuh = 3.412
    btuh_to_w = 1 / w_to_btuh
    air_density = 1.08

    air_corr = site_air_corr.loc[site_air_corr['site'] == site, 'air_corr']

    df = df.assign(Power_Output_BTUh=np.select([df['HVAC_state'] == "heat", df['HVAC_state'] == "circ"], [0, 0],
                                               default=(df['Temp_SATAvg'] - df['Temp_RAT']) * df['Cfm_Calc'] * air_density * air_corr)).assign(Power_Output_kW=df['Power_Output_BTUh'] * btuh_to_w / 1000)

    df["cop"] = abs(df['Power_Output_kW'] / df["Power_system1"])
    df = df.drop(columns=['Power_Output_BTUh'], axis=1)

    return df

