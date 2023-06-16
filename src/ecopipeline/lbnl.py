import pandas as pd
import numpy as np
import math
import pytz
import re
from typing import List
import datetime as dt
from sklearn.linear_model import LinearRegression
from ecopipeline.config import configure
from ecopipeline.config import _input_directory, _output_directory
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
        print(df["Power_OD_total1"])
        print(df["Power_OD_fan1"])
        print(df["Power_OD_total1"] - df["Power_OD_fan1"])
        df["Power_OD_compressor1"] = df["Power_OD_total1"] - df["Power_OD_fan1"]
        df["Power_OD_compressor1"] = df["Power_OD_compressor1"].clip(lower=0)

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



def condensate_calculations(df: pd.DataFrame, site: str, site_info: pd.Series) -> pd.DataFrame:
    """
    Calculates condensate values for the given dataframe
    Args:
        df (pd.DataFrame): dataframe to be modified
        site (str): name of site
        site_info (pd.Series): Series of site info
    Returns:
        pd.DataFrame: modified dataframe
    """
    oz_2_m3 = 1 / 33810  # [m3/oz]
    water_density = 997  # [kg/m³]
    water_latent_vaporization = 2264.705  # [kJ/kg]

    # Condensate calculations
    if "Condensate_ontime" in df.columns:
        cycle_length = site_info["condensate_cycle_length"]
        oz_per_tip = site_info["condensate_oz_per_tip"]

        df["Condensate_oz"] = df["Condensate_ontime"].diff().shift(-1).apply(
            lambda x: x / cycle_length * oz_per_tip if x else x)
    elif "Condensate_pulse_avg" in df.columns:
        oz_per_tip = site_info["condensate_oz_per_tip"]

        df["Condensate_oz"] = df["Condensate_pulse_avg"].apply(
            lambda x: x * oz_per_tip)

    # Get instantaneous energy from condensation
    if "Condensate_oz" in df.columns:
        df["Condensate_kJ"] = df["Condensate_oz"].apply(
            lambda x: x * oz_2_m3 * water_density * water_latent_vaporization / 1000)
        df = df.drop(columns=["Condensate_oz"])

    return df


def gas_valve_diff(df: pd.DataFrame, site: str) -> pd.DataFrame:
    """
    Function takes in the site dataframe and the site name. If the site has gas 
    heating, take the lagged difference to get per minute values. 
    
    Args: 
        df (pd.DataFrame): Dataframe for site
        site (str): site name as string
    Returns: 
        pd.DataFrame: modified Pandas Dataframe 
    """
    input_dir = configure.get('input', 'directory')
    site_info_path = input_dir + configure.get('input', 'site_info')

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
    x = row.loc["Pressure_SL_psi"]
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
    superheat_target = np.NaN

    #IF Temp_ODT, Temp_RAT, Humidity_RARH, Pressure_SL_psi, or Temp_SL_C
    # is null, just return the row early. 
    if(row.loc["Temp_ODT"] == np.NaN or row.loc["Temp_RAT"] == np.NaN or row.loc["Humidity_RARH"] == np.NaN or row.loc["Pressure_SL_psi"] == np.NaN or row.loc["Temp_SL_C"] == np.NaN):
        return row

    #Convert F to C return air temperature
    RAT_C = (row.loc["Temp_RAT"] - 32) * (5/9)
    rh = row.loc["Humidity_RARH"]

    #calculate wet bulb temp w/humidity and air temp, then convert back to F
    Temp_wb_C = RAT_C * math.atan(0.151977*(rh + 8.31659)**(1/2)) + math.atan(RAT_C + rh) - math.atan(rh - 1.676331) + 0.00391838*(rh)**(3/2) * math.atan(0.023101*rh) - 4.686035
    Temp_wb_F = (Temp_wb_C * (9/5)) + 32
    Temp_ODT = row.loc['Temp_ODT']

    #NA checks, elif bound check, else interpolations
    if math.isnan(Temp_ODT or math.isnan(Temp_wb_F)):
        #filtering out na's in recorded data
        superheat_target = np.NaN
    elif(Temp_ODT > max(row_range) or Temp_ODT < min(row_range) or Temp_wb_F > max(x_range) or Temp_wb_F < min(x_range)):
        superheat_target = np.NaN
    else:
        #row_range exists so this can have yrange
        y_max = math.ceil(Temp_ODT/5) * 5
        y_min = math.floor(Temp_ODT/5) * 5
        y_range = [y_min, y_max]

        table_v1 = np.interp(Temp_wb_F, x_range, superchart.loc[y_min])
        if(y_max == y_min):
            superheat_target = table_v1 
        else: 
            table_v2 = np.interp(Temp_wb_F, x_range, superchart.loc[y_max])
            xvalue_range3 = [table_v1, table_v2]
            if(any(np.isnan(xvalue_range3))):
                superheat_target = None
            else:
                superheat_target = np.interp(Temp_ODT, y_range, xvalue_range3)

    #finding superheat_calc
    sat_temp_f = lr_model.coef_*row.loc["Pressure_SL_psi"]+lr_model.intercept_
    Temp_SL_F = (row.loc["Temp_SL_C"])*(9/5) + 32
    superheat_calc = Temp_SL_F - sat_temp_f

    #now that we have superheat_calc and superheat_target, we calc
    #refrigerant charge and add it back to the series.
    r_charge = superheat_calc - superheat_target 
    row.loc["Refrig_charge"] = r_charge[0]
    return row

def get_refrig_charge(df: pd.DataFrame, site: str, site_info_directory: str = f"{_input_directory}site_info.csv", four_directory: str = f"{_input_directory}410a_pt.csv", superheat_directory: str = f"{_input_directory}superheat.csv") -> pd.DataFrame:
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
    #if DF empty, return the df as is
    if(df.empty):
        return df

    site_df = pd.read_csv(site_info_directory, index_col=0)
    metering_device = site_df.at[site, "metering_device"]

    #this specific lr_model is needed for both superheat AND subcooling!
    four_df = pd.read_csv(four_directory)
    X = np.array(four_df["pressure"].values.tolist()).reshape((-1, 1))
    y = np.array(four_df["temp"].values.tolist())
    lr_model = LinearRegression().fit(X, y)

    #Creating Refrig_charge column populated w/None
    df["Refrig_charge"] = np.NaN

    # .apply on every row once the metering device has been determined. different calcs for each!
    if (metering_device == "txv"):
        #calculate the refrigerant charge w/the subcooling method
        df = df.apply(_subcooling, axis=1, args=(lr_model,))
    elif (metering_device == "orifice"):
        #If any crucial vars for calculating refrigerant charge are missing, we return early
        var_names = df.columns.tolist() 
        if "Temp_ODT" not in var_names or "Temp_RAT" not in var_names or "Humidity_RARH" not in var_names or "Pressure_SL_psi" not in var_names or "Temp_SL_C" not in var_names:
            return df

        # calculate the refrigerant charge w/the superheat method
        superchart = pd.read_csv(superheat_directory, index_col=0)
        x_range = superchart.columns.values.tolist()
        x_range = [int(x) for x in x_range] 
        row_range = superchart.index.values.tolist()
        row_range = [int(x) for x in row_range]

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
      df = df.reset_index()
      df_temp = df.copy()
      df_temp = df_temp.loc[:,~df_temp.columns.duplicated()]
      if ("Power_OD_total1" in df_temp.columns):
        odc_df = df_temp[["time_utc", "Temp_ODT", "Humidity_ODRH", "Power_OD_total1"]]
        odc_df.rename(columns={"Power_OD_total1": "Power_OD"}, inplace=True)
      else:
        odc_df = df_temp[["time_utc", "Temp_ODT", "Humidity_ODRH", "Power_DHP"]]
        odc_df.rename(columns={"Power_DHP": "Power_OD"}, inplace=True)

      odc_df = odc_df[odc_df["Power_OD"] > 0.01] 
      odc_df.drop("Power_OD", axis=1, inplace=True)
      odc_df.rename(columns={"Temp_ODT": site + "_ODT", "Humidity_ODRH": site + "_ODRH"}, inplace=True)
      return odc_df
    else:
        return df

def get_hvac_state(df: pd.DataFrame, site_info: pd.Series) -> pd.DataFrame:
    stateGasValveThreshold = 1
    stateDTThreshold = 1.5
    statePowerODThreshold = 0.01
    stateODTThreshold = 65
    heating_type = site_info["heating_type"]
    dTavg = df[["event_ID", "Temp_ODT", "Temp_RAT", "Temp_SATAvg", "Power_AH1", "Power_OD_total1"]]
    dTavg = dTavg[dTavg["event_ID"] != 0]
    dTavg = dTavg.groupby("event_ID").agg({
    "Temp_ODT" : ["mean"], 
    "Temp_RAT" : ["mean"], 
    "Temp_SATAvg" : ["mean"], 
    "Power_AH1" : ["mean"],
    "Power_OD_total1" : ["mean"]
    })
    dTavg["dTavg"] = dTavg["Temp_SATAvg"] - dTavg["Temp_RAT"]
    dTavg["HVAC"] = "off"
    if(heating_type == "gas"):
        dTavg['HVAC'] = np.where(dTavg['Power_OD_total1'].isna(),
                                       np.where(~dTavg['dTavg'].isna(),
                                                np.where(dTavg['dTavg'] >= stateDTThreshold, "heat",
                                                         np.where(dTavg['dTavg'] <= -stateDTThreshold, "cool", "circ")),
                                                         np.where(dTavg['Temp_ODT'] > stateODTThreshold, "cool", "heat")),
                                                         np.where(dTavg['Power_OD_total1'] >= statePowerODThreshold, "cool",
                                                                  np.where(dTavg['Power_OD_total1'] <= 0.0001, "circ",
                                                                           np.where(~dTavg['dTavg'].isna(),
                                                                                    np.where(dTavg['dTavg'] >= stateDTThreshold, "heat",
                                                                                             np.where(dTavg['dTavg'] <= -stateDTThreshold, "cool", "circ")),
                                                                                             np.where(dTavg['Temp_ODT'] > stateODTThreshold, "cool", "heat"))))
                                                                                             )
    else:
        dTavg['HVAC'] = np.where(dTavg['Power_OD_total1'].isna(),
                                       np.where(dTavg['dTavg'].notna(),
                                                np.where(dTavg['dTavg'] >= stateDTThreshold, 'heat',
                                                         np.where(dTavg['dTavg'] <= -stateDTThreshold, 'cool', 'circ')),
                                                         np.where(dTavg['Temp_ODT'] > stateODTThreshold, 'cool', 'heat')),
                                                         np.where(dTavg['Power_OD_total1'] < statePowerODThreshold, 'circ',
                                                                  np.where(dTavg['dTavg'].isna(),
                                                                           np.where(dTavg['Temp_ODT'] > stateODTThreshold, 'cool', 'heat'),
                                                                           np.where(dTavg['dTavg'] >= stateDTThreshold, 'heat', 'cool')
                                                                          )
                                                                 )
                                )
    dTavg = dTavg.reset_index()
    df_merge = pd.DataFrame()
    df_merge['event_ID'] = dTavg['event_ID']
    df_merge['HVAC'] = dTavg['HVAC']
    df = df.reset_index()
    df = pd.merge(df, df_merge, on='event_ID')
    df = df.set_index('time_utc')
    return df   

def change_ID_to_HVAC(df: pd.DataFrame, site_info : pd.Series) -> pd.DataFrame:
    """
    Function takes in a site dataframe along with the name and path of the site and assigns
    a unique event_ID value whenever the system changes state.
    
    Args: 
        df (pd.DataFrame): Pandas Dataframe
        site_info (pd.Series):site_info.csv as a pd.Series
    Returns: 
        pd.DataFrame: modified Pandas Dataframe
    """
    
    if ("Power_FURN1" in list(df.columns)):
            df.rename(columns={"Power_FURN1": "Power_AH1"})
    statePowerAHThreshold = site_info['AH_standby_power'] * 1.5
    df["event_ID"] = 0
    df["event_ID"] = df["event_ID"].mask(pd.to_numeric(df["Power_AH1"]) > statePowerAHThreshold, 1)
    event_ID = 1

    for i in range(1,int(len(df.index))):
        if((df["event_ID"].iloc[i] > 0) and (df["event_ID"].iloc[i] == 1.0)):
            time_diff = (df.index[i] - df.index[i-1])
            diff_minutes = time_diff.total_seconds() / 60
            if(diff_minutes > 10):
                event_ID += 1
        elif (df["event_ID"].iloc[i] == 0):
            if(df["event_ID"].iloc[i - 1] > 0):
                event_ID += 1
        df.at[df.index[i], "event_ID"] = event_ID
    return df

# TODO: update this function from using a passed in date to using date from last row
def nclarity_filter_new(date: str, filenames: List[str]) -> List[str]:
    """
    Function filters the filenames list to only those from the given date or later.
    
    Args: 
        date (str): target date
        filenames (List[str]): List of filenames to be filtered
    Returns: 
        List[str]: Filtered list of filenames
    """
    date = dt.datetime.strptime(date, '%Y-%m-%d')
    return list(filter(lambda filename: dt.datetime.strptime(filename[-18:-8], '%Y-%m-%d') >= date, filenames))


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

        if not data.empty:
            data = _add_date(data, filename)
            temp_dfs.append(data)
    df = pd.concat(temp_dfs, ignore_index=False)
    return df

def aqsuite_prep_time(df : pd.DataFrame) -> pd.DataFrame:
    """
    Function takes an aqsuite dataframe and converts the time column into datetime type
    and sorts the entire dataframe by time.
    Prereq: 
        Input dataframe MUST be an aqsuite Dataframe whose columns have not yet been renamed
    Args: 
        df (pd.DataFrame): Aqsuite DataFrame
    Returns: 
        pd.DataFrame: Pandas Dataframe containing data from all files
    """
    df['time(UTC)'] = pd.to_datetime(df['time(UTC)'])
    df = df.sort_values(by='time(UTC)')
    return df


def aqsuite_filter_new(last_date: str, filenames: List[str], site: str, prev_opened: str = f'{_input_directory}previous.pkl') -> List[str]:
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
        prev_df[['start_datetime', 'end_datetime']] = prev_df[['start_datetime', 'end_datetime']].apply(pd.to_datetime)

    # Filter files by what has not been opened
    prev_filename_set = set(prev_df['filename'])
    new_filenames = [filename for filename in filenames if filename not in prev_filename_set]

    # Add files to prev_df
    for filename in new_filenames:
        data = pd.read_csv(filename)
        data["time(UTC)"] = pd.to_datetime(data["time(UTC)"])
        max_date = data["time(UTC)"].max()
        min_date = data["time(UTC)"].min()
        new_entry = {'site': site, 'filename': filename,'start_datetime': min_date, 'end_datetime': max_date}
        prev_df = pd.concat([prev_df, pd.DataFrame(new_entry, index=[0])], ignore_index=True)

    # Save new prev_df
    prev_df.to_pickle(prev_opened)

    # List all files with the date equal or newer than last_date
    last_date = dt.datetime.strptime(last_date, '%Y-%m-%d')
    filtered_prev_df = prev_df[(prev_df['site'] == site) & (prev_df['start_datetime'] >= last_date)]
    filtered_filenames = filtered_prev_df['filename'].tolist()

    return filtered_filenames



def _add_date(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """
    LBNL's nclarity files do not contain the date in the time column. This
    helper function extracts the date from the filename and adds it to the 
    time column of the data.
    Args: 
        df (pd.DataFrame): Dataframe
        filename (str): filename as string
    Returns:
        pd.DataFrame: Modified dataframe
    """
    date = filename[-18:-8]
    df['time'] = df.apply(lambda row: date + " " + str(row['time']), axis=1)
    df['time'] = pd.to_datetime(df['time'])
    return df


def add_local_time(df : pd.DataFrame, site_name : str) -> pd.DataFrame:
    """
    Function adds a column to the dataframe with the local time.
    Args:
        df (pd.DataFrame): Dataframe
        site_name (str): site name 
    """
    input_dir = configure.get('input', 'directory')
    site_info_path = input_dir + configure.get('input', 'site_info')

    try:
        site_info_df = pd.read_csv(site_info_path)
    except FileNotFoundError:
        print("File Not Found: ", site_info_path)
        return
    
    site_info_df = site_info_df.loc[site_info_df['site'] == site_name]
    if not site_info_df.empty:
        local_tz = pytz.timezone(site_info_df['local_tz'].str.upper())
        df['time_local'] = df.apply(lambda row: row['time_utc'].astimezone(pytz.timezone(local_tz)), axis=1)

    return df

def elev_correction(site_name : str) -> pd.DataFrame:
    """
    Function creates a dataframe for a given site that contains site name, elevation, 
    and the corrected elevation.
    Args: 
        site_name (str): site's name
    Returns: 
        pd.DataFrame: new Pandas dataframe
    """
    input_dir = configure.get('input', 'directory')
    site_info_path = input_dir + configure.get('input', 'site_info')

    try:
        site_info_df = pd.read_csv(site_info_path)
    except FileNotFoundError:
        print("File Not Found: ", site_info_path)
        return
    
    site_info_df = site_info_df.loc[site_info_df['site'] == site_name]
    print(site_info_df)

    if not site_info_df.empty and 'elev' in site_info_df.columns:
        elev_ft = np.array([0,1000,2000,3000,4000,5000,6000,7000,8000,9000,10000,11000,12000])
        elev_ft = elev_ft.reshape(-1,1)
        alt_corr_fact = np.array([1,0.97,0.93,0.89,0.87,0.84,0.80,0.77,0.75,0.72,0.69,0.66,0.63])

        lin_model = LinearRegression().fit(elev_ft, alt_corr_fact)
        elv_df = site_info_df[['elev']].fillna(0)
        air_corr = lin_model.predict(elv_df)

        site_air_corr = site_info_df[['site','elev']].copy()
        site_air_corr = site_air_corr.assign(air_corr=1)
        site_air_corr.loc[site_air_corr["elev"].notnull() & (site_air_corr["elev"] >= 1000), "air_corr"] = air_corr
      
        return site_air_corr
    else:
        return pd.DataFrame()


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
    if len(od_conditions):
        df.loc[df.index > date_forward, "Humidity_ODRH"] = np.nan
        data_old = df["Humidity_ODRH"]

        data_new = od_conditions.loc[od_conditions.index > date_forward]
        data_new = data_new[f"{site_name}_ODRH"]

        df["Humidity_ODRH"] = data_old.fillna(value=data_new)
    
    return df


def create_fan_curves(cfm_info: pd.DataFrame, site_info: pd.Series) -> pd.DataFrame:
    """
    Create fan curves for each site.
    Args:
        cfm_info (pd.DataFrame): DataFrame of fan curve information.
        site_info (pd.Series): Series containing the site information.
    Returns:
        pd.DataFrame: Dataframe containing the fan curves for each site.
    """

    # Convert furnace power from kW to W
    site_info['furn_misc_power'] *= 1000

    # Calculate furnace power to remove from blower power
    cfm_info['watts_to_remove'] = site_info['furn_misc_power']
    cfm_info['watts_to_remove'] = cfm_info['watts_to_remove'].fillna(0)
    cfm_info['watts_to_remove'] = cfm_info['watts_to_remove'].where(cfm_info['mode'].str.contains('heat'), 0)

    # Subtract furnace power from blower power
    mask = cfm_info['watts_to_remove'] != 0
    cfm_info.loc[mask, 'ID_blower_rms_watts'] = cfm_info.loc[mask,
                                                             'ID_blower_rms_watts'] - cfm_info.loc[mask, 'watts_to_remove']
    #cfm_info.loc[mask, 'ID_blower_rms_watts'] -= cfm_info['watts_to_remove']

    # Group by site and estimate coefficients
    by_site = cfm_info.groupby('site')

    def estimate_coefficients(group):
        """
        Estimate coefficients for the fan curve.
        Args:
            group (pd.DataFrame): Dataframe containing the data for a given site.
        Returns:
            pd.Series: Series containing the coefficients for the fan curve.
        """
        X = group[['ID_blower_rms_watts']].values ** 0.3333 - 1
        y = group['ID_blower_cfm'].values

        # For the linear regression model generated below, return the a and b coefficients
        fan_model = LinearRegression().fit(X, y)
        a, b = fan_model.intercept_, fan_model.coef_
        coefficients = pd.Series({'a': a, 'b': b[0]})
        return coefficients
    fan_coeffs = by_site.apply(estimate_coefficients)
    fan_coeffs = fan_coeffs.reset_index()
    return fan_coeffs


def get_cfm_values(df, site_cfm, site_info, site):
    site_cfm = site_cfm[site_cfm.index == site]
    fan_curve = True if site_cfm.iloc[0]["use_fan_curve"] == "TRUE" else False

    if not fan_curve:
        cfm_info = dict()
        cfm_info["circ"] = [site_cfm["ID_blower_cfm"].iloc[i] for i in range(
            len(site_cfm.index)) if bool(re.search(".*circ.*", site_cfm["mode"].iloc[i]))]
        cfm_info["heat"] = [site_cfm["ID_blower_cfm"].iloc[i] for i in range(
            len(site_cfm.index)) if bool(re.search(".*heat.*", site_cfm["mode"].iloc[i]))]
        cfm_info["cool"] = [site_cfm["ID_blower_cfm"].iloc[i] for i in range(
            len(site_cfm.index)) if bool(re.search(".*cool.*", site_cfm["mode"].iloc[i]))]

        df["Cfm_Calc"] = [np.mean(cfm_info[state]) if len(cfm_info[state]) != 0 else 0.0 for state in df["HVAC"]]

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


def get_acf(elev):
    if (elev == np.NaN) | (elev < 1000):
        return 1

    # create arrays for elevation in feet and altitude correction factor
    elev_ft = np.array([0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000])
    acf = np.array([1, 0.97, 0.93, 0.89, 0.87, 0.84, 0.80, 0.77, 0.75, 0.72, 0.69, 0.66, 0.63])
    cf_df = pd.DataFrame({'elev_ft': elev_ft, 'acf': acf})
    dens_cor_reg = LinearRegression().fit(cf_df[['elev_ft']], cf_df['acf'])

    # use the linear regression model to predict the air correction factor for each site
    site_elevation = elev.values.reshape(-1, 1)
    air_corr = dens_cor_reg.predict(site_elevation)

    return air_corr[0]


def get_cop_values(df: pd.DataFrame, site_info: pd.DataFrame):
    w_to_btuh = 3.412
    btuh_to_w = 1 / w_to_btuh
    air_density = 1.08
    air_correction_factor = get_acf(site_info["elev"])

    df["Power_Output_BTUh"] = (df["Temp_SAT1"] - df["Temp_RAT"]) * df["Cfm_Calc"] * air_density * air_correction_factor
    df.loc[(df["HVAC"] == "heat") | (df["HVAC"] == "circ"), "Power_Output_BTUh"] = 0.0
    df["Power_Output_kW"] = (df["Power_Output_BTUh"] * btuh_to_w) * (1/1000)
    df["cop"] = np.abs(df["Power_Output_kW"] / df["Power_system1"]) 
    df.loc[(df["cop"] == np.inf) | (df["cop"].isna()), "cop"] = 0.0
    
    df.drop(["Power_Output_BTUh", "Power_Output_kW"], axis=1)

    return df


def get_site_info(site: str) -> pd.Series:
    """
    Returns a dataframe of the site information for the given site
    
    Args:
        site (str): The site name
        
    Returns:
        df (pd.Series): The Series of the site information
    """
    site_info_path = _input_directory + configure.get('input', 'site_info')
    df = pd.read_csv(site_info_path, skiprows=[1])
    df.dropna(how='all', inplace=True)
    df = df[df['site'] == site]
    return df.squeeze()


def get_site_cfm_info(site: str) -> pd.DataFrame:
    """
    Returns a dataframe of the site cfm information for the given site
    NOTE: The parsing is necessary as the first row of data are comments that need to be dropped.
    
    Args:
        site (str): The site name
        
    Returns:
        df (pd.DataFrame): The DataFrame of the site cfm information
    """
    site_cfm_info_path = _input_directory + configure.get('input', 'site_cfm_info')
    df = pd.read_csv(site_cfm_info_path, skiprows=[1], encoding_errors='ignore')
    df = df.loc[df['site'] == site]
    return df

def merge_indexlike_rows(df: pd.DataFrame) -> pd.DataFrame:
     """
     Merges index-like rows together ensuring that all relevant information for a
     certain timestamp is stored in one row - not in multiple rows. It also rounds the
     timestamps to the nearest minute.

     Args:
         file_path (str): The file path to the data.
        
     Returns:
         df (pd.DataFrame): The DataFrame with all index-like rows merged. 
     """
     
     df = df.sort_index(ascending = True)
     grouped_df = df.groupby(df.index).first()

     return grouped_df



