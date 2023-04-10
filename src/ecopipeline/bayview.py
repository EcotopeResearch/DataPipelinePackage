import pandas as pd
from ecopipeline.unit_convert import energy_btu_to_kwh, energy_kwh_to_kbtu


def aggregate_values(df: pd.DataFrame, thermo_slice: str) -> pd.DataFrame:
    """
    Gets daily average of data for all relevant varibles. 

    Args:
        df (pd.DataFrame): Pandas DataFrame of minute by minute data
        thermo_slice (str): indicates the time at which slicing begins. If none no slicing is performed. The format of the thermo_slice string is "HH:MM AM/PM".

    Returns: 
        pd.DataFrame: Pandas DataFrame which contains the aggregated hourly data.
    """
    avg_sd = df[['Temp_RecircSupply_MXV1', 'Temp_RecircSupply_MXV2', 'Flow_CityWater_atSkid', 'Temp_PrimaryStorageOutTop',
                 'Temp_CityWater_atSkid', 'Flow_SecLoop', 'Temp_SecLoopHexOutlet', 'Temp_SecLoopHexInlet', 'Flow_CityWater', 'Temp_CityWater',
                 'Flow_RecircReturn_MXV1', 'Temp_RecircReturn_MXV1', 'Flow_RecircReturn_MXV2', 'Temp_RecircReturn_MXV2', 'PowerIn_SecLoopPump',
                 'EnergyIn_HPWH']].resample('D').mean()

    if thermo_slice is not None:
        avg_sd_6 = df.between_time(thermo_slice, "11:59PM")[
            ['Temp_CityWater_atSkid', 'Temp_CityWater']].resample('D').mean()
    else:
        avg_sd_6 = df[['Temp_CityWater_atSkid',
                       'Temp_CityWater']].resample('D').mean()

    cop_inter = pd.DataFrame(index=avg_sd.index)
    cop_inter['Temp_RecircSupply_avg'] = (
        avg_sd['Temp_RecircSupply_MXV1'] + avg_sd['Temp_RecircSupply_MXV2']) / 2
    cop_inter['HeatOut_PrimaryPlant'] = energy_kwh_to_kbtu(avg_sd['Flow_CityWater_atSkid'],
                                                           avg_sd['Temp_PrimaryStorageOutTop'] -
                                                           avg_sd['Temp_CityWater_atSkid'])
    cop_inter['HeatOut_PrimaryPlant_dyavg'] = energy_kwh_to_kbtu(avg_sd['Flow_CityWater_atSkid'],
                                                                 avg_sd['Temp_PrimaryStorageOutTop'] -
                                                                 avg_sd_6['Temp_CityWater_atSkid'])
    cop_inter['HeatOut_SecLoop'] = energy_kwh_to_kbtu(avg_sd['Flow_SecLoop'], avg_sd['Temp_SecLoopHexOutlet'] -
                                                      avg_sd['Temp_SecLoopHexInlet'])
    cop_inter['HeatOut_HW'] = energy_kwh_to_kbtu(avg_sd['Flow_CityWater'], cop_inter['Temp_RecircSupply_avg'] -
                                                 avg_sd['Temp_CityWater'])
    cop_inter['HeatOut_HW_dyavg'] = energy_kwh_to_kbtu(avg_sd['Flow_CityWater'], cop_inter['Temp_RecircSupply_avg'] -
                                                       avg_sd_6['Temp_CityWater'])
    cop_inter['HeatLoss_TempMaint_MXV1'] = energy_kwh_to_kbtu(avg_sd['Flow_RecircReturn_MXV1'],
                                                              avg_sd['Temp_RecircSupply_MXV1'] -
                                                              avg_sd['Temp_RecircReturn_MXV1'])
    cop_inter['HeatLoss_TempMaint_MXV2'] = energy_kwh_to_kbtu(avg_sd['Flow_RecircReturn_MXV2'],
                                                              avg_sd['Temp_RecircSupply_MXV2'] -
                                                              avg_sd['Temp_RecircReturn_MXV2'])
    cop_inter['EnergyIn_SecLoopPump'] = avg_sd['PowerIn_SecLoopPump'] * \
        (1/60) * (1/1000)
    cop_inter['EnergyIn_HPWH'] = avg_sd['EnergyIn_HPWH']

    return cop_inter


def calculate_cop_values(df: pd.DataFrame, heatLoss_fixed: int, thermo_slice: str) -> pd.DataFrame:
    """
    Performs COP calculations using the daily aggregated data. 

    Args: 
        df (pd.DataFrame): Pandas DataFrame to add COP columns to
        heatloss_fixed (float): fixed heatloss value 
        thermo_slice (str): the time at which slicing begins if we would like to thermo slice. 

    Returns: 
        pd.DataFrame: Pandas DataFrame with the added COP columns. 
    """
    cop_inter = pd.DataFrame()
    if (len(df) != 0):
        cop_inter = aggregate_values(df, thermo_slice)

    cop_values = pd.DataFrame(index=cop_inter.index, columns=[
                              "COP_DHWSys", "COP_DHWSys_dyavg", "COP_DHWSys_fixTMloss", "COP_PrimaryPlant", "COP_PrimaryPlant_dyavg"])

    try:
        cop_values['COP_DHWSys'] = (energy_btu_to_kwh(cop_inter['HeatOut_HW']) + (
            energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV1'])) + (
            energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV2']))) / (
                cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

        if thermo_slice is not None:
            cop_values['COP_DHWSys_dyavg'] = (energy_btu_to_kwh(cop_inter['HeatOut_HW_dyavg']) + (
                energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV1'])) + (
                energy_btu_to_kwh(cop_inter['HeatLoss_TempMaint_MXV2']))) / (
                    cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

        cop_values['COP_DHWSys_fixTMloss'] = ((energy_btu_to_kwh(cop_inter['HeatOut_HW'])) + (
            energy_btu_to_kwh(heatLoss_fixed))) / ((cop_inter['EnergyIn_HPWH'] +
                                                    cop_inter['EnergyIn_SecLoopPump']))

        cop_values['COP_PrimaryPlant'] = (energy_btu_to_kwh(cop_inter['HeatOut_PrimaryPlant'])) / \
            (cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

        if thermo_slice is not None:
            cop_values['COP_PrimaryPlant_dyavg'] = (energy_btu_to_kwh(cop_inter['HeatOut_PrimaryPlant_dyavg'])) / \
                (cop_inter['EnergyIn_HPWH'] +
                 cop_inter['EnergyIn_SecLoopPump'])

    except ZeroDivisionError:
        print("DIVIDED BY ZERO ERROR")
        return df

    return cop_values