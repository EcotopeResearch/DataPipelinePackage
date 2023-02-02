import pandas as pd
from dateutil.parser import parse

pd.set_option('display.max_columns', None)


def get_energy_by_min(df: pd.DataFrame) -> pd.DataFrame:
    """
    Energy is recorded cummulatively. Function takes the lagged differences in
    order to get a per/minute value for each of the energy variables.

    Input: Pandas dataframe
    Output: Pandas dataframe
    """
    energy_vars = df.filter(regex=".*Energy.*")
    energy_vars = energy_vars.filter(regex=".*[^BTU]$")
    for var in energy_vars:
        df[var] = df[var] - df[var].shift(1)
    return df


def convert_to_kwh(sensor_readings):
    return sensor_readings / (60 * 3.412)


def get_kbtu_value(gpm, delta_t):
    return 60 * 8.33 * gpm * delta_t / 1000


def aggregate_values(df: pd.DataFrame) -> dict:
    after_6pm = str(parse(df.index[0]).replace(hour=6, minute=0))

    avg_sd = df[['Temp_RecircSupply_MXV1', 'Temp_RecircSupply_MXV2', 'Flow_CityWater_atSkid',
                 'Temp_PrimaryStorageOutTop', 'Temp_CityWater_atSkid',
                 'Flow_SecLoop', 'Temp_SecLoopHexOutlet', 'Temp_SecLoopHexInlet', 'Flow_CityWater', 'Temp_CityWater',
                 'Flow_RecircReturn_MXV1', 'Temp_RecircReturn_MXV1', 'Flow_RecircReturn_MXV2', 'Temp_RecircReturn_MXV2',
                 'PowerIn_SecLoopPump', 'EnergyIn_HPWH']].mean(axis=0, skipna=True)

    avg_sd_6 = df[after_6pm:][['Temp_CityWater_atSkid', 'Temp_CityWater']].mean(axis=0, skipna=True)

    print(avg_sd)
    print(avg_sd_6)

    cop_inter = dict()
    cop_inter['Temp_RecircSupply_avg'] = (avg_sd['Temp_RecircSupply_MXV1'] + avg_sd['Temp_RecircSupply_MXV2']) / 2
    cop_inter['HeatOut_PrimaryPlant'] = get_kbtu_value(avg_sd['Flow_CityWater_atSkid'],
                                                       avg_sd['Temp_PrimaryStorageOutTop'] -
                                                       avg_sd['Temp_CityWater_atSkid'])
    cop_inter['HeatOut_PrimaryPlant_dyavg'] = get_kbtu_value(avg_sd['Flow_CityWater_atSkid'],
                                                             avg_sd['Temp_PrimaryStorageOutTop'] -
                                                             avg_sd_6['Temp_CityWater_atSkid'])
    cop_inter['HeatOut_SecLoop'] = get_kbtu_value(avg_sd['Flow_SecLoop'], avg_sd['Temp_SecLoopHexOutlet'] -
                                                  avg_sd['Temp_SecLoopHexInlet'])
    cop_inter['HeatOut_HW'] = get_kbtu_value(avg_sd['Flow_CityWater'], cop_inter['Temp_RecircSupply_avg'] -
                                             avg_sd['Temp_CityWater'])
    cop_inter['HeatOut_HW_dyavg'] = get_kbtu_value(avg_sd['Flow_CityWater'], cop_inter['Temp_RecircSupply_avg'] -
                                                   avg_sd_6['Temp_CityWater'])
    cop_inter['HeatLoss_TempMaint_MXV1'] = get_kbtu_value(avg_sd['Flow_RecircReturn_MXV1'],
                                                          avg_sd['Temp_RecircSupply_MXV1'] -
                                                          avg_sd['Temp_RecircReturn_MXV1'])
    cop_inter['HeatLoss_TempMaint_MXV2'] = get_kbtu_value(avg_sd['Flow_RecircReturn_MXV2'],
                                                          avg_sd['Temp_RecircSupply_MXV2'] -
                                                          avg_sd['Temp_RecircReturn_MXV2'])
    cop_inter['EnergyIn_SecLoopPump'] = avg_sd['PowerIn_SecLoopPump'] * (1/60) * (1/1000)
    cop_inter['EnergyIn_HPWH'] = avg_sd['EnergyIn_HPWH']

    return cop_inter


def calculate_cop_values(df: pd.DataFrame) -> dict:
    heatLoss_fixed = 27.296
    ENERGYIN_SWINGTANK1 = 0
    ENERGYIN_SWINGTANK2 = 0

    cop_inter = aggregate_values(df)

    cop_values = dict()
    cop_values['COP_DHWSys'] = (convert_to_kwh(cop_inter['HeatOut_HW']) + (
        convert_to_kwh(cop_inter['HeatLoss_TempMaint_MXV1'])) + (
        convert_to_kwh(cop_inter['HeatLoss_TempMaint_MXV2']))) / (
            cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'] + ENERGYIN_SWINGTANK1 +
            ENERGYIN_SWINGTANK2)

    cop_values['COP_DHWSys_dyavg'] = (convert_to_kwh(cop_inter['HeatOut_HW_dyavg']) + (
        convert_to_kwh(cop_inter['HeatLoss_TempMaint_MXV1'])) + (
        convert_to_kwh(cop_inter['HeatLoss_TempMaint_MXV2']))) / (
            cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'] + ENERGYIN_SWINGTANK1 +
            ENERGYIN_SWINGTANK2)

    cop_values['COP_DHWSys_fixTMloss'] = ((convert_to_kwh(cop_inter['HeatOut_HW'])) + (
        convert_to_kwh(heatLoss_fixed))) / ((cop_inter['EnergyIn_HPWH'] +
                                            cop_inter['EnergyIn_SecLoopPump'] + ENERGYIN_SWINGTANK1 +
                                             ENERGYIN_SWINGTANK2))

    cop_values['COP_PrimaryPlant'] = (convert_to_kwh(cop_inter['HeatOut_PrimaryPlant'])) / \
                                     (cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

    cop_values['COP_PrimaryPlant_dyavg'] = (convert_to_kwh(cop_inter['HeatOut_PrimaryPlant_dyavg'])) / \
                                     (cop_inter['EnergyIn_HPWH'] + cop_inter['EnergyIn_SecLoopPump'])

    return cop_values


if __name__ == "__main__":
    ecotope_data = pd.read_csv("output/1_11_23.csv")
    ecotope_data.set_index(['time'], inplace=True)

    variable_data = pd.read_csv("input/Variable_Names.csv")
    variable_data = variable_data[1:87]
    variable_alias = list(variable_data["variable_alias"])
    variable_true = list(variable_data["variable_name"])
    variable_alias_true_dict = dict(zip(variable_alias, variable_true))

    ecotope_data.rename(columns=variable_alias_true_dict, inplace=True)
    ecotope_data.ffill(axis=0, inplace=True)

    ecotope_data = get_energy_by_min(ecotope_data)
    print(ecotope_data)
    # cop = calculate_cop_values(ecotope_data)
    # print(cop)
