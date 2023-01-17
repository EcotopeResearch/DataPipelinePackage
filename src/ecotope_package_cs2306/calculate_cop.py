import pandas as pd
import extract

pd.set_option('display.max_columns', None)


def calculate_intermediate_values(df: pd.DataFrame):
    time_diff = 1

    intermediate_df = pd.DataFrame()
    intermediate_df['Temp_RecircSupply_avg'] = (df['Temp_RecircSupply_MXV1'] + df['Temp_RecircSupply_MXV2']) / 2
    intermediate_df['HeatOut_PrimaryPlant'] = 60 * 8.33 * df['Flow_CityWater_atSkid'] * \
                                   (df['Temp_PrimaryStorageOutTop'] - df['Temp_CityWater_atSkid']) / 1000
    intermediate_df['HeatOut_SecLoop'] = 60 * 8.33 * df['Flow_SecLoop'] * \
                                    (df['Temp_SecLoopHexOutlet'] - df['Temp_SecLoopHexInlet']) / 1000
    intermediate_df['HeatOut_HW'] = 60 * 8.33 * df['Flow_CityWater'] * (intermediate_df['Temp_RecircSupply_avg'] -
                                                                        df['Temp_CityWater']) / 1000
    intermediate_df['HeatLoss_TempMaint_MXV1'] = 60 * 8.33 * df['Flow_RecircReturn_MXV1'] * \
                                (df['Temp_RecircSupply_MXV1'] - df['Temp_RecircReturn_MXV1']) / 1000
    intermediate_df['HeatLoss_TempMaint_MXV2'] = 60 * 8.33 * df['Flow_RecircReturn_MXV2'] * \
                                (df['Temp_RecircSupply_MXV2'] - df['Temp_RecircReturn_MXV2']) / 1000
    intermediate_df['EnergyIn_SecLoopPump'] = df['PowerIn_SecLoopPump'] * (time_diff * (1/60))
    intermediate_df['EnergyIn_HPWH'] = df['EnergyIn_HPWH'] * 2.7778e-7

    return intermediate_df.mean(axis=0)


def calculate_cop_values(aggregated_values: pd.DataFrame) -> dict:
    heatLoss_fixed = 27.296
    ENERGYIN_SWINGTANK1 = 1
    ENERGYIN_SWINGTANK2 = 1

    cop_values = dict()
    cop_values['COP_DHWSys'] = ((aggregated_values['HeatOut_HW'] * (1/60) * (1/3.412)) + (
            aggregated_values['HeatLoss_TempMaint_MXV1'] * (1/60) * (1/3.412)) + (
            aggregated_values['HeatLoss_TempMaint_MXV2'] * (1/60) * (1/3.412))) / (
            aggregated_values['EnergyIn_HPWH'] + aggregated_values['EnergyIn_SecLoopPump'] + ENERGYIN_SWINGTANK1 +
            ENERGYIN_SWINGTANK2)

    cop_values['COP_DHWSys_fixTMloss'] = ((aggregated_values['HeatOut_HW'] * (1/60) * (1/3.412)) + (heatLoss_fixed * (
            1/60) * (1/3.412))) / ((aggregated_values['EnergyIn_HPWH'] + aggregated_values['EnergyIn_SecLoopPump'] +
                                    ENERGYIN_SWINGTANK1 + ENERGYIN_SWINGTANK2))

    cop_values['COP_PrimaryPlant'] = ((aggregated_values['HeatOut_PrimaryPlant'] * (1/60)) * (1/3.412)) / \
                                     (aggregated_values['EnergyIn_HPWH'] + aggregated_values['EnergyIn_SecLoopPump'])

    return cop_values


if __name__ == "__main__":
    df_path = "input/ecotope_wide_data.csv"
    ecotope_data1 = pd.read_csv(df_path)
    ecotope_data1.set_index("time", inplace=True)

    ecotope_data2 = extract.json_to_df(["input/DCA632A85F95_20230101100000.json"])

    print(len(set(list(ecotope_data2['id']))))
    print(len(set(list(ecotope_data1.columns))))

    # intermediate_aggregations = calculate_intermediate_values(ecotope_data)

    # cop = calculate_cop_values(intermediate_aggregations)
    # print(cop)
