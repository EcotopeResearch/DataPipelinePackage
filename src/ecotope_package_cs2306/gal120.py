import pandas as pd
import numpy as np
import re
import datetime

def main():
    # Constants #######################################
    relative_loc = pd.Series([1, .82, .64, .46, .29, .11, 0])
    tank_frxn = relative_loc.subtract(relative_loc.shift(-1))
    gal_per_tank = 285
    tot_storage = gal_per_tank * 3  #3 tanks
    zone_gals = tank_frxn * tot_storage
    zone_gals = pd.Series.dropna(zone_gals) #remove NA from leading math
    zone_list = pd.Series(["ZoneTemp_top", "ZoneTemp_midtop", "ZoneTemp_mid", "ZoneTemp_midlow", "ZoneTemp_low", "ZoneTemp_bottom"])
    gals_per_zone = pd.DataFrame({'Zone':zone_list, 'Zone_vol_g':zone_gals}) # Steal me

    test_grid = pd.read_excel('../input/test_matrix.xlsx', skiprows = 1)
    #print((~test_grid.iloc[: , -6:].isna()).sum(axis=1))
    test_grid['CountNa'] = (~test_grid.iloc[: , -6:].isna()).sum(axis=1)
    test_grid = test_grid[test_grid['CountNa'] > 0]

    event_list = test_grid.loc[:, ["Date", "Controls_vsn", "Normal", "LoadUp", "Shed", "CriticalPeak",
                            "GridEmergency", "AdvancedLoadUp"]]
    #print(event_list)

    # make the events less condensed to use in processing------
    event_list_w = pd.DataFrame()

    for i in range(2, len(event_list.columns)):
        colnm = event_list.columns[i]
        ch = ";"
        def funcC(c):
            return str(c).count(ch)
        nmax = event_list.iloc[:, 3].apply(funcC).max() + 1
        tmp = event_list[colnm].astype("string").str.split(ch,expand=True, n=nmax)
        if(len(tmp.columns) < nmax):
            for j in range(len(tmp.columns), nmax):
                tmp[j] = pd.NA
                tmp[j] = tmp[j].astype("string")
        tmpColnames = [colnm + str(s) for s in list(range(1, nmax + 1))]
        tmp.columns = tmpColnames
        event_list_w = pd.concat([event_list_w, tmp], axis=1)

    del(tmp)

    #print(pd.DataFrame(event_list['Date'], event_list['Controls_vsn']))
    firstTwoCols = pd.concat({"Date": event_list['Date'],
                              "Controls_vsn": event_list['Controls_vsn']}, axis = 1)
    event_list_l = pd.concat([firstTwoCols, event_list_w], axis=1)
    event_list_l = pd.melt(event_list_l, id_vars=["Date", "Controls_vsn"],
                           value_vars = event_list_w.columns, var_name='Mode', value_name="VAL")
    event_list_l = event_list_l.loc[~pd.isna(event_list_l['VAL'])]
    #event_list_l['VAL'] = [value.strip() for value in event_list_l['VAL']]
    event_list_l['StartTime'] = event_list_l["VAL"].str.extract(r'(.+?(?=-))')

    def addMinToTime(timeVal):
        if(":" in timeVal):
            return timeVal
        else:
            return timeVal + ":00"
    event_list_l['StartTime'] = [addMinToTime(time) for time in event_list_l['StartTime']]
    event_list_l['EndTime'] = event_list_l["VAL"].str.split("-", expand=True).iloc[:, 1]
    event_list_l['EndTime'] = [addMinToTime(time) for time in event_list_l['EndTime']]
    event_list_l['StartTime'] = event_list_l['Date'].astype("string") + " " + event_list_l['StartTime']
    event_list_l['StartTime'] = pd.to_datetime(event_list_l['StartTime'])
    event_list_l["StartTime"] = event_list_l["StartTime"].dt.tz_localize('US/Pacific')
    event_list_l['EndTime'] = event_list_l['Date'].astype("string") + " " + event_list_l['EndTime']
    event_list_l['EndTime'] = pd.to_datetime(event_list_l['EndTime'])
    event_list_l["EndTime"] = event_list_l["EndTime"].dt.tz_localize('US/Pacific')
    event_list_l = event_list_l.sort_values("StartTime", axis=0, ascending=True)

    del(event_list, event_list_w)

    # make prevop variable (previous operation)---------
    # if the start time is the same as the previous end time, then make the previous Mode into the Previous Operation
    # otherwise assign "Normal"
    # rows_events <- nrow(event_list_l)
    col_start_ind = list(event_list_l.columns).index('StartTime')
    col_end_ind = list(event_list_l.columns).index('EndTime')
    col_mode = list(event_list_l.columns).index('Mode')

    prev_op_holder = pd.DataFrame()
    prev_op_holder['StartTime'] = []
    prev_op_holder['Mode'] = []

    #for(j in 2:nrow(event_list_l)){
    for j in range(1, len(event_list_l.index)):
        #print(event_list_l.iloc[j, col_start_ind])
        if event_list_l.iloc[j, col_start_ind] == event_list_l.iloc[j - 1, col_end_ind]:
            tmp_startTime = event_list_l.iloc[j,col_start_ind]
            tmp_mode = event_list_l.iloc[j-1,col_mode]
            tmp = pd.DataFrame().append({'StartTime': tmp_startTime,
                                          'Mode': tmp_mode}, ignore_index=True)
            prev_op_holder = pd.concat([prev_op_holder, tmp], axis=0)

    del(tmp)
    prev_op_holder = prev_op_holder.rename(columns={'Mode': 'PreviousOperation'})
    prev_op_holder['PreviousOperation'] = [re.sub("[0-9]"," ",prevOp) for prevOp in prev_op_holder['PreviousOperation']]

    event_list_l = event_list_l.merge(prev_op_holder, how='left', on='StartTime')
    def fixPrevOp(prevOp):
        if (pd.isna(prevOp)):
            return "Normal"
        return prevOp
    event_list_l['PreviousOperation'] = [fixPrevOp(prevOp).strip() for prevOp in event_list_l['PreviousOperation']]

    event_join = pd.concat({"time_pt": event_list_l['StartTime'],
                            "Controls_vsn": event_list_l['Controls_vsn'],
                            "Mode": event_list_l['Mode'],
                            "PreviousOperation": event_list_l['PreviousOperation']}, axis = 1)

    # unfold the events into something discrete that can be joined to our data streams---------
    # this uses row_events and start/end_col from above
    # results in duplicates which are removed with unique(.)
    # *seqevent----------
    seq_event = pd.DataFrame()
    seq_event['time_pt'] = []

    for k in range(0, len(event_list_l.index)):
        tmp = pd.date_range(start=event_list_l.iloc[k, col_start_ind],
                            end=event_list_l.iloc[k, col_end_ind], freq="min")
        tmp = pd.DataFrame(tmp)
        tmp.columns = ['time_pt']
        seq_event = pd.concat([seq_event, tmp], axis=0)

    del(tmp)
    seq_event = pd.DataFrame({"time_pt": seq_event['time_pt'].unique()})

    seq_event = seq_event.merge(event_join, how='left')
    seq_event = seq_event.fillna(method="ffill")

    processed_data = pd.read_csv("../output/processed_data.csv")

    start_loadshift = datetime.date(2022, 3, 18)

    processed_data["time_pt"] = pd.to_datetime(processed_data["time_pt"]).dt.tz_localize('US/Pacific')
    processed_data['Date'] = [d.date() for d in processed_data["time_pt"]]
    processed_data['Hr'] = [d.hour for d in processed_data["time_pt"]]
    processed_data["WeekDay"] = [((d.weekday() + 1)%7) + 1 for d in processed_data["time_pt"]]
    def getWeekPart(weekdayNum):
        if(weekdayNum == 1 or weekdayNum == 7):
            return "Weekend"
        return "Weekday"

    processed_data["WeekPart"] = [getWeekPart(num) for num in processed_data["WeekDay"]]
    processed_data["YrMo"] = [str(d.year) + "-" + str(d.month) for d in processed_data["time_pt"]]
    processed_data = processed_data.loc[processed_data['Date'] > start_loadshift]
    processed_data = processed_data.merge(seq_event, how='left', on='time_pt')
    def replaceNAWithEmptyString(val):
        if pd.isna(val):
            return ""
        return val
    processed_data["Mode"] = [replaceNAWithEmptyString(value) for value in processed_data["Mode"]]
    processed_data["PreviousOperation"] = [replaceNAWithEmptyString(value) for value in processed_data["PreviousOperation"]]
    processed_data["ModeID"] = processed_data["Mode"] + processed_data["CycleStageID"]
    processed_data["Mode_DateID"] = processed_data['Date'].astype("string") + "_" + processed_data["Mode"]
    processed_data = processed_data.groupby(by=['Mode_DateID'])
    tmp = processed_data.cumcount() + 1
    processed_data = processed_data.obj
    processed_data['mode_minid'] = tmp
    del(tmp)

    temp_vars = list(processed_data.columns)

    def checkTemp(temp):
        if re.search((r"Temp_|Temp[0-9]|Flow_CityWater_at|PowerIn_HPWH"), temp):
            return True
        return False
    temp_vars = list(filter(checkTemp, temp_vars))
    def checkTemp(temp):
        if re.search((r"MXV|dy|avg"), temp):
            return False
        return True
    temp_vars = sorted(list(filter(checkTemp, temp_vars)))

    # remove sensor that is not working, and original TH16s calc'd by Mitsu
    temp_vars.remove('Temp3_ST2')
    temp_vars.remove('Temp3_ST2_TH16')
    temp_vars.remove('Temp5_ST2_low_TH16')

    def checkTemp(temp):
        if re.search((r"Temp[0-9]"), temp):
            return True
        return False
    sorted(list(filter(checkTemp, temp_vars))) #you should have one temp sensor at each depth, for each tank

    tank_temps = processed_data[['time_pt', 'YrMo', 'Mode_DateID', 'mode_minid']]
    tank_temps = pd.concat([tank_temps, processed_data[temp_vars]], axis=1)
    tank_temps = pd.concat([tank_temps, processed_data[[x for x in processed_data if "Mode" in x
                                                        and x!="Mode_DateID"]]], axis=1)
    tank_temps = pd.concat([tank_temps, processed_data[['PreviousOperation', 'HPWH_on']]], axis=1)
    tank_temps = tank_temps.rename(columns={'Temp_PrimaryStorageOutTop' : 'Temp_outlet'})
    def applyThis(val):
        if val < 34:
            return pd.NA
        return val
    tank_temps['Temp1_ST2_high_TH15'] = [applyThis(num) for num in tank_temps['Temp1_ST2_high_TH15']]
    tank_temps['Temp1_ST1_high'] = [applyThis(num) for num in tank_temps['Temp1_ST1_high']]
    tank_temps['Temp1_ST3_high'] = [applyThis(num) for num in tank_temps['Temp1_ST3_high']]
    #tank_top = processed_data[[x for x in processed_data if "Temp1" in x]]
    #tank_temps['Temp_top'] = tank_top.sum(axis=1)/len(tank_top.columns)
    def avgRowValsOfColContainingSubstring(df, substring):
        df_subset = processed_data[[x for x in df if substring in x]]
        result = df_subset.sum(axis=1, skipna=True) / len(df_subset.columns)
        return result

    tank_temps['Temp_top'] = avgRowValsOfColContainingSubstring(tank_temps, "Temp1")
    tank_temps['Temp_midtop'] = avgRowValsOfColContainingSubstring(tank_temps, "Temp2")
    tank_temps['Temp_mid'] = avgRowValsOfColContainingSubstring(tank_temps, "Temp3")
    tank_temps['Temp_midlow'] = avgRowValsOfColContainingSubstring(tank_temps, "Temp4")
    tank_temps['Temp_low'] = avgRowValsOfColContainingSubstring(tank_temps, "Temp5")

    tank_temps['Galsadd_outlet'] = tank_temps['Flow_CityWater_atSkid'] *(tank_temps['Temp_outlet'] - 120)/(120 - tank_temps['Temp_CityWater_atSkid'])
    tank_temps['GalsdeliveredPrimary'] = tank_temps['Galsadd_outlet'] + tank_temps['Flow_CityWater_atSkid']

    depth_levels = ["Temp_outlet", "Temp_top", "Temp_midtop", "Temp_mid",
                       "Temp_midlow", "Temp_low", "Temp_CityWater_atSkid"]
    mode_levels = ["LoadUp1", "Shed1", "LoadUp2", "Shed2"]

    gal_120_sto = tank_temps[['time_pt', 'YrMo', 'Mode_DateID', 'mode_minid', 'Temp_outlet']]
    tank_temps_colnames = list(tank_temps.columns)
    #filter tank_temps_colnames on the reg expression
    tank_temps_colnames = filter(lambda x: re.findall(r"Temp_m|Temp_to|Temp_l", x), tank_temps_colnames)
    gal_120_sto = pd.concat([gal_120_sto, tank_temps[tank_temps_colnames]], axis=1)
    gal_120_sto = pd.concat([gal_120_sto, tank_temps[['Temp_CityWater_atSkid']]], axis=1)
    print(gal_120_sto)

    # tank_temps.to_csv("../output/tank_temps_fromReWrittenScript" +
    #                  "_" + str(datetime.datetime.now().date()) #you can uncomment this if you want to save a file with today's date
    #                  + ".csv", index = False)

if __name__ == '__main__':
    main()