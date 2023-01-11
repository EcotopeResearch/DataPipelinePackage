import extract


def __main__():
    """
    stations = ["727935-24234"]
    #, 'KPWM', 'KSFO', 'KAVL'
    formatted_dfs = extract.get_noaa_data(['KBFI'])
    print(formatted_dfs)
    for key, value in formatted_dfs.items():
        value.to_csv(f"output/{key}.csv", index=False)
        print("done 1")
    print("done")
    """
    
    filenames = extract.extract_json()
    file = extract.json_to_df(filenames)
    print(file)
    merged = extract.merge_noaa(file)
    print(merged)
    

if __name__ == '__main__':
    __main__()