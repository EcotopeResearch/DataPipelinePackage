import pandas as pd
import numpy as np


# def calculate_intermediate_values(df: pd.DataFrame) -> pd.DataFrame:


if __name__ == "__main__":
    df_path = "input/ecotope_wide_data.csv"
    ecotope_data = pd.read_csv(df_path)
    ecotope_data.set_index("time", inplace=True)
    print(ecotope_data)
