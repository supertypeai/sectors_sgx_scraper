import json
import os
import pandas as pd
import numpy as np


def combine_data (df_screener):
  cwd = os.getcwd()
  data_dir = os.path.join(cwd, "data")
  data_file_path = [os.path.join(data_dir,f'P{i}_data.json') for i in range(1,5)]

  # Combine data
  all_data_list = list()
  for file_path in data_file_path:
    f = open(file_path)
    data = json.load(f)
    all_data_list = all_data_list + data

  # Make Dataframe
  df_scraped = pd.DataFrame(all_data_list)

  # Merge
  df_merge = df_screener.merge(df_scraped, left_on="stock_code", right_on="stock_code")
  
  return df_merge