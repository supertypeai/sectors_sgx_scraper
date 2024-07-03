from scraper import get_screener_page_data, scrap_function
from combiner import combine_data
import pandas as pd
from multiprocessing import Process
import os
import time
from json import loads, dumps

if __name__ == "__main__":

  start = time.time()

  # Get screener data
  data = get_screener_page_data()
  simplified_data = []

  for d in data:
    stock_data = dict()
    stock_data['company_name'] = d['companyName']
    stock_data['stock_code'] = d['stockCode']
    stock_data['ric_code'] = d['ricCode']
    stock_data['sector_code'] = d['sector']
    simplified_data.append(stock_data)

  # Make dataframe
  df = pd.DataFrame(simplified_data)

  # Divide to processes
  length_list = len(simplified_data)
  i1 = int(length_list / 4)
  i2 = 2 * i1
  i3 = 3 * i1

  p1 = Process(target=scrap_function, args=(simplified_data[:i1], 1))
  p2 = Process(target=scrap_function, args=(simplified_data[i1:i2], 2))
  p3 = Process(target=scrap_function, args=(simplified_data[i2:i3], 3))
  p4 = Process(target=scrap_function, args=(simplified_data[i3:], 4))

  p1.start()
  p2.start()
  p3.start()
  p4.start()

  p1.join()
  p2.join()
  p3.join()
  p4.join()

  # Merge data
  df_final = combine_data(df)

  # Save to JSON and CSV
  cwd = os.getcwd()
  data_dir = os.path.join(cwd, "data")
  
  result_json = df_final.to_json(orient="records")
  parsed_json = loads(result_json)
  dumps(parsed_json, indent=2)
  
  df_final.to_csv(os.path.join(data_dir, "final_data.csv"), index=False)

  # End time
  end = time.time()
  duration = int(end-start)
  print(f"The execution time: {time.strftime('%H:%M:%S', time.gmtime(duration))}")
