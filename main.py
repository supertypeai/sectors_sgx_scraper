from scraper import get_screener_page_data, scrap_function
from combiner import combine_data
import pandas as pd
from multiprocessing import Process
import os
import time
from json import loads, dumps
from dotenv import load_dotenv
from supabase import create_client

if __name__ == "__main__":

  load_dotenv()

  # Connection to Supabase
  url_supabase = os.getenv("SUPABASE_URL")
  key = os.getenv("SUPABASE_KEY")
  supabase = create_client(url_supabase, key)

  # Get the table
  db_data = supabase.table("sgx_companies").select("").execute()
  df_db_data = pd.DataFrame(db_data.data)

  cols = df_db_data.columns.tolist()

  # Get symbol data
  symbol_list = df_db_data['symbol'].tolist()

  start = time.time()

  # Divide to processes
  length_list = len(symbol_list)
  i1 = int(length_list / 4)
  i2 = 2 * i1
  i3 = 3 * i1

  p1 = Process(target=scrap_function, args=(symbol_list[:i1], 1))
  p2 = Process(target=scrap_function, args=(symbol_list[i1:i2], 2))
  p3 = Process(target=scrap_function, args=(symbol_list[i2:i3], 3))
  p4 = Process(target=scrap_function, args=(symbol_list[i3:], 4))

  p1.start()
  p2.start()
  p3.start()
  p4.start()

  p1.join()
  p2.join()
  p3.join()
  p4.join()

  # Merge data
  df_final = combine_data()

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
