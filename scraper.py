from bs4 import BeautifulSoup
from requests_html import HTMLSession
import json
import logging
import requests
import os

# Set the logging level for the 'websockets' logger
logging.getLogger('websockets').setLevel(logging.WARNING)

# If you need to configure logging for requests-html as well
logging.getLogger('requests_html').setLevel(logging.WARNING)


# Setup Constant 
SCREENER_API_URL = "https://api.sgx.com/stockscreener/v1.0/all?params=exchange%2CexchangeCountryCode%2CcompanyName%2CstockCode%2CricCode%2CmarketCapitalization%2CsalesTTM%2CpriceToEarningsRatio%2CdividendYield%2CfourWeekPricePercentChange%2CthirteenWeekPricePercentChange%2CtwentySixWeekPricePercentChange%2CfiftyTwoWeekPricePercentChange%2CnetProfitMargin%2CreturnOnAvgCommonEquity%2CpriceToCashFlowPerShareRatio%2CtotalDebtToTotalEquityRatio%2CsalesPercentageChange%2Csector%2CpriceToBookRatio%2CpriceCurrCode"
BASE_URL = "https://investors.sgx.com/_security-types/stocks/"

def get_screener_page_data() -> bytes | None:
  try:
    res = requests.get(SCREENER_API_URL)
    if (res.status_code == 200):
      # Make it to JSON
      json_data = json.loads(res.text)
      # Get the data only
      return json_data['data']
  except Exception as e:
    print(f"Failed to get API from {SCREENER_API_URL}: {e}")
    return None


def get_url(symbol: str) -> str:
  return f"{BASE_URL}{symbol}"

def read_page(url: str) -> BeautifulSoup | None:
  try:
    session = HTMLSession()
    response = session.get(url)
    response.html.render(sleep=2, timeout=10)

    soup = BeautifulSoup(response.html.html, "html.parser")
    return soup
  except Exception as e:
    print(f"Failed to open {url}: {e}")
    return None
  finally:
    session.close()
    print(f"Session in {url} is closed")

def scrap_stock_page(symbol: str) -> dict | None:
  url = get_url(symbol)
  soup = read_page(url)

  industry = None
  sector = None

  if (soup is not None):
    # Get Industry
    try:
      industry = soup.find("span", {"class": "widget-security-details-general-industry"}).get_text()
      if (industry is not None and len(industry) > 0): # Handling empty string or not found
        industry = industry.replace("Industry: ", "")
      else:
        industry = None
    except:
      print(f"Failed to get Industry data from {url}")
      industry = None
    
    # Get Sector
    try:
      collapsible = soup.findAll("div", {"class": "cmp-collapsible-two-column-col"})
      for elm in collapsible:
        collapsible_title = elm.find("div", {"class" : "cmp-collapsible-two-column-item-title"}).find("span").get_text()
        if (collapsible_title == "Sector"):
          sector = elm.find("div", {"class": "cmp-collapsible-two-column-item-value"}).get_text()
    except:
      print(f"Failed to get Sector data from {url}")
      sector = None

    stock_data = dict()
    stock_data['stock_code'] = symbol
    stock_data['industry'] = industry
    stock_data['sector'] = sector

    print(f"Successfully scrap from {symbol} stock page")
    return stock_data
  else:
    print(f"None type of BeautifulSoup")
    return None

def scrap_function(symbol_list, process_idx):
  print(f"==> Start scraping from process P{process_idx}")
  all_data = []
  cwd = os.getcwd()
  start_idx = 0
  count = 0

  # Iterate in symbol list
  for i in range(start_idx, len(symbol_list)):

    symbol = symbol_list[i]['stock_code']
    if (symbol is not None):
      scrapped_data = scrap_stock_page(symbol)
      all_data.append(scrapped_data)

    if (i % 10 == 0 and count != 0):
      print(f"CHECKPOINT || P{process_idx} {i} Data")
    
    count += 1
  
  # Save last
  filename = f"P{process_idx}_data.json"
  print(f"==> Finished data is exported in {filename}")
  file_path = os.path.join(cwd, "data", filename)

  # Save to JSON
  with open(file_path, "w") as output_file:
    json.dump(all_data, output_file, indent=2)

  return all_data