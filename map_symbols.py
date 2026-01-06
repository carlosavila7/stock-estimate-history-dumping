import requests
import json
import logging
import time
import sys
import os

import pandas as pd

log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

file_handler = logging.FileHandler('data/map_symbols.log', mode='a')
file_handler.setFormatter(log_format)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_format)

logger = logging.getLogger('MAP SYMBOLS')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def query_stock_symbol(symbol: str):
    url = "https://services.bingapis.com/contentservices-finance.csautosuggest/api/v1/Query"
    params = {
        "query": symbol,
        "market": "pt-br",
        "count": "1"
    }

    response = requests.get(url, params=params)

    logger.info(f" {response.status_code} - Symbol: {symbol}")
    
    if response.status_code == 200:
        data = response.json()
        return data
    
def main():
    output_path = "data/msn_tickers_mapping"

    tickers_df = pd.read_csv('acoes-listadas-b3.csv')
    tickers = tickers_df['Ticker'].to_list()

    all_results = []
    n_tickers = len(tickers)

    for i, ticker in enumerate(tickers):
        try:
            res = query_stock_symbol(ticker)
            
            if res is None:
                logger.warning(f"No response for {ticker}, skipping.")
                continue

            # Safely check for nested keys to avoid KeyError
            stocks_list = res.get("data", {}).get("stocks", [])
            
            if not stocks_list:
                logger.warning(f"Ticker {ticker} found no matches on MSN.")
                continue
                
            stock_data = json.loads(stocks_list[0])
            
            stock_data.pop("AC042Index", None) 
            stock_data.pop("AliasIndex", None) 
            stock_data.pop("OS001", None) 
            stock_data.pop("OS001Index", None) 
            stock_data.pop("OS01W", None) 
            stock_data.pop("OS01WIndex", None) 
            stock_data.pop("RT0SN", None) 
            stock_data.pop("RT0SNIndex", None) 
            
            all_results.append(stock_data)
            logger.info(f"Successfully processed {ticker} ({i + 1}/{n_tickers})")

        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON for {ticker}")
        except KeyError as e:
            logger.error(f"Missing expected data key for {ticker}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error with {ticker}: {e}")

        df = pd.DataFrame(all_results)
        df.to_csv(f"{output_path}_temp.csv", index=False, encoding="utf-8-sig")

        time.sleep(20)

    if all_results:
        df = pd.DataFrame(all_results)
        df.to_csv(f"{output_path}.csv", index=False, encoding="utf-8-sig")
        
        os.remove(f"{output_path}_temp.csv")
        
        logger.info(f"Done! Saved {len(all_results)} tickers to CSV.")
    else:
        logger.error("No data was collected. CSV not created.")


if __name__ == "__main__":
    main() # Call the main function
