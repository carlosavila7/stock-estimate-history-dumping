import requests
import logging
import json
import time
import os
import sys
import pandas as pd

from dotenv import load_dotenv
from datetime import datetime

from stock_estimates_db import StockEstimatesDB

log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

file_handler = logging.FileHandler('data/dump_stock_estimates.log', mode='a')
file_handler.setFormatter(log_format)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_format)

logger = logging.getLogger('DUMP STOCK ESTIMATES')
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def get_symbol_id(symbol):
    df = pd.read_csv('./data/msn_tickers_mapping.csv')
    return df[df['RT00T'] == symbol]['SecId']


def get_stock_estimate(symbol):
    stock_id = get_symbol_id(symbol)
    if stock_id is None:
        return None 
    
    url = "https://assets.msn.com/service/Finance/QuoteSummary"

    apiKey = os.getenv("API_KEY")
    params = {
        "apikey": apiKey,
        "ocid": "finance-utils-peregrine",
        "cm": "pt-br",
        "scn": "ANON",
        "ids": stock_id,
        "intents": "Quotes,QuoteDetails",
        "wrapodata": "false"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        return data

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def main():
    service = StockEstimatesDB()

    tickers_df = pd.read_csv('acoes-listadas-b3.csv')
    tickers = tickers_df['Ticker'].to_list()

    n_tickers = len(tickers)

    for i, ticker in enumerate(tickers):
        res = get_stock_estimate(ticker)

        if res is None:
            logger.info(f"Not found {ticker} ({i + 1}/{n_tickers})")
            continue

        if res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('recommendation') is None:
            logger.info(f"Not relevant information (recomendation) {ticker} ({i + 1}/{n_tickers})")
            continue

        row = {
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": res[0]['quote']['symbol'],
            "price": res[0]['quote']['price'],
            "estimateCurrency": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('currency'),
            "numberOfAnalysts": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('numberOfAnalysts'),
            "recommendationRate": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('recommendationRate'),
            "recommendation": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('recommendation'),
            "numberOfPriceTargets": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('numberOfPriceTargets'),
            "meanPriceTarget": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('meanPriceTarget'),
            "highPriceTarget": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('highPriceTarget'),
            "lowPriceTarget": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('lowPriceTarget'),
            "medianPriceTarget": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('medianPriceTarget'),
            "strongBuy": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('analystRecommendation', {}).get("strongBuy"),
            "sell": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('analystRecommendation', {}).get("sell"),
            "hold": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('analystRecommendation', {}).get("hold"),
            "buy": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('analystRecommendation', {}).get("buy"),
            "underperform": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get('analystRecommendation', {}).get("underperform"),
            "consensusPriceVolatility": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get("consensusPriceVolatility"),
            "dateLastUpdated": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get("dateLastUpdated"),
            "industryDateLastUpdated": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get("industryDateLastUpdated"),
            "pricevolatilityDateLastUpdated": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get("pricevolatilityDateLastUpdated"),
            "consensusIndustryRecommendation": res[0].get('equity', {}).get('analysis', {}).get('estimate', {}).get("consensusIndustryRecommendation"),
            "rawResponse": json.dumps(res[0])
        }

        service.insert_estimate(row)
        logger.info(f"Successfully processed {ticker} ({i + 1}/{n_tickers})")

        time.sleep(20)


if __name__ == "__main__":
    load_dotenv()
    main()  # Call the main function
