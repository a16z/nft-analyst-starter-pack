import json
from datetime import datetime, timedelta
from time import sleep

import ethereumetl
import httpx
import numpy as np
import pandas as pd


def update_eth_prices(filename):
    print("Updating ETH prices...")
    t0 = datetime.today().date()
    eth_prices_df = pd.read_csv(filename)
    last_date_updated = datetime.strptime(
        eth_prices_df.iloc[-1]["date"], "%Y-%m-%d"
    ).date()

    days_to_update = t0 - last_date_updated

    eth_prices = pd.DataFrame(columns=("date", "price_of_eth"))

    for days_prior in range(days_to_update.days):
        date_updated = t0 - timedelta(days=days_prior)
        date_updated_input = str(date_updated.strftime("%d-%m-%Y"))
        date_updated_ouput = str(date_updated.strftime("%Y-%m-%d"))

        sleep(2)
        # CoinGecko API Request
        url = "https://api.coingecko.com/api/v3/coins/ethereum/history?date={date_updated}".format(
            date_updated=date_updated_input
        )
        headers = {
            "Accept": "application/json",
        }
        r = httpx.get(url, headers=headers)
        j = r.json()

        price_of_eth = j["market_data"]["current_price"]["usd"]

        eth_prices = eth_prices.append(
            {"date": date_updated_ouput, "price_of_eth": price_of_eth},
            ignore_index=True,
        )

    eth_prices.sort_values(by="date", ascending=True, inplace=True)

    if eth_prices["date"].size != 0:
        eth_prices.to_csv(filename, header=False, index=False, mode="a")

    else:
        pass
