from datetime import datetime, timedelta
from time import sleep
import pandas as pd
import requests


def update_eth_prices(filename):
    print("Updating ETH prices...")

    # Load existing data
    eth_prices_df = pd.read_csv(filename)
    last_date_updated = datetime.strptime(eth_prices_df.iloc[-1]["date"], "%Y-%m-%d").date()

    # Start from the day after the last update
    start_date = last_date_updated + timedelta(days=1)
    end_date = datetime.today().date()

    # Iterate through each day from start_date to end_date
    current_date = start_date
    while current_date <= end_date:
        try:
            # Format date for API call
            date_for_api = current_date.strftime("%d-%m-%Y")
            date_for_output = current_date.strftime("%Y-%m-%d")

            # CoinGecko API Request
            url = f"https://api.coingecko.com/api/v3/coins/ethereum/history?date={date_for_api}"
            headers = {"Accept": "application/json"}

            # Make the API request
            response = requests.get(url, headers=headers, timeout=90)
            response.raise_for_status()  # Raise an error for bad responses

            # Extract the ETH price from the API response
            price_of_eth = response.json()["market_data"]["current_price"]["usd"]

            # Append the new price data to the CSV file
            with open(filename, 'a', newline='') as f:
                f.write(f"{date_for_output},{price_of_eth}\n")

            print(f"Updated ETH price for {current_date}: ${price_of_eth}")

            # Sleep for 10 seconds to avoid hitting API rate limits
            sleep(10)

            # Move to the next day
            current_date += timedelta(days=1)

        except Exception as e:
            print(f"Error updating ETH price for {current_date}: {e}, Stopping ETH update")
            return "Done"
