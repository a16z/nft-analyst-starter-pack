import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None


def generate_sales_output(
    sales_file, date_block_mapping_file, eth_prices_file, output
):

    sales_df = pd.read_csv(sales_file)
    
    # Read date block mapping and ETH prices from files
    date_blocks_df = pd.read_csv(date_block_mapping_file)
    eth_prices_df = pd.read_csv(eth_prices_file)

    # Transpose data from date block mapping file
    date_blocks_df.index = pd.IntervalIndex.from_arrays(
        date_blocks_df["starting_block"], date_blocks_df["ending_block"], closed="both"
    )

    # Join sales dataframe with date block mapping dataframe
    sales_df["date"] = sales_df["block_number"].apply(
        lambda x: date_blocks_df.iloc[date_blocks_df.index.get_loc(x)]["date"]
    )

    # Join the sales dataframe with ETH price dataframe
    sales_df = sales_df.merge(eth_prices_df, on="date", how="left")

    # Calculate USD sale price based on ETH price
    sales_df["sale_price_eth"] = sales_df["seller_fee"]
    sales_df["sale_price_usd"] = sales_df["sale_price_eth"] * sales_df["price_of_eth"]

    # Clean up dataframe for output
    sales_df = sales_df[
        [
            "transaction_hash",
            "block_number",
            "date",
            "asset_id",
            "marketplace",
            "seller",
            "buyer",
            "maker",
            "taker",
            "sale_price_eth",
            "sale_price_usd",
            "protocol_fee",
            "royalty_fee",
            "quantity",
        ]
    ]

    # Output sales data to CSV file
    sales_df = sales_df.sort_values(by=["block_number"], ascending=False)
    sales_df.to_csv(output, index=False)
