import numpy as np
import pandas as pd


def generate_sales_output(
    transfers_file, logs_file, date_block_mapping_file, eth_prices_file, output
):
    transfers_df = pd.read_csv(transfers_file)
    transfers_df = transfers_df[
        ["transaction_hash", "block_number", "from_address", "to_address", "value"]
    ]
    transfers_df = transfers_df.loc[
        transfers_df["from_address"] != "0x0000000000000000000000000000000000000000"
    ]
    transfers_df = transfers_df.loc[
        transfers_df["to_address"] != "0x0000000000000000000000000000000000000000"
    ]

    logs_df = pd.read_csv(logs_file)

    event_signature_hash = "0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9"  # OrdersMatched()
    logs_df = logs_df.loc[
        logs_df["address"].isin(
            [
                "0x7be8076f4ea4a4ad08075c2508e481d6c946d12b",
                "0x7f268357a8c2552623316e2562d90e642bb538e5",
            ]
        )
    ]
    logs_df = logs_df.loc[logs_df["topics"].str[:66] == event_signature_hash]
    logs_df["maker"] = "0x" + logs_df["topics"].str[93:133]
    logs_df["taker"] = "0x" + logs_df["topics"].str[160:200]
    logs_df = logs_df[["transaction_hash", "data", "topics", "maker", "taker"]]

    date_blocks_df = pd.read_csv(date_block_mapping_file)
    eth_prices_df = pd.read_csv(eth_prices_file)

    date_blocks_df.index = pd.IntervalIndex.from_arrays(
        date_blocks_df["starting_block"], date_blocks_df["ending_block"], closed="both"
    )
    transfers_df["date"] = transfers_df["block_number"].apply(
        lambda x: date_blocks_df.iloc[date_blocks_df.index.get_loc(x)]["date"]
    )

    sales_df = logs_df.merge(transfers_df, on="transaction_hash", how="left")
    sales_df = sales_df.merge(eth_prices_df, on="date", how="left")

    sales_df["sale_price_eth"] = (
        sales_df["data"].str[-32:].apply(int, base=16) / 10**18
    )
    sales_df["sale_price_usd"] = sales_df["sale_price_eth"] * sales_df["price_of_eth"]

    sales_df = sales_df[
        [
            "transaction_hash",
            "block_number",
            "date",
            "value",
            "from_address",
            "to_address",
            "maker",
            "taker",
            "sale_price_eth",
            "sale_price_usd",
        ]
    ]
    sales_df.columns = [
        "transaction_hash",
        "block_number",
        "date",
        "asset_id",
        "seller",
        "buyer",
        "maker",
        "taker",
        "sale_price_eth",
        "sale_price_usd",
    ]

    sales_df = sales_df.loc[
        (sales_df["seller"] == sales_df["maker"])
        | (sales_df["seller"] == sales_df["taker"])
    ]
    sales_df = sales_df.sort_values(by=["block_number"], ascending=False)

    sales_df.to_csv(output, index=False)
