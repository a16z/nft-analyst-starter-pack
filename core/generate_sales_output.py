import numpy as np
import pandas as pd


def generate_sales_output(
    transfers_file, logs_file, date_block_mapping_file, eth_prices_file, output
):
    # Read from transfers file and extract relevant columns
    transfers_df = pd.read_csv(transfers_file)

    # Find the first sender and last receiver per tx hash and asset_id (based on log index)
    # This allows us to identify the relevant buyer/seller from multi-sale transactions
    first_transfers_filter = (
        transfers_df.groupby(["transaction_hash", "value"])["log_index"]
        .min()
        .reset_index()
    )
    first_transfers_df = transfers_df.merge(
        first_transfers_filter,
        on=["transaction_hash", "value", "log_index"],
        how="inner",
    )
    first_transfers_df = first_transfers_df[
        ["transaction_hash", "value", "from_address"]
    ]

    last_transfers_filter = (
        transfers_df.groupby(["transaction_hash", "value"])["log_index"]
        .max()
        .reset_index()
    )
    last_transfers_df = transfers_df.merge(
        last_transfers_filter,
        on=["transaction_hash", "value", "log_index"],
        how="inner",
    )
    last_transfers_df = last_transfers_df[["transaction_hash", "value", "to_address"]]

    modified_transfers_df = first_transfers_df.merge(
        last_transfers_df, on=["transaction_hash", "value"], how="inner"
    )
    modified_transfers_df.columns = [
        "transaction_hash",
        "value",
        "seller",
        "buyer",
    ]

    transfers_df = transfers_df.merge(
        modified_transfers_df, on=["transaction_hash", "value"], how="inner"
    )

    if (
        "num_tokens" in transfers_df.columns
    ):  # ERC-1555 transfers; include num_tokens field
        transfers_df = transfers_df[
            [
                "transaction_hash",
                "block_number",
                "seller",
                "buyer",
                "value",
                "num_tokens",
            ]
        ]

    else:  # ERC-721 transfers; exclude num_tokens field
        transfers_df = transfers_df[
            ["transaction_hash", "block_number", "seller", "buyer", "value"]
        ]

    # Drop mints and burns from dataset
    transfers_df = transfers_df.loc[
        transfers_df["seller"] != "0x0000000000000000000000000000000000000000"
    ]
    transfers_df = transfers_df.loc[
        transfers_df["buyer"] != "0x0000000000000000000000000000000000000000"
    ]

    # Read from logs file
    logs_df = pd.read_csv(logs_file)

    # Filter for marketplace contracts
    logs_df = logs_df.loc[
        logs_df["address"].isin(
            [
                "0x7be8076f4ea4a4ad08075c2508e481d6c946d12b",  # OpenSea v1
                "0x7f268357a8c2552623316e2562d90e642bb538e5",  # OpenSea v2
                "0x59728544b08ab483533076417fbbb2fd0b17ce3a",  # LooksRare
                "0x74312363e45dcaba76c59ec49a7aa8a65a67eed3",  # X2Y2 Exchange
            ]
        )
    ]

    # Filter for topic0 equal to the kecakk-256 hash of the below function signatures:
    # OrdersMatched(bytes32,bytes32,address,address,uint256,bytes32)
    # TakerAsk(bytes32,uint256,address,address,address,address,address,uint256,uint256,uint256)
    # TakerBid(bytes32,uint256,address,address,address,address,address,uint256,uint256,uint256)
    # Not decoded - 0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33 (X2Y2)

    event_signature_hash = [
        "0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9",
        "0x68cd251d4d267c6e2034ff0088b990352b97b2002c0476587d0c4da889c11330",
        "0x95fb6205e23ff6bda16a2d1dba56b9ad7c783f67c96fa149785052f47696f2be",
        "0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33",
    ]

    # Substring mapping is used to parse topic data, which is grouped by 64 hexidecimal characters
    logs_df = logs_df.loc[logs_df["topics"].str[:66].isin(event_signature_hash)]

    # Filter out non-ETH denominated sales on X2Y2
    logs_df = logs_df.loc[
        logs_df["data"]
        .str[450:514]
        .isin(["0000000000000000000000000000000000000000000000000000000000000000", ""])
    ]

    # Decode log data for sale price on OpenSea and LooksRare
    logs_df["raw_price_eth"] = logs_df["data"].str[-32:]
    # Decode log data for sale price on X2Y2
    logs_df["raw_price_eth"] = logs_df["raw_price_eth"].mask(
        logs_df["topics"].str[:66]
        == "0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33",
        other=(logs_df["data"].str[802:834]),
    )

    # Extract maker and taker addresses from event logs
    # Note: OpenSea log has topic1 = maker, LooksRare log has topic1 = taker, X2Y2 log has maker/taker stored in data
    logs_df["maker"] = np.where(
        logs_df["topics"].str[:66]
        == "0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9",  # OpenSea log
        "0x" + logs_df["topics"].str[93:133],
        np.where(
            logs_df["topics"].str[:66]
            == "0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33",  # X2Y2 log
            "0x" + logs_df["data"].str[26:66],
            "0x" + logs_df["topics"].str[160:200],  # Else LooksRare logs
        ),
    )
    logs_df["taker"] = np.where(
        logs_df["topics"].str[:66]
        == "0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9",  # OpenSea log
        "0x" + logs_df["topics"].str[160:200],
        np.where(
            logs_df["topics"].str[:66]
            == "0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33",  # X2Y2 log
            "0x" + logs_df["data"].str[90:130],
            "0x" + logs_df["topics"].str[93:133],  # Else LooksRare logs
        ),
    )

    logs_df = logs_df[
        ["transaction_hash", "data", "topics", "maker", "taker", "raw_price_eth"]
    ]

    # Read date block mapping and ETH prices from files
    date_blocks_df = pd.read_csv(date_block_mapping_file)
    eth_prices_df = pd.read_csv(eth_prices_file)

    # Transpose data from date block mapping file
    date_blocks_df.index = pd.IntervalIndex.from_arrays(
        date_blocks_df["starting_block"], date_blocks_df["ending_block"], closed="both"
    )

    # Join transfers dataframe with date block mapping dataframe
    transfers_df["date"] = transfers_df["block_number"].apply(
        lambda x: date_blocks_df.iloc[date_blocks_df.index.get_loc(x)]["date"]
    )

    # Join the transfers data and ETH price data to the sales dataframe
    sales_df = logs_df.merge(transfers_df, on=["transaction_hash"], how="inner")
    sales_df = sales_df.merge(eth_prices_df, on="date", how="left")

    # Convert sale price data to ETH-denominated number format
    sales_df["sale_price_eth"] = (
        sales_df["raw_price_eth"].apply(int, base=16) / 10**18
    )

    # Calculate USD sale price based on ETH price
    sales_df["sale_price_usd"] = sales_df["sale_price_eth"] * sales_df["price_of_eth"]

    # Clean up dataframe for output
    if "num_tokens" in sales_df.columns:  # ERC-1555 transfers; include num_tokens field
        sales_df = sales_df[
            [
                "transaction_hash",
                "block_number",
                "date",
                "value",
                "seller",
                "buyer",
                "maker",
                "taker",
                "sale_price_eth",
                "sale_price_usd",
                "num_tokens",
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
            "tokens_sold",
        ]
    else:  # ERC-721 transfers; exclude num_tokens field
        sales_df = sales_df[
            [
                "transaction_hash",
                "block_number",
                "date",
                "value",
                "seller",
                "buyer",
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

    # Seller must be either the maker or taker; this addresses issues with aggregators
    sales_df = sales_df.loc[
        (sales_df["seller"] == sales_df["maker"])
        | (sales_df["seller"] == sales_df["taker"])
    ]

    # If there are multiple sales with the same buyer/seller in the same tx hash, we must drop it from dataset given the lack of granularity in event logs
    sales_df = sales_df.drop_duplicates()  # First drop exact duplicates
    sales_df = sales_df.drop_duplicates(
        subset=["transaction_hash", "buyer", "seller"], keep=False
    )

    # Output sales data to CSV file
    sales_df = sales_df.sort_values(by=["block_number"], ascending=False)
    sales_df.to_csv(output, index=False)
