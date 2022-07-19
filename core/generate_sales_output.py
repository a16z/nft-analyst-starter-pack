import numpy as np
import pandas as pd

pd.options.mode.chained_assignment = None


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
                "0x00000000006c3852cbef3e08e8df289169ede581",  # SeaPort 1.1
            ]
        )
    ]

    # Filter for topic0 equal to the kecakk-256 hash of the below function signatures:
    # OrdersMatched(bytes32,bytes32,address,address,uint256,bytes32)
    # TakerAsk(bytes32,uint256,address,address,address,address,address,uint256,uint256,uint256)
    # TakerBid(bytes32,uint256,address,address,address,address,address,uint256,uint256,uint256)
    # Not decoded - 0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33 (X2Y2)
    # Not decoded - 0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31 (SeaPort)

    opensea_v1_signature_hash = (
        "0xc4109843e0b7d514e4c093114b863f8e7d8d9a458c372cd51bfe526b588006c9"
    )
    opensea_v2_signature_hash = (
        "0x68cd251d4d267c6e2034ff0088b990352b97b2002c0476587d0c4da889c11330"
    )
    looksrare_signature_hash = (
        "0x95fb6205e23ff6bda16a2d1dba56b9ad7c783f67c96fa149785052f47696f2be"
    )
    x2y2_signature_hash = (
        "0x3cbb63f144840e5b1b0a38a7c19211d2e89de4d7c5faf8b2d3c1776c302d1d33"
    )
    seaport_v1_1_signature_hash = (
        "0x9d9af8e38d66c62e2c12f0225249fd9d721c54b83f48d9352c97c6cacdcb6f31"
    )

    # Substring mapping is used to parse topic data, which is grouped by 64 hexidecimal characters
    opensea_v1_logs_df = logs_df.loc[
        logs_df["topics"].str[:66] == opensea_v1_signature_hash
    ]
    opensea_v2_logs_df = logs_df.loc[
        logs_df["topics"].str[:66] == opensea_v2_signature_hash
    ]
    looksrare_logs_df = logs_df.loc[
        logs_df["topics"].str[:66] == looksrare_signature_hash
    ]
    x2y2_logs_df = logs_df.loc[logs_df["topics"].str[:66] == x2y2_signature_hash]
    seaport_v1_1_logs_df = logs_df.loc[
        logs_df["topics"].str[:66] == seaport_v1_1_signature_hash
    ]

    # Decode log data for ETH-denominated sale price
    opensea_v1_logs_df["raw_price_eth"] = (
        opensea_v1_logs_df["data"].str[-32:].apply(int, base=16) / 10**18
    )
    opensea_v2_logs_df["raw_price_eth"] = (
        opensea_v2_logs_df["data"].str[-32:].apply(int, base=16) / 10**18
    )
    looksrare_logs_df["raw_price_eth"] = (
        looksrare_logs_df["data"].str[-32:].apply(int, base=16) / 10**18
    )
    x2y2_logs_df["raw_price_eth"] = (
        x2y2_logs_df["data"].str[802:834].apply(int, base=16) / 10**18
    )

    # Seaport log must have a valid recipient
    seaport_v1_1_logs_df = seaport_v1_1_logs_df.loc[
        seaport_v1_1_logs_df["data"].str[66:130]
        != "0000000000000000000000000000000000000000000000000000000000000000"
    ]
    # Seaport log must represent a single-offer sale
    seaport_v1_1_logs_df = seaport_v1_1_logs_df.loc[
        seaport_v1_1_logs_df["data"].str[258:322]
        == "0000000000000000000000000000000000000000000000000000000000000001"
    ]
    # Seaport log must have two or three recipients of consideration (seller, OS wallet, [optional] creator royalty)
    seaport_v1_1_logs_df = seaport_v1_1_logs_df.loc[
        seaport_v1_1_logs_df["data"]
        .str[578:642]
        .isin(
            [
                "0000000000000000000000000000000000000000000000000000000000000002",
                "0000000000000000000000000000000000000000000000000000000000000003",
            ]
        )
    ]

    seaport_v1_1_logs_df["raw_price_eth"] = np.where(
        (
            seaport_v1_1_logs_df["data"].str[322:386]
            == "0000000000000000000000000000000000000000000000000000000000000002"
        )
        & (
            seaport_v1_1_logs_df["data"].str[578:642]
            == "0000000000000000000000000000000000000000000000000000000000000003"
        ),  # Offer is NFT and 3 recipients of consideration
        (
            seaport_v1_1_logs_df["data"].str[834:898].apply(lambda x: int(x,base=16) if x !='' else 0)
            + seaport_v1_1_logs_df["data"].str[1154:1218].apply(lambda x: int(x,base=16) if x !='' else 0)
            + seaport_v1_1_logs_df["data"].str[1474:1538].apply(lambda x: int(x,base=16) if x !='' else 0)
        )
        / 10**18,
        np.where(
            (
                seaport_v1_1_logs_df["data"].str[322:386]
                == "0000000000000000000000000000000000000000000000000000000000000002"
            )
            & (
                seaport_v1_1_logs_df["data"].str[578:642]
                == "0000000000000000000000000000000000000000000000000000000000000002"
            ),  # Offer is NFT and 2 recipients of consideration
            (
                seaport_v1_1_logs_df["data"].str[834:898].apply(lambda x: int(x,base=16) if x !='' else 0)
                + seaport_v1_1_logs_df["data"].str[1154:1218].apply(lambda x: int(x,base=16) if x !='' else 0)
            )
            / 10**18,
            seaport_v1_1_logs_df["data"].str[514:578].apply(lambda x: int(x,base=16) if x !='' else 0)
            / 10**18,  # Else offer is WETH (or other token)
        ),
    )

    # Filter out non-ETH denominated sales on X2Y2 and SeaPort
    x2y2_logs_df = x2y2_logs_df.loc[
        x2y2_logs_df["data"]
        .str[450:514]
        .isin(["0000000000000000000000000000000000000000000000000000000000000000", ""])
    ]
    seaport_v1_1_logs_df = seaport_v1_1_logs_df.loc[
        seaport_v1_1_logs_df["data"]
        .str[1026:1090]
        .isin(
            [
                "0000000000000000000000000000000000000000000000000000000000000000",
                "000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            ]
        )
    ]

    # Extract maker and taker addresses from event logs
    opensea_v1_logs_df["maker"] = "0x" + opensea_v1_logs_df["topics"].str[93:133]
    opensea_v2_logs_df["maker"] = "0x" + opensea_v2_logs_df["topics"].str[93:133]
    looksrare_logs_df["maker"] = "0x" + looksrare_logs_df["topics"].str[160:200]
    x2y2_logs_df["maker"] = "0x" + x2y2_logs_df["data"].str[26:66]
    seaport_v1_1_logs_df["maker"] = "0x" + seaport_v1_1_logs_df["topics"].str[93:133]

    opensea_v1_logs_df["taker"] = "0x" + opensea_v1_logs_df["topics"].str[160:200]
    opensea_v2_logs_df["taker"] = "0x" + opensea_v2_logs_df["topics"].str[160:200]
    looksrare_logs_df["taker"] = "0x" + looksrare_logs_df["topics"].str[93:133]
    x2y2_logs_df["taker"] = "0x" + x2y2_logs_df["data"].str[90:130]
    seaport_v1_1_logs_df["taker"] = "0x" + seaport_v1_1_logs_df["data"].str[90:130]

    opensea_v1_logs_df = opensea_v1_logs_df[
        ["transaction_hash", "data", "topics", "maker", "taker", "raw_price_eth"]
    ]
    opensea_v2_logs_df = opensea_v2_logs_df[
        ["transaction_hash", "data", "topics", "maker", "taker", "raw_price_eth"]
    ]
    looksrare_logs_df = looksrare_logs_df[
        ["transaction_hash", "data", "topics", "maker", "taker", "raw_price_eth"]
    ]
    x2y2_logs_df = x2y2_logs_df[
        ["transaction_hash", "data", "topics", "maker", "taker", "raw_price_eth"]
    ]
    seaport_v1_1_logs_df = seaport_v1_1_logs_df[
        ["transaction_hash", "data", "topics", "maker", "taker", "raw_price_eth"]
    ]

    # Consolidate all marketplaces
    consolidated_logs_df = pd.concat(
        [
            opensea_v1_logs_df,
            opensea_v2_logs_df,
            looksrare_logs_df,
            x2y2_logs_df,
            seaport_v1_1_logs_df,
        ]
    )

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
    sales_df = consolidated_logs_df.merge(
        transfers_df, on=["transaction_hash"], how="inner"
    )
    sales_df = sales_df.merge(eth_prices_df, on="date", how="left")

    # Calculate USD sale price based on ETH price
    sales_df["sale_price_eth"] = sales_df["raw_price_eth"]
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
