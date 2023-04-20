import numpy as np
import pandas as pd


def generate_transfers_output(transfers_file, date_block_mapping_file, output):
    # Read from transfers file and extract relevant columns
    transfers_df = pd.read_csv(transfers_file)

    # Read from date block mapping file and transpose data
    date_blocks_df = pd.read_csv(date_block_mapping_file)

    date_blocks_df.index = pd.IntervalIndex.from_arrays(
        date_blocks_df["starting_block"], date_blocks_df["ending_block"], closed="both"
    )

    # Join transfers dataframe with date block mapping dataframe
    transfers_df["date"] = transfers_df["block_number"].apply(
        lambda x: date_blocks_df.iloc[date_blocks_df.index.get_loc(x)]["date"]
    )

    # Clean up dataframe for output
    transfers_df = transfers_df.sort_values(
        by=["block_number", "log_index"], ascending=[False, True]
    )

    # ERC-721 transfers; exclude num_tokens field
    transfers_df = transfers_df[
        [
            "transaction_hash",
            "block_number",
            "date",
            "asset_id",
            "from_address",
            "to_address",
            "log_index",
            "value"
        ]
    ]

    # Output transfers data to CSV file
    transfers_df.to_csv(output, index=False)
