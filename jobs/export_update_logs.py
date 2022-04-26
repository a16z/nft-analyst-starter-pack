import numpy as np
import pandas as pd
import os
from datetime import datetime


def export_update_logs(update_log_file, current_block_number=0):
    print("Checking cached block number for the contract...")

    # get current date
    last_date_updated = datetime.now().strftime("%Y-%m-%d")

    # check if file exists
    if os.path.isfile(update_log_file):
        # read from existing file and find the date last updated
        recent_block_df = pd.read_csv(update_log_file)
        last_block_number = recent_block_df.iloc[-1]["most_recent_block_number"]
    else:
        # if file does not exist then create the file
        print(
            "Cache file doesn't exist, writing log with recent block number for the contract..."
        )
        last_block_number = current_block_number
        recent_block_df = pd.DataFrame(
            np.array([[last_date_updated, last_block_number]]),
            columns=("last_updated", "most_recent_block_number"),
        )
        recent_block_df.to_csv(update_log_file, header=True, index=False)

    # if current block number is greater than last block number, update the file
    if current_block_number > last_block_number:
        # update the file
        print("Updating most recent block number for the contract...")
        recent_block_df = recent_block_df.append(
            {
                "most_recent_block_number": current_block_number,
                "last_updated": last_date_updated,
            },
            ignore_index=True,
        )
        recent_block_df.sort_values(
            by="most_recent_block_number", ascending=True, inplace=True
        )
        recent_block_df.to_csv(update_log_file, header=True, index=False)
    else:
        print("Cached block number is up to date")
        pass
