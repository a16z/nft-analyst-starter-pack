import numpy as np
import pandas as pd
import os
from datetime import datetime


def export_update_logs(update_log_file, current_block_number=0):
    print("Writing update logs...")
    # Get current date
    last_date_updated = datetime.now().strftime("%Y-%m-%d")

    # Check if update log file exists
    if os.path.isfile(update_log_file):
        # Read from existing file and find the latest block number
        recent_block_df = pd.read_csv(update_log_file)
        last_block_number = recent_block_df.iloc[-1]["most_recent_block_number"]
    else:
        # If file does not exist, create the file
        last_block_number = current_block_number
        recent_block_df = pd.DataFrame(
            np.array([[last_date_updated, last_block_number]]),
            columns=("last_updated", "most_recent_block_number"),
        )
        recent_block_df.to_csv(update_log_file, header=True, index=False)

    # If current block number is greater than last block number, append to the file
    if current_block_number > last_block_number:
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
        pass
