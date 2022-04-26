import pandas as pd
import os
from utils.find_deployment_block_for_contract import find_deployment_block_for_contract


def get_recent_block(update_log_file, contract_address, web3):
    print("Checking update logs for most recent block...")

    # Check if update log file exists
    if os.path.isfile(update_log_file):
        # Read from existing file and find the latest block number
        recent_block_df = pd.read_csv(update_log_file)
        most_recent_block = recent_block_df.iloc[-1]["most_recent_block_number"]
        print("Starting with block " + str(most_recent_block))
    else:
        # If file does not exist, find contract deployment block
        most_recent_block = find_deployment_block_for_contract(contract_address, web3)

        print(
            "No existing data. Contract {} appears to have been deployed at block {}".format(
                contract_address, most_recent_block
            )
        )

    return most_recent_block
