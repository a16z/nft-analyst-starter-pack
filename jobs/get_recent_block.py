import pandas as pd
import os
from utils.find_deployment_block_for_contract import find_deployment_block_for_contract


def get_recent_block(block_file, contract_address, web3):
    print("Getting most recent cached block number for the contract...")

    # check if file exists
    if os.path.isfile(block_file):
        # read from existing file and find the date last updated
        recent_block_df = pd.read_csv(block_file)
        most_recent_block = recent_block_df.iloc[-1]["most_recent_block_number"]
    else:
        # if file does not exist find deployment block
        most_recent_block = find_deployment_block_for_contract(contract_address, web3)

        print(
            "Contract {} appears to have been deployed at block {}".format(
                contract_address, most_recent_block
            )
        )

    return most_recent_block
