import sys

# Check for Python version <= 3.10
if sys.version_info[0] == 3 and sys.version_info[1] >= 10:
    raise Exception("Python >=3.10 is not supported at this time.")

import asyncio
import os
import sys
import tempfile
import warnings
from datetime import datetime, timedelta

import click
import ethereumetl
import numpy as np
import pandas as pd
from ethereumetl.service.eth_service import EthService
from web3 import Web3

from core.generate_metadata_output import generate_metadata_output
from core.generate_sales_output import generate_sales_output
from core.generate_transfers_output import generate_transfers_output
from jobs.export_logs import export_logs
from jobs.export_token_transfers import export_token_transfers
from jobs.export_1155_transfers import export_1155_transfers
from jobs.get_nft_metadata import get_metadata_for_collection
from jobs.update_block_to_date_mapping import update_block_to_date_mapping
from jobs.update_eth_prices import update_eth_prices
from utils.check_contract_support import check_contract_support
from utils.extract_unique_column_value import extract_unique_column_value
from utils.find_deployment_block_for_contract import find_deployment_block_for_contract


# Set click CLI parameters
@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-a",
    "--alchemy-api-key",
    required=True,
    type=str,
    help="The Alchemy API key to use for data extraction.",
)
@click.option(
    "-c",
    "--contract-address",
    required=True,
    type=str,
    help="The contract address of the desired NFT collection.",
)
def export_data(contract_address, alchemy_api_key):

    if (alchemy_api_key is None) or (alchemy_api_key == ""):
        raise Exception("Alchemy API key is required.")

    # Convert address to checksummed address
    contract_address = Web3.toChecksumAddress(contract_address)

    # Check if contract address is supported by Alchemy
    check_contract_support(
        alchemy_api_key=alchemy_api_key, contract_address=contract_address
    )

    warnings.simplefilter(action="ignore", category=FutureWarning)
    print("Process started for contract address: " + str(contract_address))

    # Assign file paths (persisting files only)
    date_block_mapping_csv = "./raw-data/date_block_mapping.csv"
    eth_prices_csv = "./raw-data/eth_prices.csv"
    sales_csv = "sales_" + contract_address + ".csv"
    metadata_csv = "metadata_" + contract_address + ".csv"
    transfers_csv = "transfers_" + contract_address + ".csv"

    # Set provider
    provider_uri = "https://eth-mainnet.alchemyapi.io/v2/" + alchemy_api_key
    web3 = Web3(Web3.HTTPProvider(provider_uri))
    eth_service = EthService(web3)
    ethereum_etl_batch_size = 1000
    ethereum_etl_max_workers = 8

    # Get block range
    start_block = find_deployment_block_for_contract(contract_address, web3)
    print(
        "Contract {} appears to have been deployed at block {}".format(
            contract_address, start_block
        )
    )
    yesterday = datetime.today() - timedelta(days=1)
    _, end_block = eth_service.get_block_range_for_date(yesterday)

    # Create tempfiles
    with tempfile.NamedTemporaryFile(
        delete=False
    ) as logs_csv, tempfile.NamedTemporaryFile(
        delete=False
    ) as transaction_hashes_txt, tempfile.NamedTemporaryFile(
        delete=False
    ) as token_ids_txt, tempfile.NamedTemporaryFile(
        delete=False
    ) as raw_attributes_csv:

        # Export token transfers
        export_token_transfers(
            start_block=start_block,
            end_block=end_block,
            batch_size=ethereum_etl_batch_size,
            provider_uri=provider_uri,
            max_workers=ethereum_etl_max_workers,
            tokens=contract_address,
            output=transfers_csv,
        )

        # If there are no 721 transfers, export 1155 transfers
        if os.stat(transfers_csv).st_size == 0:
            print(
                "No ERC-721 transfers were identified.",
                "Therefore, searching for and extracting any ERC-1155 transfers.",
            )
            export_1155_transfers(
                start_block=start_block,
                end_block=end_block,
                batch_size=ethereum_etl_batch_size,
                provider_uri=provider_uri,
                max_workers=ethereum_etl_max_workers,
                tokens=contract_address,
                output=transfers_csv,
            )

        # Create staging files
        extract_unique_column_value(
            input_filename=transfers_csv,
            output_filename=transaction_hashes_txt.name,
            column="transaction_hash",
        )

        extract_unique_column_value(
            input_filename=transfers_csv,
            output_filename=token_ids_txt.name,
            column="value",
        )

        # Export logs
        export_logs(
            start_block=start_block,
            end_block=end_block,
            batch_size=ethereum_etl_batch_size,
            provider_uri=provider_uri,
            max_workers=ethereum_etl_max_workers,
            tx_hashes_filename=transaction_hashes_txt.name,
            output=logs_csv.name,
        )

        # Update date block mapping
        update_block_to_date_mapping(
            filename=date_block_mapping_csv, eth_service=eth_service
        )

        # Update ETH prices
        update_eth_prices(filename=eth_prices_csv)

        # Generate sales output
        generate_sales_output(
            transfers_file=transfers_csv,
            logs_file=logs_csv.name,
            date_block_mapping_file=date_block_mapping_csv,
            eth_prices_file=eth_prices_csv,
            output=sales_csv,
        )

        # Generate transfers output
        generate_transfers_output(
            transfers_file=transfers_csv,
            date_block_mapping_file=date_block_mapping_csv,
            output=transfers_csv,
        )

        # Fetch metadata
        get_metadata_for_collection(
            api_key=alchemy_api_key,
            contract_address=contract_address,
            output=raw_attributes_csv.name,
        )

        # Generate metadata output
        generate_metadata_output(
            raw_attributes_file=raw_attributes_csv.name,
            token_ids_file=token_ids_txt.name,
            output=metadata_csv,
        )

        print("Data exported to transfers.csv, sales.csv and metadata.csv")


if __name__ == "__main__":
    export_data()
