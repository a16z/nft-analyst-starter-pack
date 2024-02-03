import sys
import warnings
from datetime import datetime, timedelta
import click
from web3 import Web3
import time
import utils
import concurrent.futures

from core.generate_metadata_output import generate_metadata_output
from core.generate_sales_output import generate_sales_output
from core.generate_transfers_output import generate_transfers_output
from jobs.cleanup_outputs import clean_up_outputs
from jobs.export_update_logs import export_update_logs
from jobs.get_nft_metadata_v2 import get_metadata_for_collection
from jobs.get_nft_sales_v2 import get_nft_sales
from jobs.get_nft_transfers_v2 import get_nft_transfers
from jobs.get_recent_block import get_recent_block
from jobs.update_block_to_date_mapping import update_block_to_date_mapping
from jobs.update_eth_prices_v2 import update_eth_prices
from utils.check_contract_support import check_contract_support
from utils.eth_service import EthService
from utils.extract_unique_column_value import extract_unique_column_value


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
    "--contract-addresses",
    required=True,
    type=str,
    help="Comma-separated contract addresses of the desired NFT collections.",
)
def export_data(contract_addresses, alchemy_api_key):
    if (alchemy_api_key is None) or (alchemy_api_key == ""):
        raise Exception("Alchemy API key is required.")

    # Split the contract addresses into a list
    contract_addresses = contract_addresses.split(',')

    warnings.simplefilter(action="ignore", category=FutureWarning)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Launch parallel tasks for each contract address
        futures = [
            executor.submit(
                process_contract,
                contract_address=address,
                alchemy_api_key=alchemy_api_key
            ) for address in contract_addresses
        ]

        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            print(f"Completed processing for: {future.result()}")


def process_contract(contract_address, alchemy_api_key):
    if (alchemy_api_key is None) or (alchemy_api_key == ""):
        raise Exception("Alchemy API key is required.")

    # Convert address to checksummed address (a specific pattern of uppercase and lowercase letters)
    contract_address = Web3.to_checksum_address(contract_address)

    # Check if contract address is supported by Alchemy
    check_contract_support(
        alchemy_api_key=alchemy_api_key, contract_address=contract_address
    )

    warnings.simplefilter(action="ignore", category=FutureWarning)
    print("Process started for contract address: " + str(contract_address))

    # Get current timestamp
    right_now = str(datetime.now().timestamp())

    # Assign file paths (persisting files only)
    date_block_mapping_csv = "./raw-data/date_block_mapping.csv"
    eth_prices_csv = "./raw-data/eth_prices.csv"
    tmp_sales_csv = f"data/tmp/sales_{contract_address}.csv"
    final_sales_csv = f"data/final_sales_{contract_address}.csv"
    metadata_csv = f"data/final_metadata_{contract_address}.csv"
    tmp_transfers_csv = f"data/tmp/transfers_{contract_address}.csv"
    final_transfers_csv = f"data/final_transfers_{contract_address}.csv"
    updates_csv = "./update-logs/" + contract_address + ".csv"
    all_transfers_csv = f"data/tmp/transfers_{contract_address}.csv"

    all_token_ids_txt = f"data/all_token_ids_txt_{contract_address}.csv"
    raw_attributes_csv = "raw_attributes_csv_" + contract_address + ".csv"

    # Initialize retry logic
    max_retries = 1  # Set a maximum number of retries
    retries = 0

    while retries < max_retries:
        try:
            # Set provider
            provider_uri = "https://eth-mainnet.alchemyapi.io/v2/" + alchemy_api_key
            web3 = Web3(Web3.HTTPProvider(provider_uri))
            eth_service = EthService(web3)

            # Get block range
            # If update logs exist, read from the saved file and set the start block
            start_block = get_recent_block(updates_csv, contract_address, web3)

            yesterday = datetime.today() - timedelta(days=1)
            _, end_block = eth_service.get_block_range_for_date(yesterday)

            # If start_block == end_block, then data is already up to date
            if start_block == end_block:
                print("Data is up to date. No updates required.")
                sys.exit(0)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                transfers_future = executor.submit(get_nft_transfers, start_block, end_block, alchemy_api_key,
                                                   contract_address, tmp_transfers_csv)
                sales_future = executor.submit(get_nft_sales, start_block, end_block, alchemy_api_key, contract_address,
                                               tmp_sales_csv)
                # Update date block mapping
                block_update = executor.submit(update_block_to_date_mapping, date_block_mapping_csv, eth_service)
                # Update ETH prices
                eth_price = executor.submit(update_eth_prices, eth_prices_csv)

                # Fetch metadata
                # metadata = executor.submit(get_metadata_for_collection, alchemy_api_key, contract_address,
                #                            raw_attributes_csv)

                # Wait for both tasks to complete
                transfers_future.result()
                sales_future.result()
                block_update.result()
                eth_price.result()
                # metadata.result()

            # Re-generate list of token IDs from consolidated data set
            extract_unique_column_value(
                input_filename=all_transfers_csv,
                output_filename=all_token_ids_txt,
                column="asset_id",
            )

            # Generate sales output
            generate_sales_output(
                sales_file=tmp_sales_csv,
                date_block_mapping_file=date_block_mapping_csv,
                eth_prices_file=eth_prices_csv,
                output=final_sales_csv,
            )

            # Generate transfers output
            generate_transfers_output(
                transfers_file=tmp_transfers_csv,
                date_block_mapping_file=date_block_mapping_csv,
                output=final_transfers_csv,
            )

            # Generate metadata output
            # generate_metadata_output(
            #     raw_attributes_file=raw_attributes_csv,
            #     token_ids_file=all_token_ids_txt,
            #     output=metadata_csv,
            # )

            # Export to update log file
            export_update_logs(
                update_log_file=updates_csv,
                current_block_number=end_block,
            )

            # Consolidate sales and transfers data into final outputs
            clean_up_outputs()

            print("Data exported to transfers.csv, sales.csv and metadata.csv")

        except utils.eth_service.OutOfBoundsError as e:
            print(f"Retry : {retries} - Error: {e}. Retrying in 60 seconds.")
            time.sleep(60)  # Sleep for 60 seconds before retrying
            retries += 1  # Increment the retry counter
            raise e

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            raise e  # Exit the loop on unexpected error

    if retries == max_retries:
        print("Maximum retries reached. Exiting.")
        sys.exit(1)


if __name__ == "__main__":
    export_data()
