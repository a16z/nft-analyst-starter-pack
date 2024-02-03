import pandas as pd
import requests
from time import sleep
import time
import os




def save_page_key(page_key, file_path):
    with open(file_path, 'w') as file:
        file.write(page_key)


def load_page_key(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            return file.read().strip()
    return None


def get_nft_transfers(start_block, end_block, api_key, contract_address, output):
    # Method for fetching NFT transfers using Alchemy's alchemy_getAssetTransfers endpoint
    print("Fetching NFT transfers...")
    last_page_key_file = f'update-logs/transfer-last_page_key-{contract_address}.txt'
    page_key = load_page_key(last_page_key_file)
    # page_key = None
    process_active = True

    # Initialize a counter for the records
    record_counter = 0

    # Initialize an empty list to store records before saving to CSV
    records_to_save = []

    # Loop through calls using pagination tokens until complete
    count = 0
    print("Start block : ", start_block, " End Block: ", end_block)
    start_time = time.time()  # Initialize start time for timing

    while process_active:
        count += 1

        nft_transfers = pd.DataFrame(
            columns=[
                "transaction_hash",
                "block_number",
                "asset_id",
                "from_address",
                "to_address",
                "value",
                "log_index",
            ]
        )
        if not page_key:
            alchemy_url = "https://eth-mainnet.g.alchemy.com/v2/{api_key}/".format(
                api_key=api_key,
            )
            post_request_params = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [
                    {
                        "fromBlock": hex(start_block),
                        "toBlock": hex(end_block),
                        "contractAddresses": [contract_address],
                        "category": ["erc721", "erc1155"],
                        "maxCount": "0x3e8",
                    }
                ],
            }


        else:
            alchemy_url = "https://eth-mainnet.g.alchemy.com/v2/{api_key}/".format(
                api_key=api_key,
            )
            post_request_params = {
                "id": 1,
                "jsonrpc": "2.0",
                "method": "alchemy_getAssetTransfers",
                "params": [
                    {
                        "fromBlock": hex(start_block),
                        "toBlock": hex(end_block),
                        "contractAddresses": [contract_address],
                        "category": ["erc721", "erc1155"],
                        "maxCount": "0x3e8",
                        "pageKey": page_key,
                    }
                ],
            }

        # Sometimes requests can randomly fail. Retry 3 times before timing out.
        retries = 3
        for i in range(retries):
            try:
                r = requests.post(alchemy_url, json=post_request_params)
                # print("URL : ", alchemy_url, "PARAMS : ", post_request_params)
                j = r.json()

                transfers = j["result"]["transfers"]
                for transfer in transfers:
                    try:
                        transaction_hash = transfer["hash"]
                        block_number = int(transfer["blockNum"], 16)

                        if transfer["category"] == "erc1155":
                            asset_id = int(
                                transfer["erc1155Metadata"][0]["tokenId"], 16
                            )
                            value = int(transfer["erc1155Metadata"][0]["value"], 16)
                        else:
                            asset_id = int(transfer["erc721TokenId"], 16)
                            value = 1

                        from_address = transfer["from"]
                        to_address = transfer["to"]
                        log_index_substr = len(transfer["uniqueId"]) - 71
                        log_index = transfer["uniqueId"][-log_index_substr:]

                        nft_transfers_dict = {
                            "transaction_hash": [transaction_hash],
                            "block_number": [block_number],
                            "asset_id": [asset_id],
                            "from_address": [from_address],
                            "to_address": [to_address],
                            "value": [value],
                            "log_index": [log_index],
                        }

                        nft_transfers_df = pd.DataFrame(nft_transfers_dict)
                        nft_transfers = pd.concat(
                            [nft_transfers, nft_transfers_df], ignore_index=True
                        )

                        # Append the record to the list
                        records_to_save.append(nft_transfers_df.iloc[0])

                        # Increment the record counter
                        record_counter += 1

                        # Check if we have reached 1000 records
                        if record_counter == 10000:
                            elapsed_time = time.time() - start_time  # Calculate elapsed time

                            start_time = time.time()
                            # Save the records to a CSV file
                            records_df = pd.DataFrame(records_to_save)
                            file_exists = os.path.exists(output)
                            records_df.to_csv(output, mode='a', header=not file_exists, index=False)

                            print(f"Transfers :: Saved 10k records : {len(records_df)}. Time taken: {elapsed_time:.2f} seconds")

                            # Reset the record counter and empty the records list
                            record_counter = 0
                            records_to_save = []
                    except Exception as e:
                        print(f"Transferrs :: error - {e} ")
                        print(f"Transfers :: continue")
                        continue

                try:
                    page_key = j.get("result", {}).get("pageKey")
                    if page_key:
                        save_page_key(page_key, last_page_key_file)
                    else:
                        raise "No PageKey fund"
                except:
                    process_active = False
                    print(f"Transfers :: contract {contract_address} :: Finished")
                    if os.path.exists(last_page_key_file):
                        os.remove(last_page_key_file)
            except KeyError:
                if i < retries - 1:
                    print("Transfers :: Alchemy request failed. Retrying request...")
                    print(f"Transfers ::  Alchemy response -  {j}")
                    sleep(5)
                    continue
                else:
                    raise
            break

    # After the loop, save any remaining records (less than 1000) to the CSV file
    if records_to_save:
        records_df = pd.DataFrame(records_to_save)
        file_exists = os.path.exists(output)
        records_df.to_csv(output, mode='a', header=not file_exists, index=False)

    print(f"Transfers :: Exiting for contract - {contract_address}")
    return "Done"


# Example usage:
# get_nft_transfers(start_block, end_block, api_key, contract_address, output)
