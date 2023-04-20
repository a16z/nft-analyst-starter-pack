import pandas as pd
import requests
from time import sleep


def get_nft_transfers(start_block, end_block, api_key, contract_address, output):
    # Method for fetching NFT transfers using Alchemy's alchemy_getAssetTransfers endpoint
    print("Fetching NFT transfers...")

    nft_transfers = pd.DataFrame(columns=["transaction_hash", "block_number", "asset_id","from_address","to_address","value","log_index"])
    page_key = None
    process_active = True

    # Loop through calls using pagination tokens until complete
    while process_active:
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
                    "maxCount": "0x3e8"
                    }
                ]
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
                    "pageKey": page_key
                    }
                ]
            }

        # Sometimes requests can randomly fail. Retry 3 times before timing out.
        retries = 3
        for i in range(retries):
            try:
                r = requests.post(alchemy_url, json = post_request_params)
                j = r.json()

                transfers = j["result"]["transfers"]
                for transfer in transfers:
                    try:

                        nft_transfers_df = pd.DataFrame(columns=["transaction_hash", "block_number", "asset_id","from_address","to_address","value","log_index"])
                        
                        transaction_hash = transfer["hash"]
                        block_number = int(transfer["blockNum"],16)
                        asset_id = int(transfer["tokenId"],16)
                        from_address = transfer["from"]
                        to_address = transfer["to"]
                        value = transfer["value"]
                        log_index_substr = len(transfer["uniqueId"])-71
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

                        nft_transfers_df =  pd.DataFrame(nft_transfers_dict)
                        nft_transfers = pd.concat([nft_transfers, nft_transfers_df], ignore_index=True)
                        
                    except:
                        continue

                try:
                    page_key = j["result"]["pageKey"]
                except:
                    process_active = False
            except KeyError as e:
                if i < retries - 1:
                    print("Alchemy request failed. Retrying request...")
                    sleep(5)
                    continue
                else:
                    raise
            break

    # Output attributes data to CSV file
    nft_transfers.to_csv(output, index=False)

