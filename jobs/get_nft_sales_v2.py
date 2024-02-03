from time import sleep
import time

import pandas as pd
import requests
import os


def get_nft_sales(start_block, end_block, api_key, contract_address, output):
    # Method for fetching NFT sales using Alchemy's getNFTSales endpoint
    print("Fetching NFT sales...")
    page_key = None
    process_active = True

    # Loop through collection using pagination tokens until complete
    count = 0
    record_counter = 0  # Initialize a counter for the records
    records_to_save = []  # Initialize an empty list to store records before saving to CSV
    start_time = time.time()

    nft_sales = pd.DataFrame(
        columns=[
            "transaction_hash",
            "block_number",
            "asset_id",
            "marketplace",
            "seller",
            "buyer",
            "maker",
            "taker",
            "seller_fee",
            "protocol_fee",
            "royalty_fee",
            "quantity",
        ]
    )

    while process_active:
        count += 1
        if not page_key:
            alchemy_url = "https://eth-mainnet.g.alchemy.com/nft/v2/{api_key}/getNFTSales?fromBlock={start_block}&toBlock={end_block}&order=asc&contractAddress={contract_address}".format(
                api_key=api_key,
                start_block=start_block,
                end_block=end_block,
                contract_address=contract_address,
            )
        else:
            alchemy_url = "https://eth-mainnet.g.alchemy.com/nft/v2/{api_key}/getNFTSales?fromBlock={start_block}&toBlock={end_block}&order=asc&contractAddress={contract_address}&pageKey={page_key}".format(
                api_key=api_key,
                start_block=start_block,
                end_block=end_block,
                contract_address=contract_address,
                page_key=page_key,
            )

        headers = {
            "Accept": "application/json",
        }

        # Sometimes requests can randomly fail. Retry 3 times before timing out.
        retries = 3
        for i in range(retries):
            try:
                r = requests.get(alchemy_url, headers=headers)
                # print("URL : ", alchemy_url, "HEADERS : ", headers)
                j = r.json()

                sales = j["nftSales"]
                for sale in sales:
                    try:
                        transaction_hash = sale["transactionHash"]
                        block_number = sale["blockNumber"]
                        asset_id = sale["tokenId"]
                        marketplace = sale["marketplace"]
                        seller = sale["sellerAddress"]
                        buyer = sale["buyerAddress"]
                        if sale["taker"] == "BUYER":
                            taker = sale["buyerAddress"]
                            maker = sale["sellerAddress"]
                        else:
                            taker = sale["sellerAddress"]
                            maker = sale["buyerAddress"]

                        try:
                            # Trade currency must be ETH, WETH, or Blur Pool Token
                            if sale["sellerFee"]["tokenAddress"] in (
                                "0x0000000000000000000000000000000000000000",
                                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                                "0x0000000000a39bb272e79075ade125fd351887ac",
                            ):
                                seller_fee = (
                                    float(sale["sellerFee"]["amount"]) / 10**18
                                )
                            else:
                                seller_fee = 0
                        except:
                            seller_fee = 0

                        try:
                            # Trade currency must be ETH, WETH, or Blur Pool Token
                            if sale["protocolFee"]["tokenAddress"] in (
                                "0x0000000000000000000000000000000000000000",
                                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                                "0x0000000000a39bb272e79075ade125fd351887ac",
                            ):
                                protocol_fee = (
                                    float(sale["protocolFee"]["amount"]) / 10**18
                                )
                            else:
                                protocol_fee = 0
                        except:
                            protocol_fee = 0

                        try:
                            # Trade currency must be ETH, WETH, or Blur Pool Token
                            if sale["royaltyFee"]["tokenAddress"] in (
                                "0x0000000000000000000000000000000000000000",
                                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                                "0x0000000000a39bb272e79075ade125fd351887ac",
                            ):
                                royalty_fee = (
                                    float(sale["royaltyFee"]["amount"]) / 10**18
                                )
                            else:
                                royalty_fee = 0
                        except:
                            royalty_fee = 0

                        quantity = sale["quantity"]

                        nft_sales_dict = {
                            "transaction_hash": [transaction_hash],
                            "block_number": [block_number],
                            "asset_id": [asset_id],
                            "marketplace": [marketplace],
                            "seller": [seller],
                            "buyer": [buyer],
                            "maker": [maker],
                            "taker": [taker],
                            "seller_fee": [seller_fee],
                            "protocol_fee": [protocol_fee],
                            "royalty_fee": [royalty_fee],
                            "quantity": [quantity],
                        }

                        nft_sales_df = pd.DataFrame(nft_sales_dict)
                        nft_sales = pd.concat(
                            [nft_sales, nft_sales_df], ignore_index=True
                        )
                    except Exception as e:
                        print(e)
                        continue

                    record_counter += 1
                    if record_counter == 10000:
                        elapsed_time = time.time() - start_time  # Calculate elapsed time
                        print(f"Sales :: Saved {len(nft_sales)} records. Time taken: {elapsed_time:.2f} seconds")

                        # Save the records to a CSV file
                        file_exists = os.path.exists(output)
                        nft_sales.to_csv(output, mode='a', header=not file_exists, index=False)
                        nft_sales = pd.DataFrame(
                            columns=[
                                "transaction_hash",
                                "block_number",
                                "asset_id",
                                "marketplace",
                                "seller",
                                "buyer",
                                "maker",
                                "taker",
                                "seller_fee",
                                "protocol_fee",
                                "royalty_fee",
                                "quantity",
                            ]
                        )

                        # Reset the timer, record counter, and records list for the next batch
                        start_time = time.time()
                        record_counter = 0

                try:
                    page_key = j["pageKey"]
                except Exception as e:
                    print(f"Finished Sales processing for contract : {contract_address}")
                    # Save the records to a CSV file
                    file_exists = os.path.exists(output)
                    nft_sales.to_csv(output, mode='a', header=not file_exists, index=False)
                    process_active = False
            except KeyError:
                if i < retries - 1:
                    print("Alchemy request failed. Retrying request...")
                    sleep(5)
                    continue
                else:
                    raise
            break

    print(f"Sales :: Exiting for contract - {contract_address}")
    return "Done"
