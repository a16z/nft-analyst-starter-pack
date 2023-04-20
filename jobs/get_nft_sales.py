import pandas as pd
import requests
from time import sleep


def get_nft_sales(end_block, api_key, contract_address, output):
    # Method for fetching NFT sales using Alchemy's getNFTSales endpoint
    print("Fetching NFT sales...")

    nft_sales = pd.DataFrame(columns=["transaction_hash", "block_number", "asset_id","marketplace","seller","buyer","maker","taker","seller_fee","protocol_fee","royalty_fee","quantity"])
    page_key = None
    process_active = True

    # Loop through collection using pagination tokens until complete
    while process_active:
        if not page_key:
            alchemy_url = "https://eth-mainnet.g.alchemy.com/nft/v2/{api_key}/getNFTSales?fromBlock=0&toBlock={end_block}&order=asc&contractAddress={contract_address}".format(
                api_key=api_key,
                end_block=end_block,
                contract_address=contract_address,
            )
        else:
            alchemy_url = "https://eth-mainnet.g.alchemy.com/nft/v2/{api_key}/getNFTSales?fromBlock=0&toBlock={end_block}&order=asc&contractAddress={contract_address}&pageKey={page_key}".format(
                api_key=api_key,
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
                j = r.json()

                sales = j["nftSales"]
                for sale in sales:
                    try:

                        nft_sales_df = pd.DataFrame(columns=["transaction_hash", "block_number", "asset_id","marketplace","seller","buyer","maker","taker","seller_fee","protocol_fee","royalty_fee","quantity"])
                        
                        transaction_hash = sale["transactionHash"]
                        block_number = sale["blockNumber"]
                        asset_id = sale["tokenId"]
                        marketplace = sale["marketplace"]
                        seller = sale["sellerAddress"]
                        buyer = sale["buyerAddress"]
                        if (sale["taker"] == "BUYER"):
                            taker = sale["buyerAddress"]
                            maker = sale["sellerAddress"]
                        else:
                            taker = sale["sellerAddress"]
                            maker = sale["buyerAddress"]
                        
                        if sale["sellerFee"]["tokenAddress"] in ("0x0000000000000000000000000000000000000000","0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"):
                            seller_fee = float(sale["sellerFee"]["amount"]) / 10**18
                        else:
                            seller_fee = 0

                        if sale["protocolFee"]["tokenAddress"] in ("0x0000000000000000000000000000000000000000","0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"):
                            protocol_fee = float(sale["protocolFee"]["amount"]) / 10**18
                        else:
                            protocol_fee = 0

                        if sale["royaltyFee"]["tokenAddress"] in ("0x0000000000000000000000000000000000000000","0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"):
                            royalty_fee = float(sale["royaltyFee"]["amount"]) / 10**18
                        else:
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
                        "quantity":[quantity]
                        }

                        nft_sales_df =  pd.DataFrame(nft_sales_dict)
                        nft_sales = pd.concat([nft_sales, nft_sales_df], ignore_index=True)
                        
                    except:
                        continue

                try:
                    page_key = j["pageKey"]
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
    nft_sales.to_csv(output, index=False)