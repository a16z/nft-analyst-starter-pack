from time import sleep

import pandas as pd
import requests


def get_metadata_for_collection(api_key, contract_address, output):
    # Method for fetching metadata using Alchemy's getNFTsForCollection endpoint
    print("Fetching NFT metadata...")

    raw_attributes = pd.DataFrame(columns=["value", "trait_type", "asset_id"])
    start_token = None
    process_active = True

    # Loop through collection using pagination tokens until complete
    while process_active:
        if not start_token:
            alchemy_url = "https://eth-mainnet.g.alchemy.com/v2/{api_key}/getNFTsForCollection?contractAddress={contract_address}&withMetadata=true&refreshCache=true".format(
                api_key=api_key,
                contract_address=contract_address,
            )
        else:
            alchemy_url = "https://eth-mainnet.g.alchemy.com/v2/{api_key}/getNFTsForCollection?contractAddress={contract_address}&withMetadata=true&refreshCache=true&startToken={start_token}".format(
                api_key=api_key,
                contract_address=contract_address,
                start_token=start_token,
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

                nft_list = j["nfts"]
                for nft in nft_list:
                    try:
                        attributes_raw = nft["metadata"]["attributes"]
                        attributes_df = pd.DataFrame(attributes_raw)
                        attributes_df["asset_id"] = int(nft["id"]["tokenId"], 16)
                        attributes_df = attributes_df[
                            ["value", "trait_type", "asset_id"]
                        ]
                        raw_attributes = pd.concat(
                            [raw_attributes, attributes_df], ignore_index=True
                        )

                    except:
                        continue

                try:
                    start_token = int(j["nextToken"], 16)
                except:
                    process_active = False
            except KeyError:
                if i < retries - 1:
                    print("Alchemy request failed. Retrying request...")
                    sleep(5)
                    continue
                else:
                    raise
            break

    # Output attributes data to CSV file
    raw_attributes.to_csv(output, index=False)
