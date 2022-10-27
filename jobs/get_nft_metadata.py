import asyncio

import httpx
import numpy as np
import pandas as pd
import requests
from time import sleep


async def get_item(client, url, asset_id):
    # Subprocess for asynchronous API calls
    try:
        resp = await client.get(url)
        item = resp.json()

        if not bool(item["metadata"]):
            print("Metadata not found for asset #" + str(asset_id))

        elif not item["metadata"]["attributes"]:
            print("No attributes for asset #" + str(asset_id))

        else:
            attributes_raw = item["metadata"]["attributes"]
            attributes_df = pd.DataFrame(attributes_raw)
            attributes_df["asset_id"] = asset_id
            attributes_df = attributes_df[["value", "trait_type", "asset_id"]]
            return attributes_df

    except:
        print("Request failed for asset #" + str(asset_id) + " (retry pending)")
        attributes_df = pd.DataFrame(columns=["value", "trait_type", "asset_id"])
        attributes_df = attributes_df.append(
            {"value": np.nan, "trait_type": np.nan, "asset_id": asset_id},
            ignore_index=True,
        )
        return attributes_df


async def get_nft_metadata(token_ids_filename, api_key, contract_address, output):
    # Method for fetching data using asynchronous API calls
    print("Fetching NFT metadata...")
    token_ids = open(token_ids_filename).readlines()
    nft_attributes = pd.DataFrame(columns=["value", "trait_type", "asset_id"])

    limits = httpx.Limits(max_keepalive_connections=None, max_connections=None)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = []
        for asset_id in token_ids:
            asset_id = int(asset_id)
            alchemy_url = "https://eth-mainnet.g.alchemy.com/{api_key}/v1/getNFTMetadata?contractAddress={contract_address}&tokenId={asset_id}".format(
                api_key=api_key,
                contract_address=contract_address,
                asset_id=asset_id,
            )
            headers = {
                "Accept": "application/json",
            }
            tasks.append(asyncio.ensure_future(get_item(client, alchemy_url, asset_id)))

        original_item = await asyncio.gather(*tasks)

        for item in original_item:
            nft_attributes = nft_attributes.append(item, ignore_index=True)

        nft_attributes.to_csv(output, index=False)


def retry_requests(raw_attributes_filename, api_key, contract_address):
    # Method for retrying failed async requests using synchronous API calls

    raw_attributes = pd.read_csv(raw_attributes_filename)
    failed_requests = raw_attributes.loc[raw_attributes["trait_type"].isnull()]
    assets_not_fetched = failed_requests["asset_id"].tolist()
    raw_attributes = raw_attributes[raw_attributes["trait_type"].notnull()]

    if len(assets_not_fetched) > 0:
        print("Retrying for: " + str(assets_not_fetched))
        # Retry with synchronous requests for any failed asset ids
        for asset_id in assets_not_fetched:
            try:
                asset_id = int(asset_id)
                alchemy_url = "https://eth-mainnet.g.alchemy.com/{api_key}/v1/getNFTMetadata?contractAddress={contract_address}&tokenId={asset_id}".format(
                    api_key=api_key,
                    contract_address=contract_address,
                    asset_id=asset_id,
                )
                headers = {
                    "Accept": "application/json",
                }
                r = httpx.get(alchemy_url, headers=headers)
                j = r.json()
                attributes_raw = j["metadata"]["attributes"]
                attributes_df = pd.DataFrame(attributes_raw)
                attributes_df["asset_id"] = asset_id
                attributes_df = attributes_df[["value", "trait_type", "asset_id"]]
                raw_attributes = raw_attributes.append(attributes_df, ignore_index=True)
                print("Retry successful for asset #" + str(asset_id))
            except Exception as e:
                print("Retry failed for asset #" + str(asset_id))
    else:
        print("All assets fetched.")

    raw_attributes.to_csv(raw_attributes_filename, index=False)


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
                        raw_attributes = raw_attributes.append(
                            attributes_df, ignore_index=True
                        )
                    except:
                        continue

                try:
                    start_token = int(j["nextToken"], 16)
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
    raw_attributes.to_csv(output, index=False)
