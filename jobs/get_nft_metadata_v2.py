import pandas as pd
import requests
import os
import time


def get_metadata_for_collection(api_key, contract_address, output):
    print("Fetching NFT metadata...")
    start_token = None
    token_file = f'update-logs/metadata-last_token-{contract_address}.txt'

    # Check if there's a saved start_token from a previous run
    try:
        with open(token_file, 'r') as f:
            start_token = f.read()
    except FileNotFoundError:
        pass  # No saved token, start from the beginning

    process_active = True
    start_time = time.time()

    while process_active:
        alchemy_url = f"https://eth-mainnet.g.alchemy.com/v2/{api_key}/getNFTsForCollection?contractAddress={contract_address}&withMetadata=true&refreshCache=true"
        if start_token:
            alchemy_url += f"&startToken={start_token}"

        headers = {"Accept": "application/json"}
        response = requests.get(alchemy_url, headers=headers)
        data = response.json()

        nft_list = data.get("nfts", [])
        for nft in nft_list:
            if 'metadata' in nft and 'attributes' in nft['metadata']:
                attributes_raw = nft['metadata']['attributes']
                if attributes_raw:  # Check if attributes are not empty
                    attributes_df = pd.DataFrame(attributes_raw)
                    if 'value' in attributes_df.columns and 'trait_type' in attributes_df.columns:
                        attributes_df["asset_id"] = int(nft["id"]["tokenId"], 16)
                        attributes_df = attributes_df[["value", "trait_type", "asset_id"]]

                        with open(output, 'a') as f:
                            print(f"writing to {f}")
                            attributes_df.to_csv(f, header=f.tell() == 0, index=False)

        start_token = data.get("nextToken")
        if not start_token:
            process_active = False

        # Save the last start_token to a file
        with open(token_file, 'w') as f:
            f.write(start_token)

    if not process_active:
        # Remove the token file if process completed successfully
        if os.path.exists(token_file):
            os.remove(token_file)

    return "Done"

# Example usage
# get_metadata_for_collection(api_key, contract_address, 'output.csv')
