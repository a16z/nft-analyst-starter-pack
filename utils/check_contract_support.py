import json

import httpx


def check_contract_support(alchemy_api_key, contract_address):
    # Do a single request to check if the contract address is supported by Alchemy's NFT API
    alchemy_url = "https://eth-mainnet.g.alchemy.com/v2/{alchemy_api_key}/getNFTMetadata?contractAddress={contract_address}&tokenId=1".format(
        alchemy_api_key=alchemy_api_key,
        contract_address=contract_address,
    )
    headers = {
        "Accept": "application/json",
    }
    r = httpx.get(alchemy_url, headers=headers)
    j = r.json()
    contract_check = j["id"]["tokenMetadata"]["tokenType"]

    if contract_check == "UNKNOWN":
        raise Exception(
            "Contract address {contract_address} not supported. \n".format(
                contract_address=contract_address
            )
            + "See https://docs.alchemy.com/alchemy/enhanced-apis/nft-api#what-nfts-are-supported for more information."
        )
    else:
        pass
