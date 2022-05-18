import ethereumetl
from web3 import Web3
import numpy as np
import pandas as pd


def export_1155_transfers(
    start_block,
    end_block,
    batch_size,
    provider_uri,
    max_workers,
    tokens,
    output,
):
    print("Exporting 1155 transfers...")

    # Set provider
    web3 = Web3(Web3.HTTPProvider(provider_uri))

    # Create dataframe and set parameters for RPC calls
    transfer_logs = pd.DataFrame(
        columns=(
            "transaction_hash",
            "block_number",
            "log_index",
            "value",
            "from_address",
            "to_address",
            "num_tokens",
        )
    )
    first_block = start_block
    last_block = end_block

    # Single Transfers (ERC-1155)
    # Filter for topic0 equal to the kecakk-256 hash of TransferSingle(address,address,address,uint256,uint256)
    event_signature_hash = (
        "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62"
    )

    # Fetch the relevant ERC-1155 transfer event logs in batches
    process_is_active = True
    while process_is_active:
        try:
            event_filter = web3.eth.filter(
                {
                    "address": tokens,
                    "fromBlock": int(first_block),
                    "toBlock": int(last_block),
                    "topics": [event_signature_hash],
                }
            )
            events = event_filter.get_all_entries()

            # Parse the data in each single transfer event log
            for event in events:
                from_address = "0x" + Web3.toHex(event["topics"][2])[-40:]
                to_address = "0x" + Web3.toHex(event["topics"][3])[-40:]
                value = int(event["data"][2:66], 16)
                num_tokens = int(event["data"][-64:], 16)
                transaction_hash = Web3.toHex(event["transactionHash"])
                block_number = event["blockNumber"]
                log_index = event["logIndex"]

                # Append event log data to the main dataframe
                transfer_logs = transfer_logs.append(
                    {
                        "transaction_hash": transaction_hash,
                        "block_number": block_number,
                        "log_index": log_index,
                        "value": value,
                        "from_address": from_address,
                        "to_address": to_address,
                        "num_tokens": num_tokens,
                    },
                    ignore_index=True,
                )

            if last_block == end_block:  # All logs fetched
                process_is_active = False
            else:  # More logs to fetch, reset pagination
                first_block = last_block + 1
                last_block = end_block
                continue

        except:  # Too many results, decrease batch size
            last_block = int(round(first_block + (last_block - first_block) / 2))

    # Batch Transfers (ERC-1155)
    # Filter for topic0 equal to the kecakk-256 hash of TransferBatch(address,address,address,uint256,uint256)
    event_signature_hash = (
        "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb"
    )

    # Reset block range
    first_block = start_block
    last_block = end_block

    # Fetch the relevant ERC-1155 transfer event logs in batches
    process_is_active = True
    while process_is_active:
        try:
            event_filter = web3.eth.filter(
                {
                    "address": tokens,
                    "fromBlock": int(first_block),
                    "toBlock": int(last_block),
                    "topics": [event_signature_hash],
                }
            )
            events = event_filter.get_all_entries()

            # Parse the data in each batch transfer event log
            for event in events:
                # Count the number of token IDs transferred
                count_of_ids = int(event["data"][131:194], 16)

                for i in range(1, count_of_ids + 1):
                    # Find position of each token ID and the number transferred in event data
                    value_start = 2 + 192 + (64 * (i - 1)) + 1
                    num_tokens_start = (
                        value_start
                        + 64
                        + (64 * (count_of_ids - i))
                        + 64
                        + (64 * (i - 1))
                    )

                    # Assign values for each field
                    value = int(event["data"][value_start : value_start + 63], 16)
                    num_tokens = int(
                        event["data"][num_tokens_start : num_tokens_start + 63], 16
                    )
                    from_address = "0x" + Web3.toHex(event["topics"][2])[-40:]
                    to_address = "0x" + Web3.toHex(event["topics"][3])[-40:]
                    transaction_hash = Web3.toHex(event["transactionHash"])
                    block_number = event["blockNumber"]
                    log_index = event["logIndex"]

                    # Append event log data to the main dataframe
                    transfer_logs = transfer_logs.append(
                        {
                            "transaction_hash": transaction_hash,
                            "block_number": block_number,
                            "log_index": log_index,
                            "value": value,
                            "from_address": from_address,
                            "to_address": to_address,
                            "num_tokens": num_tokens,
                        },
                        ignore_index=True,
                    )

            if last_block == end_block:  # All logs fetched
                process_is_active = False
            else:  # More logs to fetch, reset pagination
                first_block = last_block + 1
                last_block = end_block
                continue

        except:  # Too many results, decrease batch size
            last_block = int(round(first_block + (last_block - first_block) / 2))

    # Export data to transfers CSV
    transfer_logs.to_csv(output, index=False)
