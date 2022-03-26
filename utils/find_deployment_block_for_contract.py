from web3 import Web3


def find_deployment_block_for_contract(
    contract_address, web3_interface, latest_block=None
):
    # Binary search for the block number in which a contract was deployed
    left = 0
    right = web3_interface.eth.getBlock(
        "latest" if not latest_block else latest_block
    ).number
    while True:
        if left == right:
            return left

        to_check = (left + right) // 2
        current_block = web3_interface.eth.getCode(
            contract_address, block_identifier=to_check
        )
        if len(current_block) == 0:
            left = to_check + 1
        else:
            right = to_check
