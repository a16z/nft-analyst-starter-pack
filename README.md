# NFT Analyst Starter Pack

Enter your Alchemy API key and an NFT contract address, and with one command generate CSV extracts for all token transfers, historical sales, and each underlying item's metadata (with calculated rarity scores).

You can read more from us about what this is, and why it matters, [here](https://a16z.com/2022/03/18/nft-starter-pack-analyze-data-metadata-build-tools).

## Getting Started

You will first need to obtain an Alchemy API key. You can sign up for a free account at [alchemy.com](https://www.alchemy.com/).

Then, install the dependencies.

`poetry install` or `pip3 install -r requirements.txt`

## Export Data

If you're using poetry, remember to prepend this command with `poetry run` or activate the appropriate environment with `poetry shell` while in the `nft-analyst-starter-pack` directory. You can learn more about poetry and how to properly install it [here](https://python-poetry.org/docs/).

```bash
python export_data.py --alchemy-api-key YourAPIKey --contract-address 0xED5AF388653567Af2F388E6224dC7C4b3241C544
```

The first argument is your Alchemy API key and the second argument is the contract address of the NFT collection you want to export (the example provided is Azuki).

### End-to-End Example

```bash
export ALCHEMY_API_KEY=put_your_api_key_here
git clone https://github.com/a16z/nft-analyst-starter-pack && cd nft-analyst-starter-pack
poetry env use python3.9
poetry install
poetry shell
python export_data.py --alchemy-api-key $ALCHEMY_API_KEY --contract-address 0xED5AF388653567Af2F388E6224dC7C4b3241C544
```

## Outputs

The data outputs are stored as three CSV files:

[transfers_0x---.csv](https://github.com/a16z/nft-analyst-starter-pack/blob/main/transfers_0xED5AF388653567Af2F388E6224dC7C4b3241C544.csv)

| transaction_hash                                                   | block_number | date   | asset_id | from_address                               | to_address                                 |
|--------------------------------------------------------------------|--------------|--------|----------|--------------------------------------------|--------------------------------------------|
| 0xaabd9e8ef8c06995ffccf4946215dea21e8c51c9a46af1cb2921fc23390ce775 | 14349209     | 3/8/22 | 2069     | 0x6791102212777d7bce8ed433d54192b7d8af0f9a | 0x5fa6d7ea41d365dba778001d76f092cdedb2eaf1 |
| 0xc74993c3b32f3edff483f2d8fbce7e5dbc55dcc345780a9964561a39cc639f9d | 14349204     | 3/8/22 | 5601     | 0x6791102212777d7bce8ed433d54192b7d8af0f9a | 0x5fa6d7ea41d365dba778001d76f092cdedb2eaf1 |
| 0xcacdccbb2da5660912a62282a2561579b13a53b6348a4319669adcb56eba7a58 | 14349091     | 3/8/22 | 1842     | 0xc310e760778ecbca4c65b6c559874757a4c4ece0 | 0x932e834ff9d2697da014118c85ea5bcb442f3297 |

[sales_0x---.csv](https://github.com/a16z/nft-analyst-starter-pack/blob/main/sales_0xED5AF388653567Af2F388E6224dC7C4b3241C544.csv)

| transaction_hash                                                   | block_number | date   | asset_id | seller                                     | buyer                                      | maker                                      | taker                                      | sale_price_eth | sale_price_usd |
|--------------------------------------------------------------------|--------------|--------|----------|--------------------------------------------|--------------------------------------------|--------------------------------------------|--------------------------------------------|----------------|----------------|
| 0x36243c95335137f0bda0cba15249debeaf3d9216505b5be238236c69c119339f | 14349012     | 3/8/22 | 5601     | 0x90b8c9d44576410725d3e7c892efc54d22334ec9 | 0x6791102212777d7bce8ed433d54192b7d8af0f9a | 0x90b8c9d44576410725d3e7c892efc54d22334ec9 | 0x6791102212777d7bce8ed433d54192b7d8af0f9a | 18             | 44975.8596     |
| 0x522ad16861bfd6ecd7f6000d31f048780cc569f91b576aa98be52a2b6185fca7 | 14349002     | 3/8/22 | 5653     | 0x6791102212777d7bce8ed433d54192b7d8af0f9a | 0x5e7dce19818e31d568d5e4ac087eebd2c37a4746 | 0x5e7dce19818e31d568d5e4ac087eebd2c37a4746 | 0x6791102212777d7bce8ed433d54192b7d8af0f9a | 8.6            | 21488.4662     |
| 0xc4608e2a9823f65f79c826b6913e71ed15249f5a8ff9cdb0088fbdc58fec0454 | 14348885     | 3/8/22 | 3433     | 0xbd74c3f96c38478460a0e3c88ac86dd133af72be | 0xa89542b64941800789f93710ef2d7f0165768c93 | 0xbd74c3f96c38478460a0e3c88ac86dd133af72be | 0xa89542b64941800789f93710ef2d7f0165768c93 | 9.95           | 24861.6557     |

[metadata_0x---.csv](https://github.com/a16z/nft-analyst-starter-pack/blob/main/metadata_0xED5AF388653567Af2F388E6224dC7C4b3241C544.csv)

| asset_id | attribute_count | attribute_count_rarity_score | Type_attribute | Type_rarity_score | Hair_attribute | Hair_rarity_score | Clothing_attribute    | Clothing_rarity_score | Eyes_attribute | Eyes_rarity_score | Mouth_attribute | Mouth_rarity_score | Offhand_attribute | Offhand_rarity_score | Background_attribute | Background_rarity_score | Ear_attribute | Ear_rarity_score | Headgear_attribute | Headgear_rarity_score | Neck_attribute | Neck_rarity_score | Face_attribute | Face_rarity_score | Special_attribute | Special_rarity_score | overall_rarity_score |
|----------|-----------------|------------------------------|----------------|-------------------|----------------|-------------------|-----------------------|-----------------------|----------------|-------------------|-----------------|--------------------|-------------------|----------------------|----------------------|-------------------------|---------------|------------------|--------------------|-----------------------|----------------|-------------------|----------------|-------------------|-------------------|----------------------|----------------------|
| 0        | 7               | 3.18572794                   | Human          | 1.10035211        | Water          | 476.190476        | Pink Oversized Kimono | 140.84507             | Striking       | 24.3309002        | Frown           | 27.2479564         | Monkey King Staff | 68.0272109           | Off White A          | 5.46448087              |               | 1.22458976       |                    | 1.54273372            |                | 1.29349373        |                | 1.47863374        |                   | 1.06746371           | 752.99909            |
| 1        | 7               | 3.18572794                   | Human          | 1.10035211        | Pink Hairband  | 128.205128        | White Qipao with Fur  | 117.647059            | Daydreaming    | 25.5754476        | Lipstick        | 24.3902439         | Gloves            | 87.7192982           | Off White D          | 5.00250125              |               | 1.22458976       |                    | 1.54273372            |                | 1.29349373        |                | 1.47863374        |                   | 1.06746371           | 399.432673           |
| 2        | 7               | 3.18572794                   | Human          | 1.10035211        | Pink Flowy     | 112.359551        | Vest                  | 61.7283951            | Ruby           | 26.1780105        | Chewing         | 27.8551532         |                   | 3.21543408           | Red                  | 9.85221675              | Red Tassel    | 303.030303       |                    | 1.54273372            |                | 1.29349373        |                | 1.47863374        |                   | 1.06746371           | 553.887468           |

*Note: Category rarity scores are calculated as 1 divided by the statistical probability of selecting an item at random from the collection with the given trait. The overall rarity score is the sum of the category rarity scores.

## Processing Time

The script can take up to ~5 minutes to run, depending on the contract's deployment date and the number of tokens in the collection.

## Limitations

(1) Only compatible with Ethereum-based collections

(2) Only compatible with collections supported by Alchemy's NFT API

(3) Sales denominated in non-ETH/WETH values may produce misleading outputs

(4) ETH/USD prices are only tracked at a daily granularity

(5) The script is not compatible with Python >=3.10 at this time

## Related Work and Credits
- [Alchemy](https://www.alchemy.com/): The transfers and sales output is generated using Ethereum JSON-RPC calls, powered by Alchemy. The metadata output is generated from Alchemy's NFT API.
- [ethereum-etl](https://github.com/blockchain-etl/ethereum-etl): A majority of the on-chain data extraction is done using `ethereum-etl` scripts.
- [CoinGecko](https://www.coingecko.com/): Historical ETH/USD prices are extracted using CoinGecko's public API.

## Disclaimer
_This code is being provided as is. No guarantee, representation or warranty is being made, express or implied, as to the safety or correctness of the code. It has not been audited and as such there can be no assurance it will work as intended, and users may experience delays, failures, errors, omissions or loss of transmitted information. Nothing in this repo should be construed as investment advice or legal advice for any particular facts or circumstances and is not meant to replace competent counsel. It is strongly advised for you to contact a reputable attorney in your jurisdiction for any questions or concerns with respect thereto. a16z is not liable for any use of the foregoing, and users should proceed with caution and use at their own risk. See a16z.com/disclosure for more info._
