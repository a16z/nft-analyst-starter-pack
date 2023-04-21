# NFT Analyst Starter Pack

Enter your Alchemy API key and an NFT contract address, and with one command generate CSV extracts for all token transfers, historical sales, and each underlying item's metadata (with calculated rarity scores).

You can read more from us about what this is, and why it matters, [here](https://a16z.com/2022/03/18/nft-starter-pack-analyze-data-metadata-build-tools).

## Getting Started

You will first need to obtain an Alchemy API key. You can sign up for a free account at [alchemy.com](https://www.alchemy.com/).

Then, install the dependencies.

`poetry install` or `pip3 install -r requirements.txt`

## Export Data

If you're using poetry, remember to prepend this command with `poetry run` or activate the appropriate environment with `poetry shell` while in the `nft-analyst-starter-pack` directory. You can learn more about poetry and how to properly install it [here](https://python-poetry.org/docs/). If using Docker you can spin up a containerized environment with `docker-compose up` and cd into the container to run commands from the CLI. You can learn more about Docker Compose [here](https://docs.docker.com/compose/).

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

|transaction_hash                                                  |block_number|date   |asset_id|from_address                              |to_address                                |log_index|value|
|------------------------------------------------------------------|------------|-------|--------|------------------------------------------|------------------------------------------|---------|-----|
|0xda1c66592b56293d37b16b4c72222e6c2ea8768db8480860132ad9b0d723c719|17090650    |4/20/23|1200    |0x29c0f446b983d002a8d110aee7e904426da5327f|0x7d88f221e9ecef9eef118e0d0e8a47f81b37e0b6|178      |1    |
|0xe9cc5b02af90369061dcf42cd696c30bd0ef4e378d88400645322eb65dc51f03|17090502    |4/20/23|4693    |0xeecba4834ca010c0ced9b2ed8b7050f5229667b8|0x53bf035f62f27dd99f8afc867abf328c218aaa03|439      |1    |
|0x1d87dbb54ee80ff34a775412db433b5d6ce3b3c40153345b53c9acb234be67e8|17090095    |4/20/23|5406    |0x703229ec1005fba3108883b8a094ea206fc1e161|0xe52cec0e90115abeb3304baa36bc2655731f7934|364      |1    |


[sales_0x---.csv](https://github.com/a16z/nft-analyst-starter-pack/blob/main/sales_0xED5AF388653567Af2F388E6224dC7C4b3241C544.csv)

|transaction_hash                                                  |block_number|date   |asset_id|marketplace|seller                                    |buyer                                     |maker                                     |taker                                     |sale_price_eth|sale_price_usd|protocol_fee_eth|protocol_fee_usd|royalty_fee_eth|royalty_fee_usd|quantity|
|------------------------------------------------------------------|------------|-------|--------|-----------|------------------------------------------|------------------------------------------|------------------------------------------|------------------------------------------|--------------|--------------|----------------|----------------|---------------|---------------|--------|
|0xda1c66592b56293d37b16b4c72222e6c2ea8768db8480860132ad9b0d723c719|17090650    |4/20/23|1200    |blur       |0x29c0f446b983d002a8d110aee7e904426da5327f|0x7d88f221e9ecef9eef118e0d0e8a47f81b37e0b6|0x29c0f446b983d002a8d110aee7e904426da5327f|0x7d88f221e9ecef9eef118e0d0e8a47f81b37e0b6|16.29         |31544.37825   |0               |0               |0.08145        |157.7218912    |1       |
|0xe9cc5b02af90369061dcf42cd696c30bd0ef4e378d88400645322eb65dc51f03|17090502    |4/20/23|4693    |seaport    |0xeecba4834ca010c0ced9b2ed8b7050f5229667b8|0x53bf035f62f27dd99f8afc867abf328c218aaa03|0x53bf035f62f27dd99f8afc867abf328c218aaa03|0xeecba4834ca010c0ced9b2ed8b7050f5229667b8|17.7375       |34347.35477   |0.4125          |798.7756923     |0.825          |1597.551385    |1       |
|0xbdb62c189c11ca8e16c8c56a41ffad2e27875407aabdf1c8fd3cb9cc505966cf|17089646    |4/20/23|8516    |blur       |0xdc2d12cdefe1b3b9060692a5d47015d219286077|0xfa89ec40699bbfd749c4eb6643dc2b22ff0e2aa6|0xdc2d12cdefe1b3b9060692a5d47015d219286077|0xfa89ec40699bbfd749c4eb6643dc2b22ff0e2aa6|15.16         |29356.21696   |0               |0               |0.758          |1467.810848    |1       |



[metadata_0x---.csv](https://github.com/a16z/nft-analyst-starter-pack/blob/main/metadata_0xED5AF388653567Af2F388E6224dC7C4b3241C544.csv)

|asset_id|attribute_count|attribute_count_rarity_score|Type_attribute|Type_rarity_score|Hair_attribute|Hair_rarity_score|Clothing_attribute   |Clothing_rarity_score|Eyes_attribute|Eyes_rarity_score|Mouth_attribute|Mouth_rarity_score|Offhand_attribute|Offhand_rarity_score|Background_attribute|Background_rarity_score|Ear_attribute|Ear_rarity_score|Headgear_attribute|Headgear_rarity_score|Neck_attribute|Neck_rarity_score|Face_attribute|Face_rarity_score|Special_attribute|Special_rarity_score|overall_rarity_score|
|--------|---------------|----------------------------|--------------|-----------------|--------------|-----------------|---------------------|---------------------|--------------|-----------------|---------------|------------------|-----------------|--------------------|--------------------|-----------------------|-------------|----------------|------------------|---------------------|--------------|-----------------|--------------|-----------------|-----------------|--------------------|--------------------|
|0       |7              |3.160556258                 |Human         |1.108893324      |Water         |476.1904762      |Pink Oversized Kimono|140.8450704          |Striking      |24.50980392      |Frown          |27.54820937       |Monkey King Staff|68.96551724         |Off White A         |5.512679162            |             |1.222344457     |                  |1.535626536          |              |1.290988897      |              |1.47275405       |                 |1.067121972         |754.4300418         |
|1       |7              |3.160556258                 |Human         |1.108893324      |Pink Hairband |129.8701299      |White Qipao with Fur |119.047619           |Daydreaming   |25.83979328      |Lipstick       |24.50980392       |Gloves           |90.09009009         |Off White D         |5.025125628            |             |1.222344457     |                  |1.535626536          |              |1.290988897      |              |1.47275405       |                 |1.067121972         |405.2408473         |
|2       |7              |3.160556258                 |Human         |1.108893324      |Pink Flowy    |114.9425287      |Vest                 |62.5                 |Ruby          |26.24671916      |Chewing        |28.49002849       |                 |3.159557662         |Red                 |9.940357853            |Red Tassel   |303.030303      |                  |1.535626536          |              |1.290988897      |              |1.47275405       |                 |1.067121972         |557.945436          |


*Note: Category rarity scores are calculated as 1 divided by the statistical probability of selecting an item at random from the collection with the given trait. The overall rarity score is the sum of the category rarity scores.

## Processing Time

The script can take up to ~5 minutes to run, depending on the contract's deployment date and the number of tokens in the collection.

## Limitations

(1) Only compatible with Ethereum-based collections

(2) Only compatible with collections supported by Alchemy's NFT API

(3) Only includes sales denominated in ETH/WETH

(4) ETH/USD prices are only tracked at a daily granularity

## Related Work and Credits
- [Alchemy](https://www.alchemy.com/): The transfers output is generated using Alchemy's Transfers API. The sales and metadata outputs are generated using Alchemy's NFT API.
- [CoinGecko](https://www.coingecko.com/): Historical ETH/USD prices are extracted using CoinGecko's public API.

## Disclaimer
_This code is being provided as is. No guarantee, representation or warranty is being made, express or implied, as to the safety or correctness of the code. It has not been audited and as such there can be no assurance it will work as intended, and users may experience delays, failures, errors, omissions or loss of transmitted information. Nothing in this repo should be construed as investment advice or legal advice for any particular facts or circumstances and is not meant to replace competent counsel. It is strongly advised for you to contact a reputable attorney in your jurisdiction for any questions or concerns with respect thereto. a16z is not liable for any use of the foregoing, and users should proceed with caution and use at their own risk. See a16z.com/disclosure for more info._
