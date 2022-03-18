from datetime import datetime, timedelta

import ethereumetl
import numpy as np
import pandas as pd
from ethereumetl.service.eth_service import EthService


def update_block_to_date_mapping(filename, eth_service):
    print("Updating block-to-date mapping...")
    t1 = datetime.today().date() - timedelta(days=1)
    date_block_mapping_df = pd.read_csv(filename)
    last_date_updated = datetime.strptime(
        date_block_mapping_df.iloc[-1]["date"], "%Y-%m-%d"
    ).date()

    days_to_update = t1 - last_date_updated

    date_block_mapping = pd.DataFrame(
        columns=("date", "starting_block", "ending_block")
    )

    for days_prior in range(days_to_update.days):
        date_updated = t1 - timedelta(days=days_prior)
        date_updated_ouput = date_updated.strftime("%Y-%m-%d")
        date_updated_input = (
            datetime.today() - timedelta(days=1) - timedelta(days=days_prior)
        )

        date_range = eth_service.get_block_range_for_date(date_updated_input)

        date_block_mapping = date_block_mapping.append(
            {
                "date": date_updated_ouput,
                "starting_block": date_range[0],
                "ending_block": date_range[1],
            },
            ignore_index=True,
        )

    date_block_mapping.sort_values(by="date", ascending=True, inplace=True)

    if date_block_mapping["date"].size != 0:
        date_block_mapping.to_csv(filename, header=False, index=False, mode="a")
    else:
        pass
