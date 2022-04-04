import ethereumetl
import numpy as np
import pandas as pd
from ethereumetl.jobs.export_token_transfers_job import ExportTokenTransfersJob
from ethereumetl.jobs.exporters.token_transfers_item_exporter import (
    token_transfers_item_exporter,
)
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.web3_utils import build_web3


def export_token_transfers(
    start_block, end_block, batch_size, provider_uri, max_workers, tokens, output
):
    print("Exporting token transfers...")
    # Export token transfers using Ethereum ETL script
    job = ExportTokenTransfersJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        web3=ThreadLocalProxy(lambda: build_web3(get_provider_from_uri(provider_uri))),
        item_exporter=token_transfers_item_exporter(output),
        max_workers=max_workers,
        tokens=tokens,
    )
    job.run()
