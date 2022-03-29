import ethereumetl
import numpy as np
import pandas as pd
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.jobs.exporters.receipts_and_logs_item_exporter import (
    receipts_and_logs_item_exporter,
)
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy


def export_logs(
    start_block,
    end_block,
    batch_size,
    provider_uri,
    max_workers,
    tx_hashes_filename,
    output,
):
    print("Exporting logs...")
    # Export event logs using Ethereum ETL script
    with open(tx_hashes_filename, "r") as transaction_hashes_file:
        job = ExportReceiptsJob(
            transaction_hashes_iterable=(
                tx_hash.strip() for tx_hash in transaction_hashes_file
            ),
            batch_size=batch_size,
            batch_web3_provider=ThreadLocalProxy(
                lambda: get_provider_from_uri(provider_uri, batch=True)
            ),
            max_workers=max_workers,
            item_exporter=receipts_and_logs_item_exporter(None, output),
            export_receipts=None,
            export_logs=output,
        )

        job.run()
