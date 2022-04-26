import pandas as pd
import os


def clean_up_outputs():

    # Find all csv files and organize by data type, contract, and run date
    csv_files = [f for f in os.listdir(".") if f.endswith(".csv")]

    filetypes = []
    contracts = []
    run_dates = []

    for c in csv_files:
        filetypes.append(c.split("_")[0].split(".")[0])
        contracts.append(c.split("_")[1].split(".")[0])

        try:
            run_dates.append(c.split("_")[2].split(".")[0])
        except:
            run_dates.append("N/A")

    df = pd.DataFrame(
        {
            "filetype": filetypes,
            "contract": contracts,
            "run_date": run_dates,
            "file": csv_files,
        }
    )

    # Get unique contracts with existing sales or transfer data
    unique_contracts = df[
        (df["filetype"] == "sales") | (df["filetype"] == "transfers")
    ]["contract"].unique()

    # Consolidate sales and transfers data into final output CSVs
    for uc in unique_contracts:
        clean_sales_csv = "sales_" + uc + ".csv"
        clean_transfers_csv = "transfers_" + uc + ".csv"

        sales_df = pd.DataFrame()
        transfers_df = pd.DataFrame()

        transfer_files = df[(df["contract"] == uc) & (df["filetype"] == "transfers")][
            "file"
        ]
        sales_files = df[(df["contract"] == uc) & (df["filetype"] == "sales")]["file"]

        for t in transfer_files:
            transfers_df = pd.concat([transfers_df, pd.read_csv(t)])

        for s in sales_files:
            sales_df = pd.concat([sales_df, pd.read_csv(s)])

        # Remove historical files
        for s in sales_files:
            os.remove(s)
        for t in transfer_files:
            os.remove(t)

        # Export to final output csv files
        transfers_df.sort_values(by=["block_number"], ascending=False).to_csv(
            clean_transfers_csv, index=False
        )
        sales_df.sort_values(by=["block_number"], ascending=False).to_csv(
            clean_sales_csv, index=False
        )
