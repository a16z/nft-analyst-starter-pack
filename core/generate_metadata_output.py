import copy

import numpy as np
import pandas as pd


def generate_metadata_output(raw_attributes_file, token_ids_file, output):
    # Calculate statistical rarity across attribute count, traits, and categories
    raw_attributes = pd.read_csv(raw_attributes_file)
    raw_attributes = raw_attributes[raw_attributes["trait_type"].notnull()]

    token_ids = open(token_ids_file).readlines()
    num_tokens = len(token_ids)

    attribute_count = (
        raw_attributes.groupby("asset_id").size().reset_index(name="attribute_count")
    )
    attribute_count_rarity = (
        attribute_count.groupby("attribute_count")
        .size()
        .reset_index(name="count_rarity")
    )
    attribute_count_rarity["attribute_count_rarity_score"] = 1 / (
        attribute_count_rarity["count_rarity"] / (num_tokens)
    )

    trait_rarity = (
        raw_attributes.groupby(["trait_type", "value"])
        .size()
        .reset_index(name="trait_rarity")
    )
    trait_rarity["trait_rarity_score"] = 1 / (
        trait_rarity["trait_rarity"] / (num_tokens)
    )

    category_rarity = (
        trait_rarity[["trait_type", "value", "trait_rarity"]]
        .groupby("trait_type")
        .sum("trait_rarity")
        .reset_index()
    )
    category_rarity["category_none_score"] = 1 / (
        ((num_tokens) - category_rarity["trait_rarity"]) / (num_tokens)
    )

    # Construct traits dataframe and export to CSV
    categories = raw_attributes[["asset_id", "value", "trait_type"]]
    categories = categories.merge(
        trait_rarity[["trait_type", "value", "trait_rarity_score"]],
        on=["trait_type", "value"],
        how="left",
    )
    nft_df = copy.deepcopy(attribute_count)
    nft_df = nft_df.merge(
        attribute_count_rarity[["attribute_count", "attribute_count_rarity_score"]],
        on="attribute_count",
        how="left",
    )

    categories["trait_type"] = categories["trait_type"].str.replace(
        " ", "_", regex=True
    )
    categories["trait_type"] = categories["trait_type"].str.replace("(", "", regex=True)
    categories["trait_type"] = categories["trait_type"].str.replace(")", "", regex=True)
    category_rarity["trait_type"] = category_rarity["trait_type"].str.replace(
        " ", "_", regex=True
    )
    category_rarity["trait_type"] = category_rarity["trait_type"].str.replace(
        "(", "", regex=True
    )
    category_rarity["trait_type"] = category_rarity["trait_type"].str.replace(
        ")", "", regex=True
    )

    distinct_trait_types = categories["trait_type"].unique()

    df_dict = {}
    for name in distinct_trait_types:
        df_dict[name] = pd.DataFrame()
        df_dict[name] = categories[(categories["trait_type"] == name)]

    for name in distinct_trait_types:
        nft_df = nft_df.merge(df_dict[name], on="asset_id", how="left")

    base_column_names = ["asset_id", "attribute_count", "attribute_count_rarity_score"]
    trait_column_names = []

    for name in distinct_trait_types:
        trait_column_names.append(str(name) + "_attribute")
        trait_column_names.append(str(name))
        trait_column_names.append(str(name) + "_rarity_score")

    column_names = base_column_names + trait_column_names
    nft_df.columns = column_names

    category_none_scores = category_rarity[["trait_type", "category_none_score"]]

    for name in distinct_trait_types:
        nft_df[str(name) + "_rarity_score"] = nft_df[
            str(name) + "_rarity_score"
        ].fillna(
            value=category_none_scores.loc[
                category_none_scores["trait_type"] == name, "category_none_score"
            ].iloc[0]
        )

    nft_df["overall_rarity_score"] = nft_df[
        [col for col in nft_df.columns if col.endswith("_rarity_score")]
    ].sum(axis=1)

    for name in distinct_trait_types:
        nft_df.drop(columns=[name], axis=1, inplace=True)

    nft_df = nft_df.drop_duplicates(subset=["asset_id"])

    nft_df.to_csv(output, index=False)
