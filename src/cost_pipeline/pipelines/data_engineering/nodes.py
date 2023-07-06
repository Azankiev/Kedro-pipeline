"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.18.10
"""

import pandas as pd

from typing import Callable, Any, Dict
import logging

logger = logging.getLogger(__name__)

def preprocess_accounts_per_org(accounts_per_org: pd.DataFrame) -> pd.DataFrame:

    df = accounts_per_org[['Account ID', 'Name']].copy()
    df = df.rename(columns={'Account ID': 'account_id', 'Name': 'name'})
    df = df.drop_duplicates()

    return df

def add_account_names(df_cur: pd.DataFrame, df_accounts_per_org: pd.DataFrame) -> pd.DataFrame:

    df_cur = df_cur.merge(df_accounts_per_org, how='left', left_on='bill_payer_account_id', right_on='account_id', validate='many_to_one')
    df_cur = df_cur.merge(df_accounts_per_org, how='left', left_on='line_item_usage_account_id', right_on='account_id', validate='many_to_one')

    return df_cur

def merge_cur_partitions(cur_dataset: Dict[str, Callable[[], Any]]) -> pd.DataFrame:

    loaded_partitions = []
    for filename, fileloader in cur_dataset.items():
        logger.info(f'Appending partition {filename} to merged file...')
        loaded_partitions.append(fileloader())
    
    return pd.concat(loaded_partitions)
