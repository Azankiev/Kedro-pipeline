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

def _lazy_add_account_names(cur_loader: Callable([], pd.DataFrame), df_accounts_per_org: pd.DataFrame) -> pd.DataFrame:

    df_cur = cur_loader()
    df_cur = df_cur.merge(df_accounts_per_org, how='left', left_on='bill_payer_account_id', validate='many_to_one')
    df_cur = df_cur.merge(df_accounts_per_org, how='left', left_on='line_item_usage_account_id', validate='many_to_one')

    return df_cur

def add_account_names(cur_dataset: Dict[str, Callable[[], Any]], df_accounts_per_org: pd.DataFrame) -> Dict[str, Callable[[], Any]]:

    all_files = list(cur_dataset.items())
    logger.info(f'Found {all_files.shape[0]} to load.')

    return {
        fname: lambda vars=[cur_loader, df_accounts_per_org]: _lazy_add_account_names(vars[0], vars[1]) 
            for fname, cur_loader in all_files
    } 

