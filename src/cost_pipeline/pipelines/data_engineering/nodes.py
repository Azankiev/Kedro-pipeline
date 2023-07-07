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

    logger.info('Adding account names...')
    df_cur = df_cur.merge(df_accounts_per_org, how='left', left_on='bill_payer_account_id', right_on='account_id', suffixes=('', '_payer'), validate='many_to_one')
    df_cur = df_cur.rename(columns={'name': 'payer_account_name'})
    df_cur = df_cur.merge(df_accounts_per_org, how='left', left_on='line_item_usage_account_id', right_on='account_id', suffixes=('', '_usage'), validate='many_to_one')
    df_cur = df_cur.rename(columns={'name': 'usage_account_name'})

    return df_cur

def merge_cur_partitions(cur_dataset: Dict[str, Callable[[], Any]]) -> pd.DataFrame:

    loaded_partitions = []
    for filename, fileloader in cur_dataset.items():
        logger.info(f'Appending partition {filename} to merged file...')
        loaded_partitions.append(fileloader())
    
    return pd.concat(loaded_partitions)

def aggregate_invoice_account_products(df_cur_merged: pd.DataFrame) -> pd.DataFrame:

    logger.info('Aggregating CUR by invoice, bill date, account ID and product...')
    df_cur_merged = df_cur_merged.groupby(by=['bill_invoice_id', 'bill_billing_entity', 'bill_invoicing_entity', 
                                              'bill_payer_account_id', 'payer_account_name', 'line_item_usage_account_id', 'usage_account_name',
                                              'line_item_line_item_type', 'line_item_usage_start_date', 
                                              'line_item_usage_end_date', 'product_product_name', 'line_item_currency_code'])['line_item_unblended_cost'].sum().reset_index()   
    logger.info('Aggregation is done.')
    
    return df_cur_merged
