"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.18.10
"""

import pandas as pd

from typing import Callable, Any, Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

def preprocess_accounts_per_org(accounts_per_org: pd.DataFrame) -> pd.DataFrame:
    account_id_digits = 12

    df = accounts_per_org[['Account ID', 'Name']].copy()
    df = df.rename(columns={'Account ID': 'account_id', 'Name': 'name'})
    df['account_id'] = df['account_id'].astype(str)
    df['account_id'] = df['account_id'].str.pad(account_id_digits, side='left', fillchar='0')
    df = df.drop_duplicates()

    return df

def _lazy_add_account_names(fname: str, df_cur_loader: Callable[[], Any], df_accounts_per_org: pd.DataFrame) -> pd.DataFrame:

    df_cur = df_cur_loader()
    logger.info(f'Adding account_names to [{fname}]...')
    df_cur = df_cur.merge(df_accounts_per_org, how='left', left_on='bill_payer_account_id', right_on='account_id', suffixes=('', '_payer'), validate='many_to_one')
    df_cur = df_cur.rename(columns={'name': 'payer_account_name'})
    df_cur = df_cur.merge(df_accounts_per_org, how='left', left_on='line_item_usage_account_id', right_on='account_id', suffixes=('', '_usage'), validate='many_to_one')
    df_cur = df_cur.rename(columns={'name': 'usage_account_name'})
    logger.info(f'Account names added to [{fname}].')

    return df_cur


def add_account_names(cur_dataset: Dict[str, Callable[[], Any]], df_accounts_per_org: pd.DataFrame) -> pd.DataFrame:

    logger.info('Adding account names to CUR files...')
    all_files = sorted(list(cur_dataset.items()), key=lambda x: x[0])
    logger.info(f'Found {len(all_files)} to add account names.')

    return {
        fname: lambda vars=[fname, loader, df_accounts_per_org]: _lazy_add_account_names(vars[0], vars[1], vars[2])
            for fname, loader in all_files
    }


def _lazy_aggregate_invoice_account_products(fname, group: List[Callable[[], Any]]) -> pd.DataFrame:

    df_partitions = []
    for df_loader in group:
        df_partitions.append(df_loader())
    
    df_cur = pd.concat(df_partitions)

    logger.info(f'[{fname}] Aggregating CUR by invoice, bill date, account ID and product...')
    agg_mapping = {'line_item_unblended_cost':'sum', 'discount_spp_discount': 'sum', 'discount_total_discount': 'sum'}
    df_cur = df_cur.groupby(by=['bill_invoice_id', 'bill_billing_period_start_date', 'bill_billing_period_end_date',
                                                'bill_billing_entity', 'bill_invoicing_entity', 
                                                'bill_payer_account_id', 'line_item_usage_account_id',
                                                'line_item_line_item_type', 'line_item_usage_start_date', 
                                                'line_item_usage_end_date', 'product_product_name', 
                                                'line_item_currency_code']).agg(agg_mapping).reset_index()   
    logger.info(f'[{fname}] Aggregation is done.')
    
    return df_cur

def _group_month_year_extractor(fpath: str):

    year_month, _ = fpath.split('-')
    year_month = f'{year_month}.csv'

    return year_month

def _group_month_year(cur_dataset: List[Tuple[str, Callable[[], Any]]]) -> Dict[str, Callable[[], Any]]:

    month_year_groups = {}

    logger.info('Grouping files by year-month...')
    for fpath, df_loader in cur_dataset:
        year_month = _group_month_year_extractor(fpath)
        if year_month in month_year_groups:
            month_year_groups[year_month].append(df_loader)
        else:
            month_year_groups[year_month] = [df_loader]
    logger.info(f'All year-month groups: {month_year_groups.keys()}')
    logger.info('Finished grouping files by year-month.')

    return month_year_groups

def aggregate_invoice_account_products(cur_dataset: Dict[str, Callable[[], Any]]) -> pd.DataFrame:

    all_files = sorted(list(cur_dataset.items()), key=lambda x: x[0])
    logger.info(f'Found {len(all_files)} to load.')

    grouped_files = _group_month_year(all_files)

    return {
        group_key : lambda vars=[group_key, group]: _lazy_aggregate_invoice_account_products(vars[0], vars[1]) 
            for group_key, group in grouped_files.items()
    }


def merge_cur_partitions(cur_dataset: Dict[str, Callable[[], Any]]) -> pd.DataFrame:

    loaded_partitions = []
    for filename, fileloader in cur_dataset.items():
        logger.info(f'Appending partition {filename} to merged file...')
        loaded_partitions.append(fileloader())
    
    return pd.concat(loaded_partitions)
