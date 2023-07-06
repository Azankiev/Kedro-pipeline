"""
This is a boilerplate pipeline 'data_integration'
generated using Kedro 0.18.10
"""
from typing import Callable, Dict, Any
import logging

import pandas as pd


logger = logging.getLogger(__name__)

def _s3_key_formatter(s3_key: str) -> str:
    '''
        Conert default CUR file S3 key to usable filename

        Examples:

        'year=2022/month=10/CUR-00001.snappy.parquet' -> '2022_10_CUR-00001.snappy.parquet'
        'year=2023/month=1/CUR-00002.snappy.parquet' -> '2023_1_CUR-00002.snappy.parquet'

        Parameters
        ----------
        s3_key : str
            S3 key of the CUR file extracted from S3.

        Returns
        -------
        str
            Formatted filename.
        
            
    '''

    year, month, fname = s3_key.split('/')
    year = year.replace('year=', '').zfill(4)
    month = month.replace('month=', '').zfill(2)
    fname = fname.replace('.snappy.parquet', '.csv')
    
    return f'{year}_{month}_{fname}'


def _lazy_preprocessing(s3_key, df_loader: Callable[[], Any], params: Dict) -> pd.DataFrame:

    logger.info(f'Preprocessing {s3_key}...')
    df_cur = df_loader()
    logger.info(f'File {s3_key} has dimensions: {df_cur.shape}')


    df_cur = df_cur[params['kept_columns']].copy()
    nrows = df_cur.shape[0]
    df_cur = df_cur.drop_duplicates()
    logger.info(f'{nrows - df_cur.shape[0]} duplicated rows were dropped.')

    # Apply zero left padding to ID columns. When reading Parquet files with Pandas we can't choose dtypes.
    # By default, it converts all IDs to int, deleting all the zeros on the left.
    # By observation, all account IDs have 12 digits.
    account_id_digits = 12
    account_id_cols = [col for col in df_cur.columns if 'account_id' in col]
    for col in account_id_cols:
        df_cur[col] = df_cur[col].astype(str).str.pad(account_id_digits, side='left', fillchar='0')
    
    logger.info(f'File {s3_key} preprocessed successfully.')

    return df_cur

def preprocess_cur(cur_dataset: Dict[str, Callable[[], Any]], params: Dict) -> Dict[str, Callable[[], Any]]:
    
    all_files = sorted(list(cur_dataset.items()), key=lambda x: _s3_key_formatter(x[0]))
    # TODO: Delete this limit, it's for test only.
    all_files = [f for f in all_files if 'year=2023' in f[0] and 'month=5' in f[0]]
    logger.info(f'Found {len(all_files)} to load.')

    return {
        _s3_key_formatter(s3_key): lambda vars=[s3_key, cur_loader, params]: _lazy_preprocessing(vars[0], vars[1], vars[2]) 
            for s3_key, cur_loader in all_files
    }