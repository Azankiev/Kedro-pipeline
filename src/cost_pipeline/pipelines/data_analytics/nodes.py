"""
This is a boilerplate pipeline 'data_analytics'
generated using Kedro 0.18.10
"""
import logging
from typing import Dict, Callable, Any

import pandas as pd

logger = logging.getLogger(__name__)

def _lazy_generate_aggregated_invoices(fname: str, loader: Callable[[], Any], params: Dict) -> pd.DataFrame:
    
    df_agg = loader()

    logger.info(f'[{fname}] Generating aggregated invoices...')

    logger.info(f'[{fname}] Pre-adding totals...')
    df_agg_totals = df_agg.groupby(by=params['unique_invoice_line_columns'] + [params['item_type_column']]).agg(
                                    {
                                            params['cost_column']: 'sum', 
                                            params['discount_spp_column']: 'sum'
                                    }).reset_index()
    logger.info(f'[{fname}] Finished pre-adding totals.')

    item_type_values = df_agg_totals[params['item_type_column']].unique()
    logger.info(f'[{fname}] Pivoting item types... [{list(item_type_values)}]')
    df_agg_totals_pivoted = df_agg_totals.pivot(index=params['unique_invoice_line_columns'] + [params['discount_spp_column']],
                                                columns=params['item_type_column'], 
                                                values=params['cost_column']).reset_index()

    logger.info(f'[{fname}] Item types pivoted.')
    df_agg_totals_pivoted = df_agg_totals_pivoted.fillna(0)
    logger.info(f'[{fname}] Adding up the totals... ')

    # Setting non-existing numerical cost columns to 0.
    for col in params['default_item_type_values']:
        if col not in df_agg_totals_pivoted.columns: df_agg_totals_pivoted[col] = 0
  
    df_agg_totals_pivoted = df_agg_totals_pivoted.groupby(by=params['unique_invoice_line_columns']).agg(
                                                                            {'discount_spp_discount': 'sum', 
                                                                              'Credit': 'sum',
                                                                              'Fee': 'sum',
                                                                              'Refund': 'sum',
                                                                              'Tax': 'sum',
                                                                              'Usage': 'sum'
                                                                            }).reset_index()
    

    df_result = df_agg_totals_pivoted[params['unique_invoice_line_columns']].copy()
    df_result['charges'] = df_agg_totals_pivoted['Usage'] + df_agg_totals_pivoted['Fee']
    df_result['discount_spp'] = df_agg_totals_pivoted['discount_spp_discount']
    df_result['credits'] = df_agg_totals_pivoted['Credit']
    df_result['tax'] = df_agg_totals_pivoted['Tax']
    df_result['total'] = df_result['charges'] + df_result['discount_spp'] + df_result['credits'] + df_result['tax']
    logger.info('Totals added up.')

    return df_result

def generate_aggregated_invoices(cur_dataset: Dict[str, Callable[[], Any]], params: Dict) -> pd.DataFrame:
    '''
    
    '''

    all_files = sorted(list(cur_dataset.items()), key=lambda x: x[0])
    logger.info(f'Found {len(all_files)} to load.')

    return {
        fname: lambda vars=[fname, loader, params]: _lazy_generate_aggregated_invoices(vars[0], vars[1], vars[2])
          for fname, loader in all_files
    }