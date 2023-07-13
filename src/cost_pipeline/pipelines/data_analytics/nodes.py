"""
This is a boilerplate pipeline 'data_analytics'
generated using Kedro 0.18.10
"""
import logging
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)

def generate_aggregated_invoices(df_agg: pd.DataFrame, params: Dict) -> pd.DataFrame:

    logger.info('Generating aggregated invoices...')

    logger.info('Pre-adding totals...')
    df_agg_totals = df_agg.groupby(by=params['unique_invoice_line_columns'] + [params['item_type_column']]).agg(
                                    {
                                            params['cost_column']: 'sum', 
                                            params['discount_spp_column']: 'sum'
                                    }).reset_index()
    logger.info('Finished pre-adding totals.')

    logger.info('Pivoting item types...')
    df_agg_totals_pivoted = df_agg_totals.pivot(index=params['unique_invoice_line_columns'] + [params['discount_spp_column']],
                                                columns=params['item_type_column'], 
                                                values=params['cost_column']).reset_index()

    logger.info('Item types pivoted.')
    df_agg_totals_pivoted = df_agg_totals_pivoted.fillna(0)
    logger.info('Adding up the totals... ')
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