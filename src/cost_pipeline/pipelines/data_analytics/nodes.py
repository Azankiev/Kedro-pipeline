"""
This is a boilerplate pipeline 'data_analytics'
generated using Kedro 0.18.10
"""

import pandas as pd
import logging

logger = logging.getLogger(__name__)

def generate_aggregated_invoices(df_agg: pd.DataFrame) -> pd.DataFrame:

    logger.info('Generating aggregated invoices...')

    logger.info('Pre-adding totals...')
    df_agg_totals = df_agg.groupby(by=['bill_invoice_id', 'bill_billing_period_start_date', 
                                            'bill_billing_period_end_date', 'bill_payer_account_id', 'line_item_usage_account_id', 
                                            'usage_account_name', 'product_product_name', 'line_item_line_item_type']).agg({
                                            'line_item_unblended_cost': 'sum', 
                                            'discount_spp_discount': 'sum'
                                    }).reset_index()
    logger.info('Finished pre-adding totals.')

    logger.info('Pivoting item types...')
    df_agg_totals_pivoted = df_agg_totals.pivot(index=['bill_invoice_id', 'bill_billing_period_start_date', 
                                                            'bill_billing_period_end_date', 'bill_payer_account_id', 
                                                            'line_item_usage_account_id', 'usage_account_name', 
                                                            'product_product_name', 'discount_spp_discount'],
                                                columns='line_item_line_item_type', 
                                                values='line_item_unblended_cost').reset_index()

    logger.info('Item types pivoted.')
    df_agg_totals_pivoted = df_agg_totals_pivoted.fillna(0)
    logger.info('Adding up the totals... ')
    df_agg_totals_pivoted = df_agg_totals_pivoted.groupby(by=['bill_invoice_id', 'bill_payer_account_id', 
                                                            'bill_billing_period_end_date', 'bill_billing_period_start_date', 
                                                            'line_item_usage_account_id', 'usage_account_name', 
                                                            'product_product_name']).agg(
                                                                            {'discount_spp_discount': 'sum', 
                                                                              'Credit': 'sum',
                                                                              'Fee': 'sum',
                                                                              'Refund': 'sum',
                                                                              'Tax': 'sum',
                                                                              'Usage': 'sum'
                                                                            }).reset_index()
    

    df_result = df_agg_totals_pivoted[['bill_invoice_id', 'bill_payer_account_id',
                                    'bill_billing_period_end_date', 'bill_billing_period_start_date',
                                    'line_item_usage_account_id', 'usage_account_name', 
                                    'product_product_name']].copy()
    df_result['charges'] = df_agg_totals_pivoted['Usage'] + df_agg_totals_pivoted['Fee']
    df_result['discount_spp'] = df_agg_totals_pivoted['discount_spp_discount']
    df_result['credits'] = df_agg_totals_pivoted['Credit']
    df_result['tax'] = df_agg_totals_pivoted['Tax']
    df_result['total'] = df_result['charges'] + df_result['discount_spp'] + df_result['credits'] + df_result['tax']
    logger.info('Totals added up.')
    
    return df_result