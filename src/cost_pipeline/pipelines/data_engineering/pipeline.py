"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.18.10
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import preprocess_accounts_per_org, merge_cur_partitions, add_account_names, aggregate_invoice_account_products


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=preprocess_accounts_per_org, 
            inputs='accounts_per_organization',
            outputs='processed_accounts_per_organization',
            tags=['de_cliente', 'cliente', 'de_cliente', 'cliente']
        ),
        node(
            func=aggregate_invoice_account_products,
            inputs='cliente_cur_dataset',
            outputs='cliente_cur_dataset_agg',
            tags=['de_cliente', 'cliente']
        ),
        node(
            func=add_account_names,
            inputs=['cliente_cur_dataset_agg', 'processed_accounts_per_organization'],
            outputs='cliente_cur_dataset_agg_enriched',
            tags=['de_cliente', 'cliente', 'cliente_add_account_names']
        ),
        node(
            func=aggregate_invoice_account_products,
            inputs='cliente_cur_dataset',
            outputs='cliente_cur_dataset_agg',
            tags=['de_cliente', 'cliente']
        ),
        node(
            func=add_account_names,
            inputs=['cliente_cur_dataset_agg', 'processed_accounts_per_organization'],
            outputs='cliente_cur_dataset_agg_enriched',
            tags=['de_cliente', 'cliente', 'cliente_add_account_names']
        ),
        # node(
        #     func=merge_cur_partitions,
        #     inputs='cliente_cur_dataset',
        #     outputs='cliente_cur_dataset_merged',
        #     tags=['de_cliente', 'cliente']
        # ),


    ], tags='de')
