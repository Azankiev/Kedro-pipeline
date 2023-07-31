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
            tags=['de_infra2', 'infra2', 'de_rnp', 'rnp']
        ),
        node(
            func=aggregate_invoice_account_products,
            inputs='infra2_cur_dataset',
            outputs='infra2_cur_dataset_agg',
            tags=['de_infra2', 'infra2']
        ),
        node(
            func=add_account_names,
            inputs=['infra2_cur_dataset_agg', 'processed_accounts_per_organization'],
            outputs='infra2_cur_dataset_agg_enriched',
            tags=['de_infra2', 'infra2', 'infra2_add_account_names']
        ),
        node(
            func=aggregate_invoice_account_products,
            inputs='rnp_cur_dataset',
            outputs='rnp_cur_dataset_agg',
            tags=['de_rnp', 'rnp']
        ),
        node(
            func=add_account_names,
            inputs=['rnp_cur_dataset_agg', 'processed_accounts_per_organization'],
            outputs='rnp_cur_dataset_agg_enriched',
            tags=['de_rnp', 'rnp', 'rnp_add_account_names']
        ),
        # node(
        #     func=merge_cur_partitions,
        #     inputs='infra2_cur_dataset',
        #     outputs='infra2_cur_dataset_merged',
        #     tags=['de_infra2', 'infra2']
        # ),


    ], tags='de')
