"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.18.10
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import preprocess_accounts_per_org, add_account_names


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=preprocess_accounts_per_org, 
            inputs='accounts_per_organization',
            outputs='processed_accounts_per_organization'
        ),
        node(
            func=add_account_names,
            inputs=['pocs_cur_dataset', 'accounts_per_organization'],
            outputs='pocs_cur_dataset_enriched'
        )
    ], tags='de')
