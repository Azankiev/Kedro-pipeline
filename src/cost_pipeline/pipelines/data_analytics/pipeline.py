"""
This is a boilerplate pipeline 'data_analytics'
generated using Kedro 0.18.10
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import generate_aggregated_invoices


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=generate_aggregated_invoices, 
            inputs=['infra2_cur_dataset_agg_enriched', 'params:data_analytics_params'],
            outputs='infra2_agg_invoices',
            tags=['da_infra2', 'infra2']
        )
    ])
