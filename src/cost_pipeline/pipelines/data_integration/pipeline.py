"""
This is a boilerplate pipeline 'data_integration'
generated using Kedro 0.18.10
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import preprocess_cur


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=preprocess_cur, 
            inputs=['s3_pocs_cur_dataset', 
                    'params:data_prep_raw_params'], 
            outputs='pocs_cur_dataset',
            tags=['di_pocs', 'pocs']
        ),
        node(
            func=preprocess_cur, 
            inputs=['s3_intelbras_cur_dataset', 
                    'params:data_prep_raw_params'], 
            outputs='intelbras_cur_dataset',
            tags=['di_intebras', 'intelbras']
        ),
        node(
            func=preprocess_cur, 
            inputs=['s3_infra2_cur_dataset', 
                    'parameters'], 
            outputs='infra2_cur_dataset',
            name='infra2_preprocess_cur',
            tags=['di_infra2', 'infra2']
        )
    ], tags='di')