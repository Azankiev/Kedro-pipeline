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
            inputs=['s3_cliente_cur_dataset', 
                    'params:data_prep_raw_params'], 
            outputs='cliente_cur_dataset',
            tags=['di_cliente', 'cliente']
        ),
        node(
            func=preprocess_cur, 
            inputs=['s3_cliente_cur_dataset', 
                    'params:data_prep_raw_params'], 
            outputs='cliente_cur_dataset',
            tags=['di_intebras', 'cliente']
        ),
        node(
            func=preprocess_cur, 
            inputs=['s3_cliente_cur_dataset', 
                    'parameters'], 
            outputs='cliente_cur_dataset',
            name='cliente_preprocess_cur',
            tags=['di_cliente', 'cliente']
        ),
        node(
            func=preprocess_cur, 
            inputs=['s3_cliente_cur_dataset', 
                    'parameters'], 
            outputs='cliente_cur_dataset',
            name='cliente_preprocess_cur',
            tags=['di_cliente', 'cliente']
        )
    ], tags='di')