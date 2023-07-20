from kedro.framework.context import KedroContext
from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro.framework.hooks import hook_impl

import os
from typing import Any
import logging

from .pipelines.data_integration.nodes import _s3_key_formatter

class NodeHooks:

    @hook_impl
    def before_node_run(
        self,
        node: Node,
        catalog: DataCatalog,
        inputs: dict[str, Any],
    ) -> dict[str, Any] | None:

        logger = logging.getLogger(__name__)

        if node._func_name == 'preprocess_cur':
            logger.info('Filtering out existing files.')
            output_dataset_name = node.outputs[0]
            output_path = catalog._get_dataset(output_dataset_name)._normalized_path

            input_dataset_name = [dataset for dataset in inputs.keys() if 's3_' in dataset][0]
            n_total_files = len(inputs[input_dataset_name])
            logger.info(f'Total number of files: {n_total_files}')
            inputs[input_dataset_name] = {s3_key: loader for s3_key, loader in inputs[input_dataset_name].items() if _s3_key_formatter(s3_key) not in os.listdir(output_path)}
            n_remaining_files = len(inputs[input_dataset_name])
            logger.info(f'Files remaining to dowload: {n_remaining_files}')

            return inputs

