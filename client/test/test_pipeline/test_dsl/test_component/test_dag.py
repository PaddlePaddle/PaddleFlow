#!/usr/bin/env python3
""" unit test for paddleflow.pipeline.dsl.component.DAG
"""

import pytest

from pathlib import Path
from paddleflow.pipeline import DAG
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline.dsl.io_types import Artifact
from paddleflow.pipeline.dsl.io_types import Parameter
from paddleflow.pipeline.dsl.options import CacheOptions
from paddleflow.pipeline.dsl.options import FSScope
from paddleflow.pipeline.dsl.options import ExtraFS
from paddleflow.pipeline.dsl.component.component import register_component_handler
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class TestDAG(object):
    """ unit test for ContainerStep
    """
    @pytest.mark.init
    def test_init(self):
        """ test __init__
        """
        dag = DAG(name="dag1", condition="abc", loop_argument=[1,2,3])
        assert dag.name == "dag1"
        assert dag.condition == "abc"
        assert dag.loop_argument.argument == [1,2,3]
        assert str(dag.loop_argument.item) == "{{loop: dag1.PF_LOOP_ARGUMENT}}"

    @pytest.mark.add
    def test_add(self):
        """ test with
        """
        with DAG(name="dag1") as dag:
            step1 = ContainerStep(name="step1")
            step2 = ContainerStep(name="step2")

        assert len(dag.entry_points) == 2
        assert dag.entry_points["step1"] == step1
        assert dag.entry_points["step2"] == step2
        assert step1.full_name == "dag1.step1"

        with pytest.raises(PaddleFlowSDKException):
            with DAG(name="dag1") as dag:
                step1 = ContainerStep(name="step1")
                step2 = ContainerStep(name="step1")

        with DAG(name="dag1") as dag:
            step1 = ContainerStep(name="step1")
            step2 = ContainerStep(name="step2")
            with DAG(name="dag2") as dag2:
                step3 = ContainerStep(name="step3")
                step4 = ContainerStep(name="step4")

        assert len(dag.entry_points) == 3
        assert dag.entry_points["step1"] == step1
        assert dag.entry_points["step2"] == step2
        assert dag.entry_points["dag2"] == dag2
        assert dag2.entry_points["step3"] == step3
        assert dag2.entry_points["step4"] == step4
        assert step3.full_name == "dag1.dag2.step3"

        