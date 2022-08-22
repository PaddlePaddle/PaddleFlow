#!/usr/bin/env python3 

""" unit test for paddelflow.pipeline.dsl.io_types.parameter
"""
import copy
import pytest

from .mock_step import Step
from paddleflow.pipeline import Parameter
from paddleflow.pipeline.dsl.io_types.placeholder import ParameterPlaceholder
from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class TestParameter(object):
    """ unit test for Parameter
    """
    @pytest.mark.type
    def test_trans_type_to_str(self):
        """ test _trans_type_to_str
        """
        param = Parameter()
        assert param._trans_type_to_str(int) == "int"
        assert param._trans_type_to_str(str) == "string"
        
        with pytest.raises(PaddleFlowSDKException):
            param._trans_type_to_str(bool)
    
    @pytest.mark.init
    def test_init(self):
        """ test __init__
        """
        param = Parameter("123", str)

        assert param.default == "123"
        assert param.type == "string"

        param = Parameter(type="string")
        assert  param.type == "string"
        
        param = Parameter(1, str)
        assert param.default == str(1)

        Parameter(1.0, int)
        
        with pytest.raises(PaddleFlowSDKException):
            Parameter("ade", int)

    @pytest.mark.baseinfo
    def test_set_base_info(self):
        """ test set_base_info
        """
        param = Parameter()
        step = Step(name="xiaodu")

        param.set_base_info(component=step, name="num", ref="123")
        assert param.component == step and param.default is None and param.type is None and param.ref == "123"

        param.set_base_info(component=step, name="num")

        with pytest.raises(PaddleFlowSDKException):
            param.set_base_info(component=step, name="123num")

        pl = ParameterPlaceholder(name="abc", component_full_name="bac")
        param.set_base_info(component=step, name="num", ref=pl)
        assert param.ref.component_full_name == "bac"
    
    @pytest.mark.set_default
    def test_set_default(self):
        """ test default.setter
        """
        param = Parameter(type=int)
        param.default = "123"
        assert param.default == 123

    @pytest.mark.eq
    def test_eq(self):
        """ test __eq__
        """
        param1 = Parameter()
        param2 = Parameter()

        assert param1 == param2

        param1.default = "123"
        assert param1 != param2

        step2 = Step(name="dudu")
        param1.set_base_info(component=step2, name="num")

        param2.default = "123"
        param2.set_base_info(component=step2, name="num")
        assert  param2 == param1

    @pytest.mark.deepcopy
    def test_deepcopy(self):
        """ test deepcopy
        """
        param1 = Parameter()
        param2 = copy.deepcopy(param1)
        assert  param1 == param2 and param2 is not param1

        step2 = Step(name="dudu")
        param1.set_base_info(component=step2, name="num")
        param2 = copy.deepcopy(param1)
        assert  param2 == param1 and param2 is not param1

    @pytest.mark.str
    def test_str(self):
        """ test __str__
        """
        param1 = Parameter()
        with pytest.raises(PaddleFlowSDKException):
            str(param1)

        step2 = Step(name="dudu")
        step2.full_name = "pf-entry-point.dudu"
        param1.set_base_info(component=step2, name="num")
        assert str(param1) == "{{parameter: pf-entry-point.dudu.num}}"