"""
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/home/ahz/baidu/bmlc/paddleflow/client/paddleflow/pipelineSee the License for the specific language governing permissions and
limitations under the License.
"""
import os
import yaml
import copy
import configparser
from typing import Callable
from typing import List
from typing import Dict
from pathlib import Path

from .component import component
from .component.steps import step
from .component.steps import Step
from .component import DAG
from .options import CacheOptions
from .options import FailureOptions
from .io_types import EnvDict
from .options import FSOptions
from .utils.util import validate_string_by_regex
from .utils.consts import PIPELINE_NAME_REGEX
from .utils.consts import PipelineDSLError
from .utils.consts import VARIBLE_NAME_REGEX
from .utils.consts import ENTRY_POINT_NAME
from .compiler import Compiler
from .inferer import DAGInferer
from .inferer import ContainerStepInferer

from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException
from paddleflow.common.util import  get_default_config_path


class Pipeline(object):
    """ Pipeline is a workflow which is composed of Step  
    """
    def __init__(
            self,
            name: str,
            parallelism: int=None,
            docker_env: str=None,
            env: Dict[str, str]=None,
            cache_options: CacheOptions=None,
            failure_options: FailureOptions=None,
            fs_options: FSOptions=None
            ):
        """ create a new instance of Pipeline

        Args:
            name (str): the name of Pipeline 
            parallelism (str): the max number of total parallel step that can execute at the same time in this Pipeline.
            docker_env (str): the docker image address of this Pipeline, if Step of this Pipeline has their own docker_env, then the docker_env of Step has higher priority
            env (Dict[str, str]): the environment variable of Step runtime, which is shared by all steps. If the environment variable with the same name is set to step, the priority of step is higher
            cache_options (CacheOptions): the cache options of Pipeline which is shared by all steps, if Step of this Pipeline has their own cache_options, then the priority of step is higher
            failure_options (FailureOptions): the failure options of Pipeline, Specify how to handle the rest steps of the pipeline when there is a step failure


        Raises:
            PaddleFlowSDKException: if some params has error value

        Example: ::
            
            @Pipeline(docker_env="images/training.tgz", env=Env, parallelism=5, name="myproject", cache_options=cache_options)
            def myproject(data_path, iteration):
                step1 = data_process(data_path)
                step2 = main(step1.parameters["data_file"], iteration, step1.outputs["train_data"])
                step3 = validate(step2.parameters["data_file"], step1.outputs["validate_data"], step2.outputs["train_model"])
        
        .. note:: This is not used directly by the users but auto generated when the Pipeline decoration exists
        """
        if parallelism and parallelism <1:
            raise PaddleFlowSDKException(PipelineDSLError, "parallelism should >= 1")
        
        self.name = name
        
        self.__error_msg_prefix = f"error occurred in Pipeline[{self.name}]: "
        self.docker_env = docker_env
        self.parallelism = parallelism
        self.cache_options = cache_options
        self.failure_options = failure_options
        self.fs_options = fs_options

        self._client = None

        self._env = EnvDict(self)
        self.add_env(env)

        # the name of step to step 
        self._entry_points = None
        self._post_process = {}

    
    def _init_client(
            self,
            config: str=None,
            ):
        """ create a client which response to communicate with server

        Args:
            config (str): the path of config file

        Raises:
            PaddleFlowSDKException: if cannot create run  
        """
        if self._client is not None:
            return
        
        from paddleflow.client import Client
        from paddleflow.cli.cli import DEFAULT_PADDLEFLOW_PORT
        
        if config:
            config_file = config
        else:
            config_file = get_default_config_path()

        if not os.access(config_file, os.R_OK):
            raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + \
                        f"no config file in {config_file}")

        config = configparser.RawConfigParser()
        config.read(config_file, encoding='UTF-8')

        if 'user' not in config or 'server' not in config:
            raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + \
                        f"no user or server conf in {config_file}")
            
        if 'password' not in config['user'] or 'name' not in config['user']:
            raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + \
                        f"no name or password conf['user'] in {config_file}")
        
        name = config['user']['name']
        password = config['user']['password']

        if 'paddleflow_server_host' not in config['server']:
            raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + \
                        f"no paddleflow_server_host in {config_file}")
            
        paddleflow_server = config['server']['paddleflow_server_host']

        if 'paddleflow_server_port' in config['server']:
            paddleflow_port = config['server']['paddleflow_server_port']
        else:
            paddleflow_port = DEFAULT_PADDLEFLOW_PORT

        client = Client(paddleflow_server, config['user']['name'], config['user']['password'], paddleflow_port)
        ok, msg = client.login(name, password)
        if not ok:
            raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + f"login failed: {msg}")
        
        self._client = client
      
       
    def __call__(
            self,
            func: Callable):
        """ __call__

        Args:
            func (Callable): the function which complete organize pipeline
        
        Returns:
            a function wrapper the pipeline function
        """
        self.__func = func
        return  self._organize_pipeline

    def _organize_pipeline(
            self, 
            *args,
            **kwargs
            ):
        """ organize pipeline
        """
        new_ppl = Pipeline(
            name=self.name,
            parallelism=self.parallelism,
            docker_env=self.docker_env,
            cache_options=self.cache_options,
            failure_options=self.failure_options,
            fs_options=self.fs_options,
            env=self._env,
            )
        
        new_ppl.__func = self.__func
        if self._post_process:
            new_ppl.set_post_process(self._post_process)

        new_ppl._client = new_ppl._client

        new_ppl._entry_points = DAG(name=ENTRY_POINT_NAME)

        with new_ppl._entry_points:
            new_ppl.__func(*args, **kwargs)

        return new_ppl
    
    def run(
            self,
            config: str=None,
            username: str=None,
            fs_name: str=None,
            run_name: str=None,
            desc: str=None,
            disabled: List[str]=None,
            ):
        """ create a pipelint run

        Args:
            username (str): create the specified run by username, only useful for root.
            config (str): the path of config file
            fs_name (str): the fsname of paddleflow
            run_name (str): the name of this run 
            desc (str): description of run 
            disabled (List[str]): a list of step's name which need to disable in this run

        Raises:
            PaddleFlowSDKException: if cannot create run  
        """
        # TODO: add validate logical

        # 1. compile
        pipeline = yaml.dump(self.compile())
        pipeline = pipeline.encode("utf-8")

        self._init_client(config)
        if disabled:
            disabled = ",".join([disable.strip(ENTRY_POINT_NAME + ".") for disable in disabled])
        
        return self._client.create_run(fs_name, username, run_name, desc, run_yaml_raw=pipeline, disabled=disabled)

    def compile(
            self,
            save_path: str=None
            ):
        """ trans this Pipeline to a static description such as yaml or json string

        Args:
            save_path: the path of file which to save the content of Pipeline after compile

        Returns:
            a dict which description this Pipeline instance

        Raises:
           PaddleFlowSDKException: if compile failed 
        """
        if not self._entry_points:
            raise PaddleFlowSDKException(PipelineDSLError,
                    self.__error_msg_prefix + f"there is no Step or DAG in Pipeline[{self.name}]")
    
        # TODO: validate

        if len(self._post_process) > 1:
            raise PaddleFlowSDKException(PipelineDSLError, 
                self.__error_msg_prefix + "There can only be one step at most in post_process right now")
        
        # infer parameter and artifact for entry_point
        DAGInferer(self._entry_points).infer(self.env)

        if self._post_process:
            for _, cp in self._post_process.items():
                ContainerStepInferer(cp).infer(self.env)
                
        # Compile
        pipeline_dict = Compiler().compile(self, save_path)
        return pipeline_dict

    @property
    def entry_points(self):
        """ get all steps except post_process that this Pipelines instance include

        Returns:
            a dict which key is the name of step and value is Step instances
        """
        return self._entry_points.entry_points

    @property
    def name(self):
        """ get the name of this Pipelines instance

        Returns:
            the name of this Pipelines instance
        """
        return self._name
    
    @name.setter
    def name(
            self,
            name: str):
        """ set the name of it
    
        Args:
            name (str): new name of Pipeline

        Raises:
            PaddleFlowSDKException: if name is illegal 
        """
        if not validate_string_by_regex(name, PIPELINE_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + \
                    f"the name of Pipeline[{name}] is is illegal" + \
                    f"the regex used for validation is {PIPELINE_NAME_REGEX}")

        self._name = name

    @property
    def env(self):
        """ get Pipeline's env

        Returns:
            a dict while the key is the name of env and the value is the value of env
        """
        return self._env
    
    def add_env(
            self, 
            env: Dict[str, str]
            ):
        """ add enviroment varible

        Args:
            env (Dict[str, str]): enviroment varible need to be set when executing Step.

        Raises:
            PaddleFlowSDKException: if some enviroment is illegal
        """
        if not env:
            return 

        for name, value in env.items():
            self._env[name] = value

    def set_post_process(self, step: Step):
        """ set the post_process step

        Args:
            step: the step of Pipeline's post_process, would be execute after other steps finished
        """
        # TODO：增加校验逻辑
        if not isinstance(step, Step):
            raise PaddleFlowSDKException(PipelineDSLError, 
                self.__error_msg_prefix + "post_process of pipeline should be an instance of Step")
        
        # There can only be one step at most in post_process right now
        self._post_process = {}
        self._post_process[step.name] = step

    def get_post_process(self):
        """ get post_process

        Returns:
            The Step in post_process
        """
        # There can only be one step at most in post_process right now
        for _, value in self._post_process.items():
            return value

        return None
