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

from .steps import step
from .steps import Step
from .options import CacheOptions
from .options import FailureOptions
from .utils.util import validate_string_by_regex
from .utils.util import CaseSensitiveConfigParser
from .utils.consts import PIPELINE_NAME_REGEX
from .utils.consts import PipelineDSLError
from .utils.consts import VARIBLE_NAME_REGEX
from .compiler import Compiler

from paddleflow.common.exception.paddleflow_sdk_exception import PaddleFlowSDKException

class Pipeline(object):
    """ Pipeline is a workflow which is composed of Step  
    """
    def __init__(
            self,
            name: str,
            parallelism: int=None,
            image: str=None,
            env: Dict[str, str]=None,
            cache_options: CacheOptions=None,
            failure_options: FailureOptions=None
            ):
        """ create an new instance of Pipeline

        Args:
            name (str): the name of Pipeline 
            parallelism (str): the max number of total parallel pods that can execute at the same time in this Pipeline.
            image (str): the docker image address of this Pipeline, if Step of this Pipeline has their own image, then the image of Step has higher priority
            env (Dict[str, str]): the environment variable of Step runtime, which is shared by all steps. If the environment variable with the same name is set to step, the priority of step is higher
            cache_options (cache_options): the cache options of Pipeline which is shared by all steps, if Step of this Pipeline has their own cache_options, then the priority of step is higher
            failure_options (FailureOptions): the failure options of Pipeline, Specify how to handle the rest of the steps when there is a step failure

        .. note:: This is not used directly by the users but auto generated when the Pipeline decoration exists

        Raises:
            PaddleFlowSDKException: if some params has error value

        Example: ::
            
            @Pipeline(image="images/training.tgz", env=Env, parallelism=5, name="myproject", cache_options=cache_options)
            def myproject(data_path, iteration):
                step1 = data_process(data_path)
                step2 = main(step1.params["data_file"], iteration, step1.outputs["train_data"])
                step3 = validate(step2.params["data_file"], step1.outputs["validate_data"], step2.outputs["train_model"])
        """
        if parallelism and parallelism <1:
            raise PaddleFlowSDKException(PipelineDSLError, "parallelism should >= 1")

        if not validate_string_by_regex(name, PIPELINE_NAME_REGEX):
            raise PaddleFlowSDKException(PipelineDSLError, f"the name of Step[{name}] is is illegal" + \
                    f"the regex used for validation is {PIPELINE_NAME_REGEX}")
        
        self.name = name
        
        self.__error_msg_prefix = f"error occurred in Pipeline[{self.name}]: "
        self.image = image
        self.parallelism = parallelism
        self.cache_options = cache_options
        self.failure_options = failure_options

        self._env = env or {}
    
        self.add_env(self._env)

        # the name of step to step 
        self._steps = {}
        self._post_process = {}

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
        def register_step_to_pipeline(step: Step):
            """ register step to pipeline, will be invoked when instantiating step
            
            Args:
                step (Step): the step which need to register this Pipeline instance
            """
            self.add(step)

        old_register = step.register_step_handler
        step.register_step_handler = register_step_to_pipeline

        self.__func(*args, **kwargs)

        step.register_step_handler = old_register

        return self

    def add(
            self, 
            step
            ):
        """ add step

        Args:
            step (Step):  the step which need to register to this Pipeline 
        """
        # 1. to avoid mutiple steps have the same name
        step.name = self._make_step_name_unique_by_adding_index(step.name)
        self._steps[step.name] = step

    def _make_step_name_unique_by_adding_index(
            self,
            name: str
            ):
        """ to ensure every step in pipeline has unique name 

        Args:
            name (str): the name of steps:
        """
        names = list(self._steps.keys()) + list(self._post_process.keys())
        index = 1 
        
        unique_name = name
        while unique_name in names:
            unique_name = f"{name}-{index}"
            index += 1

        return unique_name
    
    def run(
            self,
            config: str=None,
            username: str=None,
            fsname: str=None,
            runname: str=None,
            desc: str=None,
            entry: str=None,
            disabled: List[str]=None,
            ):
        """ create a run

        Args:
            username (str): Run the specified run by username, only useful for root.
            config (str): the path of configf file
            fsname (str): the fsname of paddleflow
            runname (str): the name of this run 
            desc (str): description of run 
            entry (str): the entry of run, should be one of step's name in pipeline
            disable (List[str]): a list of step's name which need to disable in this run

        Raises:
            PaddleFlowSDKException: if cannot create run  
        """
        from paddleflow.client import Client
        from paddleflow.cli.cli import DEFAULT_PADDLEFLOW_PORT
        
        if config:
            config_file = config
        else:
            home_path = os.getenv('HOME')
            config_file = os.path.join(home_path, '.paddleflow/config')

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

        if 'paddleflow_server' not in config['server']:
            raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + \
                        f"no paddleflow_server in {config_file}")
            
        paddleflow_server = config['server']['paddleflow_server']

        if 'paddleflow_port' in config['server']:
            paddleflow_port = config['server']['paddleflow_port']
        else:
            paddleflow_port = DEFAULT_PADDLEFLOW_PORT

        client = Client(paddleflow_server, config['user']['name'], config['user']['password'], paddleflow_port)
        ok, msg = client.login(name, password)
        if not ok:
            raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + f"login failed: {msg}")

        # 1. validate
        ## 1.1 validate disabled
        if disabled is not None:
            for name in disabled:
                if name not in self._steps or name not in self._post_process:
                    raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + \
                        f"cannot find the step who's name is [{name}]")
 
            disabled = ",".join(disabled)

        ## 1.2 validate cache_options.fs_scope and fs_id
        if hasattr(self, "cache_options") and self.cache_options and \
            self.cache_options.fs_scope is not None and fsname is None:
                raise PaddleFlowSDKException(PipelineDSLError,
                    self.__error_msg_prefix + f"cannot set fs_scope for CacheOptions when fsname is None")

        # 2. compile
        pipeline = yaml.dump(self.compile())
        pipeline = pipeline.encode("utf-8")
        
        # 3. run
        if entry and entry not in self._steps:
            raise PaddleFlowSDKException(PipelineDSLError,
                self.__error_msg_prefix + f"the entry[{entry}] of run is not in pipeline")

        return client.create_run(fsname, username, runname, desc, entry, runyamlraw=pipeline, disabled=disabled)
        
    def _update_and_validate_steps(self, steps: Dict[str, Step]):
        """ update steps before compile

        Args:
            steps (Dict[string, Step]): the steps need to update and validate
        """
        # 1. Synchronize environment variables of pipeline and step
        for name, step in steps.items():
            # 1. Synchronize environment variables of pipeline and step
            env = copy.deepcopy(self.env)
            env.update(step.env)
            step.add_env(env)

            # 2. Ensure that the input / output aritact and parameter of the step have different names
            step.validate_io_names()
        
            # 3. Resolve dependencies between steps and validate all deps are Pipeline
            for dep in step.get_dependences():
                if dep.name not in steps or dep is not steps[dep.name]:
                    raise PaddleFlowSDKException(PipelineDSLError, self.__error_msg_prefix + \
                            f"the upstream step[{dep.name}] for step[{step.name}] is not in Pipeline[{self.name}].\n" + \
                                "Attentions: step in postProcess cannot depend on any other step")
            
            # 4. validate image
            if not self.image and not step.image:
                raise PaddleFlowSDKException(PipelineDSLError, 
                    self.__error_msg_prefix + f"cannot set the image of step[step.name]")  

    def compile(
            self,
            save_path: str=None
            ):
        """ trans this Pipeline to a static description such as yaml or json string

        Args:
            save_path: the path of file which to save the content of Pipeline after compile

        Returns:
            a json string which description this Pipeline instance

        Raises：
           PaddleFlowSDKException: if compile failed 
        """
        if not self._steps:
            raise PaddleFlowSDKException(PipelineDSLError,
                    self.__error_msg_prefix + f"there is no Step in Pipeline[{self.name}]")

        # 1. Synchronize environment variables of pipeline and step
        self._update_and_validate_steps(self._steps)

        if len(self._post_process) > 1:
            raise PaddleFlowSDKException(PipelineDSLError, 
                self.__error_msg_prefix + "There can only be one step at most in post_process right now")
        self._update_and_validate_steps(self._post_process)
        
        # 4、Check whether there is a ring and whether it depends on steps that do not belong to this pipeline
        self.topological_sort()
        
        # 5、Compile
        pipeline_json = Compiler().compile(self, save_path)
        return pipeline_json

    def topological_sort(self):
        """ List Steps in topological order.

        Returns:
            A list of Steps in topological order

        Raises:
            PaddleFlowSDKException: if there is a ring
        """
        topo_sort = []
        
        while len(topo_sort) < len(self._steps):
            exists_ring = True
            for step in self._steps.values():
                if step in topo_sort:
                    continue

                need_add = True

                for dep in step.get_dependences():
                    if dep not in topo_sort:
                        need_add = False
                        break

                if need_add:
                    topo_sort.append(step)
                    exists_ring = False
            
            if exists_ring:
                ring_steps = [step.name for step in self._steps.values()  if step not in topo_sort]
                raise PaddleFlowSDKException(PipelineDSLError, 
                    self.__error_msg_prefix + f"there is a ring between {ring_steps}")
        
        # append post_process  
        topo_sort += list(self._post_process.values())
        return topo_sort

    def get_params(self):
        """ get all step's Parameter

        Return:
            a dict which contain all step's Paramter, for example::

                {
                    "step1": {"param_a": 1},
                    "step2": {"param_a": 2},
                }
        """

        params = {}
        for step in self._steps.values():
            params[step] = step.params

        return params

    @property
    def steps(self):
        """ get all steps that this Pipelines instance include

        Returns:
            a dict which key is the name of step and value is Step instances
        """
        return self._steps

    @property
    def name(self):
        """ get the name this Pipelines instance

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
            PaddleFlowPaddleFlowSDKExceptionSDKException: if name is illegal 
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
        for name, value in env.items():
            if not validate_string_by_regex(name, VARIBLE_NAME_REGEX):
                err_msg = self.__error_msg_prefix + \
                    f"the name of env[{name}] is illegal, the regex used for validation is {VARIBLE_NAME_REGEX}"
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)

            try:
                value = str(value)
            except Exception as e:
                err_msg = self.__error_msg_prefix + f"the value of env[{name}] should be an instances of string"
                raise PaddleFlowSDKException(PipelineDSLError, err_msg)

            self._env[name] = value

    def add_env_from_file(
            self,
            file: str
            ):
        """ add env from file

        Args:
            file (str): the path of file in ini format which has env section

        Raises:
            PaddleFlowSDKException: if the file is not exists or the format of the file is error
        """
        if not Path(file).is_file():
            err_msg = self.__error_msg_prefix + f"the file[{file}] is not exists or it's not a file"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)

        config = CaseSensitiveConfigParser()
        env = {}

        try:
            config.read(file)
            for key, value in config.items("env"):
                env[key] = value

            self.add_env(env)
        except PaddleFlowSDKException as e:
            raise e
        except Exception as e:
            err_msg = self.__error_msg_prefix + \
                f"The file[{file}] needs to be in ini format and have [env] section: {repr(e)}"
            raise PaddleFlowSDKException(PipelineDSLError, err_msg)

    def set_post_process(self, step: Step):
        """ set the post_process step

        Args:
            step: the step of Pipeline's post_process, would be execute after other steps finished
        """
        if not isinstance(step, Step):
            raise PaddleFlowSDKException(PipelineDSLError, 
                self.__error_msg_prefix + "the step of post_process should be an instance of Step")
        
        # There can only be one step at most in post_process right now
        self._post_process = {}
        step.name = self._make_step_name_unique_by_adding_index(step.name)
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
