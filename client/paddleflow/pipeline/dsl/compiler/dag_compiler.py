def _update_and_validate_steps(self, steps: Dict[str, Step]):
        """ update steps before compile

        Args:
            steps (Dict[string, Step]): the steps need to update and validate
        """
        # 1. Synchronize environment variables of pipeline and step
        for name, step in steps.items():
            # 1. Synchronize environment variables of pipeline and step
            env = dict(self.env)
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
            
            # 4. validate docker_env
            if not self.docker_env and not step.docker_env:
                raise PaddleFlowSDKException(PipelineDSLError, 
                    self.__error_msg_prefix + f"cannot set the docker_env of step[step.name]")  

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