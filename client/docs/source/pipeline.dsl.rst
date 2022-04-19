pipeline.dsl package
====================

Subpackages
-----------

.. toctree::
   :maxdepth: 4

   pipeline.dsl.io_types
   pipeline.dsl.options
   pipeline.dsl.steps

pipeline.dsl.pipeline module
----------------------------

.. automodule:: pipeline.dsl.pipeline
   :members:
   :undoc-members:
   :show-inheritance:

pipeline.dsl.sys\_params module
-------------------------------
The paddeflow pipeline provides some system environment variables for each step, which can be directly used in the step command. Examples are as follows: ::
    
    from paddleflow.pipeline import PF_RUN_ID
    def show_run_id(name):
        return ContainerStep(name=name, 
                             command=f"echo {PF_RUN_ID}",
                             docker_env="registry.baidubce.com/pipeline/kfp_mysql:1.7.0"
                             )

    
All system environment variables are shown in the following table:

.. csv-table::
   :file: sys_params.csv
