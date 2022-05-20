from paddleflow.pipeline import Pipeline
from paddleflow.pipeline import ContainerStep
from paddleflow.pipeline import FailureOptions
from paddleflow.pipeline import FAIL_CONTINUE
from paddleflow.pipeline import FAIL_FAST

def job_info():
    return {
        "PF_JOB_TYPE": "vcjob",
        "PF_JOB_MODE": "Pod",
        "PF_JOB_QUEUE_NAME": "ppl-queue",
        "PF_JOB_FLAVOUR": "flavour1",
    }

def echo_step(name, exit_error=False):
    command = f"echo {name}"
    command  = f"{command}; exit  1" if exit_error else command

    return ContainerStep(
        name=name,
        command=command
    )

@Pipeline(name="failure_option_and_postprocess_example", docker_env="registry.baidubce.com/pipeline/nginx:1.7.9", env=job_info())
def failure_option_and_postprocess_example():
    step1 = echo_step("step1")
    step2 = echo_step("step2")
    step3 = echo_step("step3")
    step2.after(step1)
    step3.after(step2)

    step4 = echo_step("step4", True)
    step5 = echo_step("step5")
    step5.after(step4)

def set_post_process(ppl):
    post_process = echo_step("step6")
    ppl.set_post_process(post_process)

def set_failure_options(ppl, strategy):
    fail = FailureOptions(strategy)
    ppl.failure_options = fail

if __name__ == "__main__":

    ppl = failure_option_and_postprocess_example()
    set_post_process(ppl)
    set_failure_options(ppl, FAIL_CONTINUE)
    
    result = ppl.run(fsname="your_fs_name")
    print(result)
