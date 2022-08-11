import os
import sys
import json

def get_artifact_path(art_type, art_name):
    """ 获取 aritfact 在环境变量中的名称
    """
    type_map = {"input": "INPUT", "output": "OUTPUT"}
    prefix = f"PF_{type_map[art_type]}_ARTIFACT"
    art_env_name = f"{prefix}_{art_name.upper()}"

    path = os.getenv(art_env_name)
    if not path:
        print(f"cannot get the path of aritfact[{art_name}]")
        sys.exit(1)

    return path


if __name__ == "__main__":
    nums = get_artifact_path("input", "nums")
    result = get_artifact_path("output", "result")
    
    sum = 0
    for path in nums.split(","):
        with open(path) as fp:
            num = int(fp.read().strip())
            print(f"num in path[{path}]: {num}")

            sum += num
    
    print(f"result: {sum}")
    with open(result, 'w') as fp:
        fp.write(str(sum))