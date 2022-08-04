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
    positive = get_artifact_path("input", "positive")
    result = get_artifact_path("output", "result")

    res =[]
    with open(positive) as fp:
        negs = json.load(fp)

        for num in negs:
            print(f"process num: {num}")
            res.append(num*num)
    
    print(f"result: {res}")
    with open(result, 'w') as fp:
        json.dump(res, fp)