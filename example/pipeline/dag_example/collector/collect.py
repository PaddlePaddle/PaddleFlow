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
    negetive = get_artifact_path("input", "negetive")
    positive = get_artifact_path("input", "positive")

    collection = get_artifact_path("output", "collection")

    print("begin to collect the result of process_positive")
    with open(negetive) as fp:
        n_nums = json.load(fp)
        print(f"collect num from process_negetive: {n_nums}")

    print("begin to collect the result of process_positive")
    with open(positive) as fp:
        p_nums = json.load(fp)
        print(f"collect num from process_positive: {p_nums}")

    print(f"all num in colletions: {n_nums + p_nums}")
    with open(collection, 'w') as fp:
        json.dump(n_nums + p_nums, fp)
        

    
