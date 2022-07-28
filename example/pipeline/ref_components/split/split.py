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
    negetive = get_artifact_path("output", "negetive")
    positvie = get_artifact_path("output", "positive")
    nums = get_artifact_path("input", "nums")

    threshold = int(sys.argv[1])
    negetive_fp = open(negetive, 'w')
    positvie_fp = open(positvie, 'w')

    with open(nums) as fp:
        num_list = json.load(fp)
    
    neg, pos = [], []
    for num in num_list:
        if num < threshold:
            neg.append(num)
        elif num > threshold:
            pos.append(num)

    print(f"negetive: {neg}\npositive:{pos}")
    
    with open(negetive, 'w') as fp:
        json.dump(neg, fp)
    
    with open(positvie, 'w') as fp:
        json.dump(pos, fp)
