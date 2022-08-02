"""
PurPose: Generate random numbers as required 
"""
import os
import sys
import json 

from random import randint

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
    if len(sys.argv) < 4:
        print(f"Usage: python3 {sys.argv[0]} [num] [lower] [upper]")
        sys.exit(1)

    num = int(sys.argv[1])
    lower = int(sys.argv[2])
    upper = int(sys.argv[3])

    random_num = get_artifact_path("output", "random_num")

    if not random_num:
        print("cannot get the path for saving result")
        sys.exit(1)

    res = []
    for i in range(num):
        r_n = randint(lower, upper)
        print(f"random num: {r_n}")

        res.append(r_n)

    with open(random_num, 'w') as fp:
        json.dump(res, fp)