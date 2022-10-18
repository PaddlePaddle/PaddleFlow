import ray
import random
import time
import math

from fractions import Fraction


ray.init(address='auto')
@ray.remote
def pi4_sample(sample_count):
    """pi4_sample runs sample_count experiments, and returns the
    fraction of time it was inside the circle.
    """
    in_count = 0
    for i in range(sample_count):
        x = random.random()
        y = random.random()
        if x*x + y*y <= 1:
            in_count += 1
    return Fraction(in_count, sample_count)


SAMPLE_COUNT = 1000 * 1000
FULL_SAMPLE_COUNT = 100 * 1000 * 1000
BATCHES = int(FULL_SAMPLE_COUNT / SAMPLE_COUNT)
print(f'Doing {BATCHES} batches')
results = []
for _ in range(BATCHES):
    results.append(pi4_sample.remote(sample_count = SAMPLE_COUNT))
output = ray.get(results)