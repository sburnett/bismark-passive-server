from collections import defaultdict
from datetime import datetime

def initialize_int_dict():
    return defaultdict(int)
def initialize_set_dict():
    return defaultdict(set)
def initialize_list_dict():
    return defaultdict(list)
def initialize_min_timestamp():
    return datetime.min
def initialize_max_timestamp():
    return datetime.max
def update_dict(first, second):
    second.update(first)
    return second
def sum_dicts(first, second):
    for key, value in first.items():
        second[key] += value
    return second
def return_negative_one():
    return -1
