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
def initialize_min_timestamp_dict():
    return defaultdict(initialize_min_timestamp)
def initialize_max_timestamp_dict():
    return defaultdict(initialize_max_timestamp)
def update_dict(first, second):
    second.update(first)
    return second
def sum_dicts(first, second):
    for key, value in first.iteritems():
        second[key] += value
    return second
def min_dicts(first, second):
    for key, value in first.iteritems():
        second[key] = min(value, second[key])
    return second
def return_negative_one():
    return -1
