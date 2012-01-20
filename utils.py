from collections import defaultdict
from datetime import datetime

def initialize_int_dict():
    return defaultdict(int)
def initialize_int_pair():
    return [int(), int()]
def initialize_int_pair_dict():
    return defaultdict(initialize_int_pair)
def initialize_int_triple():
    return [int(), int(), int()]
def initialize_int_triple_dict():
    return defaultdict(initialize_int_triple)
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
def update_set(first, second):
    second.update(first)
    return second
def union_set_dicts(first, second):
    for key, dataset in first.iteritems():
        second[key].update(dataset)
    return second
def sum_dicts(first, second):
    for key, value in first.iteritems():
        second[key] += value
    return second
def sum_pair_dicts(first, second):
    for key, value in first.iteritems():
        second[key][0] += value[0]
        second[key][1] += value[1]
    return second
def min_dicts(first, second):
    for key, value in first.iteritems():
        second[key] = min(value, second[key])
    return second
def overwrite_dict(first, second):
    for key, value in first.iteritems():
        second[key] = value
    return second
def merge_disjoint_dicts(first, second):
    for key, value in first.iteritems():
        if key in second:
            raise ValueError('dictionaries are not disjoint')
        second[key] = value
    return second
def return_negative_one():
    return -1
def append_lists(first, second):
    second.extend(first)
    return second
