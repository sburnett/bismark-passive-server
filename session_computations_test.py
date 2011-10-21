from session_computations import merge_timeseries

from collections import defaultdict
import unittest

class MergeTimeseriesTest(unittest.TestCase):
    def test_merge(self):
        first = defaultdict(int, { 1: 1, 2: 2 })
        second = defaultdict(int, { 1: 4, 3: 8 })
        result = defaultdict(int, { 1: 5, 2: 2, 3: 8 })
        merge_timeseries(first, second)
        self.assertTrue(second == result)

if __name__ == '__main__':
    unittest.main()
