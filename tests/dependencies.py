import pytest
import os
import sys
import json
import ast


parentPath = os.path.abspath("../")
if parentPath not in sys.path:
    sys.path.insert(0, parentPath)

import mrop


def test_topological_sort_and_dependecies():
    zero = mrop.ComputeGraph(source='city_ids.txt')
    zero.finalize()

    a = mrop.ComputeGraph(source=zero)
    a.sort(('id',))
    a.finalize()

    b = mrop.ComputeGraph(source=a)
    b.sort(keys=('city',))
    b.finalize()

    c = mrop.ComputeGraph(source=b, verbose=True)
    c.join(on=a, keys=('id', 'city'))
    c.finalize()

