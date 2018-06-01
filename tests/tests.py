import pytest

parentPath = os.path.abspath("/..")
if parentPath not in sys.path:
    sys.path.insert(0, parentPath)

import mrop


def test_import_and_run():
    input_table = [{'a' : 'b', 'c' : 'd'},
             {'1' : '2', '5' : '7'}
            ]
    graph = mrop.ComputeGraph(source=input_table)
    graph.finalize()
    graph.run()
    assert list(run) == input_table

