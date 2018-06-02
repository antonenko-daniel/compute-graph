import pytest
import os
import sys
import json
import ast


parentPath = os.path.abspath("../")
if parentPath not in sys.path:
    sys.path.insert(0, parentPath)

import mrop

simple_input = [
    {'a' : '1', 'b' : '2'},
    {'a' : '2', 'b' : '1'}
]

def test_import_and_run():
    graph = mrop.ComputeGraph(source=simple_input)
    graph.finalize()
    graph.run()
    assert list(graph) == simple_input

def test_map():
    def mapper(line):
        yield {**line, **{'map' : 'test'}}
    answer = [
        {'a' : '1', 'b' : '2', 'map' : 'test'},
        {'a' : '2', 'b' : '1', 'map' : 'test'}
    ]
    graph = mrop.ComputeGraph(source=simple_input)
    graph.map(mapper)
    graph.finalize()
    graph.run()
    assert list(graph) == answer

def test_sort():
    answer = [
        {'a' : '2', 'b' : '1'},
        {'a' : '1', 'b' : '2'}
    ]
    graph = mrop.ComputeGraph(source=simple_input)
    graph.sort(('b',))
    graph.finalize()
    graph.run()
    assert list(graph) == answer

def test_fold():
    def folder(line, initial):
        return {'count' : initial['count'] + 1}
    answer = [
        {'count' : 2}
    ]
    graph = mrop.ComputeGraph(source=simple_input)
    graph.fold(folder, initial={'count' : 0})
    graph.finalize()
    graph.run()
    assert list(graph) == answer

def test_reduce():
    simple_input = [
        {'a' : 1, 'b' : 2},
        {'a' : 2, 'b' : 1},
        {'a' : 2, 'b' : 10}
    ]
    def reducer(table):
        result = 0
        for line in table:
            result += line['b']
        yield {'a' : line['a'], 'result' : result}
    answer = [
        {'a' : 1, 'result' : 2},
        {'a' : 2, 'result' : 11}
    ]

    graph = mrop.ComputeGraph(source=simple_input)
    graph.reduce(reducer, keys=('a',))
    graph.finalize()
    graph.run()
    assert list(graph) == answer

cities = mrop.ComputeGraph(source='city_ids.txt')
cities.finalize()

names = mrop.ComputeGraph(source='citizens.txt')
names.finalize()


class TestJoin:
    def get_answer_from_file(self, filename):
        answer = []
        with open(filename) as file:
            for line in file:
                answer.append(ast.literal_eval(line))
        return answer

    def test_inner_join(self):
        join_inner = mrop.ComputeGraph(source=names)
        join_inner.join(on=cities, keys=('id',), strategy='inner')
        join_inner.sort(('id',))
        join_inner.finalize()
        join_inner.run()
        assert self.get_answer_from_file('join_inner.txt') == list(join_inner)

    def test_left_join(self):
        join_left = mrop.ComputeGraph(source=names)
        join_left.join(on=cities, keys=('id',), strategy='left')
        join_left.sort(('id',))
        join_left.finalize()
        join_left.run()
        assert self.get_answer_from_file('join_left.txt') == list(join_left)

    def test_right_join(self):
        join_right = mrop.ComputeGraph(source=names)
        join_right.join(on=cities, keys=('id',), strategy='right')
        join_right.sort(('id',))
        join_right.finalize()
        join_right.run()
        assert self.get_answer_from_file('join_right.txt') == list(join_right)

    def test_outer_join(self):
        join_outer = mrop.ComputeGraph(source=names)
        join_outer.join(on=cities, keys=('id',), strategy='outer')
        join_outer.sort(('id',))
        join_outer.finalize()
        join_outer.run()
        assert self.get_answer_from_file('join_outer.txt') == list(join_outer)

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



