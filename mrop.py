import json
import sys
"""compute-graph by Antonenko Daniil (May 2018)

The module implements operations over tables, each line represented by a json-like structure.
"""


class ComputeGraphError(Exception):
    """Basic error, specific for ComputeGraph class"""
    pass


class ComputeGraph(object):
    """Each graph is defined as sequence of elementary operation (map, sort, fold, reduce, join). 
    Graphs can have dependencies via join operation.

    After being defined, graph should be finalized and then it can be evaluated on an arbitrary input, or used
    as a dependence for another graph. 
    """

    def __init__(self, source=None, verbose=False):
        self.finalized = False
        self.dependences = []
        self.operations = []
        self.visited_by_sort = False
        self.n_to_be_used_again = 0
        self.save_intermediate = None
        self.verbose = verbose
        self.result = None

        self.source_data = None
        self.source_filename = None

        if source:
            self.change_source(source)
        else:
            self.source = None

    def _print(self, *args, **kwargs):
        """Print if self.verbose is True"""
        if self.verbose:
            print(*args, **kwargs)

    def parse_file(self):
        """Make a generator from a json file"""
        self._print('_parse_file entered')
        with open(self.source_filename) as file:
            for line in file:
                yield json.loads(line)

    def source_wrapper(self):
        """wrapper for source not from file"""
        self._print('_source_wrapper entered, class=', self)
        yield from iter(self.source_data)

    def map(self, mapper):
        """Add map operation to the graph"""
        if self.finalized:
            raise ComputeGraphError('Adding operations to finalized graph')
        self.operations.append(('_map', mapper))
        return self

    def sort(self, keys):
        """Add sort operation to the graph"""
        if self.finalized:
            raise ComputeGraphError('Adding operations to finalized graph')
        self.operations.append(('_sort', keys))
        return self

    def fold(self, folder, initial=None):
        """Add fold operation to the graph"""
        if self.finalized:
            raise ComputeGraphError('Adding operations to finalized graph')
        self.operations.append(('_fold', folder, initial))
        return self      

    def reduce(self, reducer, keys):
        """Add reduce operation to the graph"""
        if self.finalized:
            raise ComputeGraphError('Adding operations to finalized graph')
        self.operations.append(('_reduce', reducer, keys))
        return self

    def join(self, on, keys, strategy='inner'):
        """Add join operation to the graph"""
        if self.finalized:
            raise ComputeGraphError('Adding operations to finalized graph')
        self.operations.append(('_join', on, keys, strategy))
        if isinstance(on, ComputeGraph):
            self.dependences.append(on)
        return self

    def finalize(self):
        """Finalize graph sequence: no operation addition afterwards"""
        if self.finalized:
            raise ComputeGraphError('Graph already finalized')
        else:
            self.finalized=True
        return self

    def change_source(self, source):
        """Change source for the graph

        source -- generator or str with filename
        """
        if isinstance(source, str):
            self.source_filename = source
            self.source = self.parse_file
        else:
            if not hasattr(source, '__iter__'):
                raise ComputeGraphError('source is neither str nor iterable')
            self.source_data = source
            self.source = self.source_wrapper

        return self


    def run(self, save_intermediate=None, source=None, verbose=False):
        """Run the calculation, defined by the graph (should be finalized)

        Keyword arguments:
        save_intermediate -- None, True or list of dependent graphs (default=None)
                             Keep the result of the evaluation in the memory (in each graph)
        source            -- generator or str with filename (default=None)
                             If not None change the source for the graph
        verbose           -- True/False (default=None)
                             Whether to trace evaluation
        """
        if self.result:
            return self.result
        else:
            self.save_intermediate = save_intermediate
            self.verbose = verbose
            if source:
                self.change_source(source)
            self.result = list(self.__iter__())
            return self.result

    def __iter__(self):
        """Iterate over result. Triggers graph evaluation."""
        if self.result:
            self._print("\tresult already here, class = ", self)
            yield from self.result
            self.n_to_be_used_again -= 1
            if not self.n_to_be_used_again:
                self.delete_result()
        elif not self.finalized:
            raise ComputeGraphError('Run of a nonfinalized graph')
        elif not self.source:
            raise ComputeGraphError('Source not specified')
        else:
            self._print("\twill evaluate result, class = ", self)
            if not self.n_to_be_used_again:
                yield from self._plan_and_run()
            else:
                self.result = list(self._plan_and_run())
                yield from self.result

    def delete_result(self):
        """Delete result to free the memory"""
        self.result = None

    def _plan_and_run(self):
        """First triggers topological sort, then --- computation"""
        if not self.visited_by_sort:
            self._topological_sort()

        # sys.exit()
        self.visited_by_sort = False
        yield from self._result_generator()


    def _topological_sort(self):
        """Topological sort. The idea is first to traverse the composition of graphs in the same order
        that it will be traversed in the computation (when entering a graph that was already passed, further
        dependencies are not added to the sequence).
        Then those graphs that are computed more then once are told that they should save the result and delete
        it in the last call (to free the memory).
        """
        self._print('_topological_sort entered')
        # self._print('_topological_sort entered, self={}, dependences='.format(self), self.dependences)
        sequence = []
        sequence = self._traverse(sequence)
        self._print('_topological_sort got sequence', sequence)

        for i, graph in enumerate(sequence[:-1]):
            if not graph.n_to_be_used_again:
                graph.n_to_be_used_again = sequence[i + 1:].count(graph)

    def _traverse(self, sequence):
        """Traverse the composition of graphs in the same order as further during the computation
        """
        # print('_traverse entered, sequence = ', sequence)
        if self.source == self.source_wrapper and isinstance(self.source_data, ComputeGraph):
            self.dependences.insert(0, self.source_data)
        # print('_traverse entered, self = {}, dependences={}'.format(self, self.dependences))

        sequence.append(self)
        if not self.visited_by_sort:
            self.visited_by_sort = True
            for link in self.dependences:
                sequence = link._traverse(sequence)
        return sequence

    def _result_generator(self):
        """Internal function that iterates over operations in the graph and triggers evaluation of dependent graphs"""
        self._print('_result_generator entered, self = ', self)
        table = self.source()
        if isinstance(self.source_data, ComputeGraph):
            self.source_data.verbose = self.verbose
        # print('table', list(table))
        # self._print('source', self.source)
        # self._print('source (->list)', list(self.source()))
        for operation in self.operations:
            table = getattr(self, operation[0])(table, *operation[1:])
            # print('table', list(table))
            # self._print('table', table)
        return table

    def _map(self, table, mapper):
        """Implementation of map operation"""
        self._print("_map with {}".format(mapper))
        # print('table', list(table))
        for line in table:
            yield from mapper(line)

    def _getitems(self, line, keys):
        """Given tuple of keys evaluate values from line"""
        # print('_getitems, line={}, keys={}'.format(line, keys))
        return tuple(line[k] for k in keys)

    def _sort(self, table, keys):
        """Implementation of sort operation"""
        self._print("_sort using keys={}".format(keys))
        yield from iter(sorted(table, key=lambda line: self._getitems(line, keys)))

    def _fold(self, table, folder, initial):
        """Implementation of fold operation"""
        self._print("_fold with folder {} and initial {}".format(folder, initial))
        for line in table:
            initial = folder(line, initial)
        yield initial

    def _reduce(self, table, reducer, keys):
        """Implementation of reduce operation"""
        self._print("_reduce with reducer {} and keys {}".format(reducer, keys))
        current_keys = None
        current_subtable = None
        for line in table:
            # self._print("current_keys: {}, keys: {}".format(current_keys, self._getitems(line, keys)))
            if current_keys != self._getitems(line, keys):
                if current_subtable:
                    yield from reducer(current_subtable)
                current_keys = self._getitems(line, keys)
                current_subtable = [line]
            else:
                current_subtable.append(line)
        # TODO: check sorting

    def _inner_join(self, table, on, keys):
        """Realization of inner join"""
        for first in on:
            for second in table:
                # print('first, second', first, second)
                # print('keys', keys)
                if self._getitems(first, keys) == self._getitems(second, keys):
                    yield {**first, **second}

    def _left_join_addition(self, table, on, keys):
        """Realization of additional elements computation during left join"""
        # print('_left_join_addition entered')
        for first in table:
            # print('first', first)
            second_matched = False
            for second in on:
                # print('second', second)
                if self._getitems(first, keys) == self._getitems(second, keys):
                    second_matched = True
            if not second_matched:
                # print(first)
                yield {**first, **{k : None for k in set(second.keys()) - set(keys)}}

    def _right_join_addition(self, table, on, keys):
        """Realization of additional elements computation during right join"""
        yield from self._left_join_addition(on, table, keys)

    def _join(self, table, on, keys, strategy='inner'):
        """Implementation of join operation. Tables should not have coicident keys except those that used to join."""
        self._print("_join on {} with key {} and strategy {}".format(on, keys, strategy))
        on.verbose = self.verbose

        if strategy == 'inner':
            table = list(table)
            yield from self._inner_join(table, on, keys)
        elif strategy == 'left':
            table = list(table)
            on = list(on)
            yield from self._inner_join(table, on, keys)
            yield from self._left_join_addition(table, on, keys)
        elif strategy == 'right':
            table = list(table)
            on = list(on)
            yield from self._inner_join(table, on, keys)
            yield from self._right_join_addition(table, on, keys)
        elif strategy == 'outer':
            table = list(table)
            on = list(on)
            yield from self._inner_join(table, on, keys)
            yield from self._left_join_addition(table, on, keys)
            yield from self._right_join_addition(table, on, keys)
        else:
            raise ValueError('Unknown strategy for join')

    def save_to_file(self, filename):
        """Saves the result to file, each row from the table to json-like string, ended with '\n' """
        if not self.result:
            raise ComputeGraphError('The graph is not computed')
        else:
            with open(filename, 'w') as file:
                for line in self.result:
                    file.write(str(line) + '\n')
