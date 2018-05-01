import json
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

    def __init__(self, source=None):
        self.finalized = False
        self.dependences = set()
        self.source_filename = None
        self.operations = []
        self.color = None
        self.n_to_be_used_again = 0
        self.save_intermediate = None
        self.verbose = False
        self.result = None

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
        with open(self.source_filename) as file:
            for line in file:
                yield json.loads(line)

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

    def join(self, on, keys, strategy):
        """Add join operation to the graph"""
        if self.finalized:
            raise ComputeGraphError('Adding operations to finalized graph')
        self.operations.append(('_join', on, keys, strategy))
        self.dependences.add(on)
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
            self.source = source
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
            return result
        else:
            self.save_intermediate = save_intermediate
            self.verbose = verbose
            if source:
                self.change_source(source)
            result = list(self.__iter__())
            return result

    def __iter__(self):
        """Iterate over result. Triggers graph evaluation."""
        if self.result:
            yield from result
            self.n_to_be_used_again -= 1
            if not self.n_to_be_used_again:
                self.delete_result()
        elif not self.finalized:
            raise ComputeGraphError('Run of a nonfinalized graph')
        elif not self.source:
            raise ComputeGraphError('Source not specified')
        else:
            if not self.n_to_be_used_again:
                yield from self._result_generator()
            else:
                result = list(self._result_generator())
                yield from result

    def delete_result(self):
        """Delete result to free the memory"""
        self.result = None

    def _result_generator(self):
        """Internal function that iterates over operations in the graph and triggers evaluation of dependent graphs"""
        self._print('_result_generator entered')
        table = self.source()
        # self._print('source', self.source)
        # self._print('source (->list)', list(self.source()))
        for operation in self.operations:
            table = getattr(self, operation[0])(table, *operation[1:])
            # self._print('table', table)
        return table

    def _map(self, table, mapper):
        """Implementation of map operation"""
        self._print("_map with {}".format(mapper))
        for line in table:
            yield from mapper(line)

    def _getitems(self, line, keys):
        """Given tuple of keys evaluate values from line"""
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

    def _join(self, table, on, keys, strategy='inner'):
        """Implementation of join operation"""
        self._print("_join on {} with key {} and strategy {}".format(on, keys, strategy))
        on.verbose = self.verbose

        # INNER JOIN
        for first in table:
            for second in on:
                if _getitems(first, keys) == _getitems(second, keys):
                    yield {**first, **second}
