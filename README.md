compute-graph by Antonenko Daniil (May 2018)

The module implements operations over tables, each line represented by a json-like structure.

Each graph is defined as sequence of elementary operation (map, sort, fold, reduce, join). 
Graphs can have dependencies via join operation.

After being defined, graph should be finalized and then it can be evaluated on an arbitrary input, or used
as a dependence for another graph.
