import os, sys
import re
import math
from collections import Counter


parentPath = os.path.abspath("../..")
if parentPath not in sys.path:
    sys.path.insert(0, parentPath)

import mrop


def words_extractor_mapper(line):
    tokens = re.compile('[a-zA-z]+').findall(line["text"])
    # print('tokens', tokens)
    generator = (
        {
            "doc_id" : line["doc_id"],
            "word" : token.lower()
        }
        for token in tokens if token.isalpha()
    )
    yield from generator


def count_docs_folder(line, initial):
    return {'docs_count' : initial['docs_count'] + 1}


def unique_words_reducer(table):
    yield table[0]

def calc_idf_reducer(table):
    docs_n = 0
    for line in table:
        docs_n += 1
    total_n_of_docs = line['docs_count']
    yield {'word' : line['word'], 'idf' : docs_n / total_n_of_docs}

def tf_reducer(table):
    # print()
    # print('tf_reducer, table \n', table)
    counter = Counter()

    for line in table:
        counter[line["word"]] += 1

    total = sum(counter.values())
    for word, n in counter.items():
        # print("word, n", word, n)
        yield {
            "doc_id" : line["doc_id"],
            "word" : word,
            "tf" : n / total
        }


def tf_idf_top3(table):
    docs_values = []
    for line in table:
        docs_values.append((line['doc_id'],
                            line['tf'] * math.log(1 / line['idf']))
        )
    index = sorted(docs_values, key=lambda x : x[1])[-3:]

    yield {'term' : line['word'], 'index' : index}



source_filename = '../data/text_corpus.txt'


split_word = mrop.ComputeGraph(source=source_filename)
split_word.map(words_extractor_mapper)
split_word.finalize()


count_docs = mrop.ComputeGraph(source=source_filename)
count_docs.fold(count_docs_folder, {'docs_count' : 0})
count_docs.finalize()


count_idf = mrop.ComputeGraph(source=split_word)
count_idf.sort(('doc_id', 'word'))
count_idf.reduce(unique_words_reducer, keys=('doc_id', 'word'))
count_idf.join(on=count_docs, keys=tuple(), strategy='outer')
count_idf.sort(('word',))
count_idf.reduce(calc_idf_reducer, keys=('word',))
count_idf.finalize()


calc_index = mrop.ComputeGraph(source=split_word)
calc_index.sort(('doc_id',))
calc_index.reduce(tf_reducer, keys=('doc_id',))
calc_index.join(on=count_idf, keys=('word',), strategy='left')
calc_index.sort(('word',))
calc_index.reduce(tf_idf_top3, keys=('word',))
calc_index.finalize()


calc_index.run(verbose=True)
calc_index.save_to_file('tf_idf_result.txt')
