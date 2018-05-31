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


def tf_reducer(table):
    counter = Counter()

    for line in table:
        counter[line["word"]] += 1

    total = sum(counter.values())
    for word, n in counter.items():
        if n >= 2 and len(word) >= 4:
            yield {
                "doc_id" : line["doc_id"],
                "word" : word,
                "tf" : n / total
            }

def total_tf_reducer(table):
    counter = Counter()

    for line in table:
        counter[line["word"]] += 1

    total = sum(counter.values())
    for word, n in counter.items():
        yield {
            "word" : word,
            "total_tf" : n / total
        }

def select_words_that_are_in_all_docs(table):
    table = list(table)
    if len(table) == table[0]["docs_count"]:
        yield from table


def top3_pmi_reducer(table):
    pmis = [(line['word'], line['tf'] / line['total_tf']) for line in table]
    pmis = sorted(pmis, key=lambda x : x[1])
    yield {'doc_id' : table[0]['doc_id'],
           'top-10' : pmis[-10:]
          }

# def forget_doc_mapper(line):
#     yield {'word' : line[word]}

# def total_tf_reducer(table):
#     counter = Counter()

#     for line in table:
#         counter[line["word"]] += 1

#     total = sum(counter.values())
#     for word, n in counter.items():
#         yield {
#             "word" : word,
#             "total_tf" : n / total
#         }

# def unique_words_reducer(table):
#     yield from set(table)

# def select_words_reducer(table):
#     table_copy = list(table)

#     current_doc = None
#     current_n = 1000
#     for line in table:
#         if line['doc_id'] != current_n:
#             if current_n < 2:
#                 return
#             current_doc = line['doc_id']
#             current_n = 0
#         else:
#             current_n +=1
#     if current_n <
#     len(table_copy) >= 2 * table_copy[0]['count_docs']
#     yield from table_copy

# def top3_pmi_reducer(table):
#     pmis = [line['tf'] / line['total_tft'] for line in table]
#     top = sorted(pmis)[-10:] if len(pmis) >= 10 else pmis
#     yield {'doc_id' : line['doc_id'],
#            'top-10' : top
#           }

source_filename = '../data/text_corpus.txt'


split_word = mrop.ComputeGraph(source=source_filename)
split_word.map(words_extractor_mapper)
split_word.finalize()

count_docs = mrop.ComputeGraph(source=source_filename)
count_docs.fold(count_docs_folder, {'docs_count' : 0})
count_docs.finalize()


tf = mrop.ComputeGraph(source=split_word)
tf.sort(('doc_id',))
tf.reduce(tf_reducer, keys=('doc_id',))
tf.join(on=count_docs, keys=tuple(), strategy='outer')
tf.sort(('word',))
tf.reduce(select_words_that_are_in_all_docs, keys=('word',))
tf.finalize()

total_tf = mrop.ComputeGraph(source=split_word)
total_tf.reduce(total_tf_reducer, keys=tuple())
total_tf.finalize()


pmi = mrop.ComputeGraph(source=tf)
pmi.join(on=total_tf, keys=('word',), strategy='inner')
pmi.sort(('doc_id',))
pmi.reduce(top3_pmi_reducer, keys=('doc_id',))
pmi.finalize()

pmi.run()
pmi.save_to_file('max_mutual_info_result.txt')
