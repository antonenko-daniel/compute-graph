import os, sys
import re
from collections import Counter

parentPath = os.path.abspath("../..")
if parentPath not in sys.path:
    sys.path.insert(0, parentPath)

import mrop


def words_extractor(line):
    tokens = re.compile('[a-zA-z]+').findall(line["text"])
    # print('tokens', tokens)
    generator = (
        {
            "doc_id" : line["doc_id"],
            "word" : token
        }
        for token in tokens if token.isalpha()
    )
    yield from generator

def words_in_doc_counter(table):
    counter = Counter()

    for line in table:
        counter[line["word"]] += 1
    # print("Counter:", list(counter.items()))
    for word, n in counter.items():
        # print("word, n", word, n)
        yield {
            "doc_id" : line["doc_id"],
            "word" : word,
            "n" : n
        }


word_count = mrop.ComputeGraph()
word_count.map(words_extractor)
word_count.sort(("doc_id", "word"))
word_count.reduce(words_in_doc_counter, keys=("doc_id", "word"))
word_count.finalize()

result = word_count.run(source='../data/simple_texts.txt', verbose=True)
print("First run:\n", result)

print()
print("Second run")

for line in word_count:
    print(line)

word_count.save_to_file('word_count_simple_result.txt')
# word_count.delete_output