Эта папка содержит примеры использования библиотеки ComputeGraph

### word_count
Принимает на вход корпус текстов из 'data/text_corpus.txt'
Считает количество вхождений слов в документы.

word_count_simple содержит версию, принимающую очень маленький файл 'data/simple_texts.txt'

### tf-idf
Принимает на вход корпус текстов из 'data/text_corpus.txt'
Для каждого слова вычисляет топ-3 документов по tf-idf, которая имеет следующий вид:
$$
	TF_IDF(word_i, doc_i) = (frequency of word_i in doc_i) \cdot \log \left( \frac{|total number of docs|}{|docs where word_i is present|} \right) 
$$

### max_mutual_info 
Принимает на вход корпус текстов из 'data/text_corpus.txt'
Считает для каждого текста топ-10 слов, каждое из которых длиннее четырех символов и встречаются в каждом из документов не менее двух раз по метрике Pointwise mutual information:
$$
	pmi(word_i, doc_i) = \log \left( \frac{frequency of word_i in doc_i}{frequency of word_i in all documents combined} \right)
$$

### average_velocity
Принмает на вход данные по перемещениям машин в Москве из 'data/travel_times.txt'
и данные по координатам графа, содержащего участки улиц из 'data/graph_data.txt'

Вычисляет среднюю скорость в Москве по дням недели и часам