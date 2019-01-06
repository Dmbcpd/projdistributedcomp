from mrjob.job import MRJob
from mrjob.step import MRStep

import json
import  heapq

class MostUsedWords(MRJob):
    """
    MostUsedWords() counts the 25 most used words
    """

    def steps(self):
        return[
            MRStep(mapper=self.mapper_words,
                   combiner=self.combiner_words,
                   reducer=self.reducer_words),
            MRStep(reducer=self.reducer_find_top)

        ]

    def mapper_words(self, _, line):
        from nltk.tokenize import TweetTokenizer
        tknzr = TweetTokenizer()
        line = line.strip()
        data = json.loads(line)
        review = tknzr.tokenize(data.get("text"))
        for word in review:
            yield word.lower(), 1

    def combiner_words(self, word, counts):
        yield word, sum(counts)

    def reducer_words(self, word, counts):
        yield word, sum(counts)

    def reducer_find_top(self, _, counts):
        for count in heapq.nlargest(25, counts):
            yield  count

if __name__ == '__main__':
    MostUsedWords().run()

