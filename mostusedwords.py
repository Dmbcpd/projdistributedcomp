from mrjob.job import MRJob
from mrjob.step import MRStep

import json
import  heapq

class MostUsedWords(MRJob):
    """
    MostUsedWords() counts the 25 most used words
    """

     # Defining the MRJob steps
    def steps(self):
        return[
            MRStep(mapper=self.mapper_words,
                   combiner=self.combiner_words,
                   reducer=self.reducer_words),
            MRStep(reducer=self.reducer_find_top)

        ]

    # Reads each line in the reviews.json and yields a key pair with the word and 1
    def mapper_words(self, _, line):
        from nltk.tokenize import TweetTokenizer
        tknzr = TweetTokenizer()
        line = line.strip()
        data = json.loads(line)
        # we tokenize the reviews
        review = tknzr.tokenize(data.get("text"))
        for word in review:
            yield word.lower(), 1
    
    # The combiner counts the times (sum) each word has been present in the review.json
    # yields a key pair with the word and the sum 
    def combiner_words(self, word, counts):
        yield word, sum(counts)

    # The reducer needs to pass all the data to the final step 
    
    # Notice that the combiner might not have process all the key pairs for the same word
    # that is why the reducer has again the sum
    def reducer_words(self, word, counts):
        yield word, sum(counts)

    # finds the 25 most used words based on the counts
    def reducer_find_top(self, _, counts):
        for count in heapq.nlargest(25, counts):
            yield  count

#if the python script is called run the class
if __name__ == '__main__':
    MostUsedWords().run()

