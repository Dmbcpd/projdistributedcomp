from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import  heapq

class ReviewAmountByUserID(MRJob):
    """
    ReviewAmountByUserID() maps the user ID to 1 from input json
    Combiner sums the amount of each user id
    Reduces sums
    Reducer #2 Finds Top 10
    """
    
    # Defining the MRJob steps
    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_uid,
                   combiner=self.combiner_count_uid,
                   reducer=self.reducer_count_uid),
            MRStep(reducer=self.reducer_find_top)
        ]

    # Reads each line in the reviews.json and yields a key pair with the user_id and 1
    def mapper_count_uid(self,_, line):
        line = line.strip()
        data = json.loads(line)
        uid = data.get("user_id", None)
        yield (uid, 1)

    # The combiner counts the times (sum) each user_id has been present in the review.json
    # yields a key pair with the user_id and the sum 
    def combiner_count_uid(self, uid, counts):
        yield (uid, sum(counts))

    # The reducer needs to pass all the data to the final step 
    # it does so by creating the same key for all data, 
    # and having as data tupples consist of the count sum and the user_id
    # Notice that the combiner might not have process all the key pairs with the same user_id
    # that is why the reducer has again the sum
    def reducer_count_uid(self, uid, counts):
        yield None, (sum(counts), uid)
    
    # finds the 10 largest based on the counts
    def reducer_find_top(self, _, uidcounts):
        for uidcount in heapq.nlargest(10, uidcounts):
            yield  uidcount

#if the python script is called run the class
if __name__ == '__main__':
    ReviewAmountByUserID().run()

