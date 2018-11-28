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
    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_uid,
                   combiner=self.combiner_count_uid,
                   reducer=self.reducer_count_uid),
            MRStep(reducer=self.reducer_find_top)
        ]


    def mapper_count_uid(self,_, line):
        line = line.strip()
        data = json.loads(line)
        uid = data.get("user_id", None)
        yield (uid, 1)

    def combiner_count_uid(self, uid, counts):
        yield (uid, sum(counts))

    def reducer_count_uid(self, uid, counts):
        yield None, (sum(counts), uid)

    def reducer_find_top(self, _, uidcounts):
        for uidcount in heapq.nlargest(10, uidcounts):
            yield  uidcount

if __name__ == '__main__':
    ReviewAmountByUserID().run()

