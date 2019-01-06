from mrjob.job import MRJob
from mrjob.step import MRStep

import json
import  heapq



class TopBusiness(MRJob):
    """
    TopBusiness() counts the top 10 business by weighted review score
    """

    def steps(self):
        return[
            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2),
            MRStep(reducer=self.reducer_find_top)

        ]

    def mapper_1(self, _, line):
        line = line.strip()
        data = json.loads(line)
        bid = data.get("business_id")
        star = data.get("stars")
        useful = data.get("useful")
        vi = useful + 1
        wi = star*(useful+1)
        yield bid, (vi, wi, 1)

    def reducer_1(self, bid, tup):
        tup = list(tup)
        v = 0
	w = 0
	ct = 0
	for el in tup:
            v+= el[0]
            w+= el[1]
            ct+=el[2]
        yield bid, (v, w, ct)

    def reducer_2(self, bid, tup):
        tup = list(tup)
        if tup[0][2] >= 5:
            yield None, (tup[0][1]/tup[0][0], bid)

    def reducer_find_top(self, _, scores):
        for score in heapq.nlargest(10, scores):
            yield  score

if __name__ == '__main__':
    TopBusiness().run()
