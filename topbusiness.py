from mrjob.job import MRJob
from mrjob.step import MRStep

import json
import  heapq



class TopBusiness(MRJob):
    """
    TopBusiness() counts the top 10 business by weighted review score
    """

    # Defining the MRJob steps	
    def steps(self):
        return[
            MRStep(mapper=self.mapper_1,
                   reducer=self.reducer_1),
            MRStep(reducer=self.reducer_2),
            MRStep(reducer=self.reducer_find_top)

        ]
    
    # Reads each line in the businesses.json and yields a key pair with the business_id and a tuple
    # the tuple consists of the vi, wi and 1 
    # the vi, the part of each review on the sum from the first formula 
    # the wi which is the upper part of the fraction in the second formula for each review
    # the 1 is to count how many times each word appears
    def mapper_1(self, _, line):
        line = line.strip()
        data = json.loads(line)
        bid = data.get("business_id")
        star = data.get("stars")
        useful = data.get("useful")
        vi = useful + 1
        wi = star*(useful+1)
        yield bid, (vi, wi, 1)

    # The reducer needs calculate all the needed scores for each business_id
    # it yields a key value pair with key the business_id and value a tuple
    # the tuple consists of the formula 1 result, the upper part of the fraction on formula 2, and the sum ofcounts
    # to calculate the final scores, the reducer process a tuple of tuples consist of the 3 indicators yield on the mapper
    def reducer_1(self, bid, tup):
	# make the tuple a list so we can work easier with it
        tup = list(tup)
        v = 0
	w = 0
	ct = 0
	for el in tup:
            v+= el[0]
            w+= el[1]
            ct+=el[2]
        yield bid, (v, w, ct)

    # The reducer needs to pass all the data to the final step 
    # it does so by creating the same key for all data, 
    # and having as data tupples consist of the weighted score and the business_id
    # Note that we only take into consideration the businesses with minimum of 5 reviews 
    def reducer_2(self, bid, tup):
	# make the tuple a list so we can work easier with it
        tup = list(tup)
	# minimum 5 reviews
        if tup[0][2] >= 5:
            yield None, (tup[0][1]/tup[0][0], bid)

    # finds the 10 biggest based on the counts
    def reducer_find_top(self, _, scores):
        for score in heapq.nlargest(10, scores):
            yield  score

#if the python script is called run the class
if __name__ == '__main__':
    TopBusiness().run()
