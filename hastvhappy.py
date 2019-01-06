from mrjob.job import MRJob
from mrjob.step import MRStep

import json

class HasTVandHasHappyHour(MRJob):
    """
    HasTVandHasHappyHour() counts the num of
    """

    def steps(self):
        return[
            MRStep(mapper=self.mapper_count_both,
                   reducer=self.reduce_counter)
        ]

    def mapper_count_both(self, _, line):
        line = line.strip()
        data = json.loads(line)
        bid = data.get("business_id")
        tv = ""
        hour = ""
        try:
            tv = data.get("attributes").get("HasTV")
            hour = data.get("attributes").get("HappyHour")
        except:
             yield "Unknown", 1
        if tv=="True" and hour=="True":
             yield "HasTVHappy", 1
        else:
             yield "Not", 1

    def reduce_counter(self, state, count):
        yield state, sum(count)

if __name__ == '__main__':
    HasTVandHasHappyHour().run()
