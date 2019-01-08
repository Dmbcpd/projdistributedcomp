from mrjob.job import MRJob
from mrjob.step import MRStep

import json

class HasTVandHasHappyHour(MRJob):
    """
    HasTVandHasHappyHour() counts the num of
    """
    
    # Defining the MRJob steps
    def steps(self):
        return[
            MRStep(mapper=self.mapper_count_both,
                   reducer=self.reduce_counter)
        ]
    
     # Reads each line in the busineses.json and yields a key pair with the state and 1
     # the state can be HasTVHappy or Not or Unknown(if it is not specified on the dataset)
    def mapper_count_both(self, _, line):
        line = line.strip()
        data = json.loads(line)
        bid = data.get("business_id")
        tv = ""
        hour = ""
        try:
            tv = data.get("attributes").get("HasTV")
            hour = data.get("attributes").get("HappyHour")
        
        # the line does not contain the HasTV or the HappyHour attribute
        except:
             yield "Unknown", 1
        
        # has both HasTV and HappyHour
        if tv=="True" and hour=="True":
             yield "HasTVHappy", 1
                
        # has NOT both HasTV and HappyHour
        else:
             yield "Not", 1

    # reduce each state to its total amount
    def reduce_counter(self, state, count):
        yield state, sum(count)
        
#if the python script is called run the class
if __name__ == '__main__':
    HasTVandHasHappyHour().run()
