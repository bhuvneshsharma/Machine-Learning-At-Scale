
from mrjob.job import MRJob 
from mrjob.step import MRStep 

class MRMappersideJoin(MRJob):

    # Does an inner join - load smaller table into memoery (countries.dat)
    
    SORT_VALUES = True
    countries = {}
    
    def mapper_init(self):
        with open('/media/notebooks/SP18-1-maynard242/HW5/Countries.dat', 'r') as f:
            for line in f:
                tokens = line.strip().split('|')
                self.countries[tokens[1]] = tokens[0]
                                 
    # Compare transactions dat against memory and append country names
    
    def mapper_innerjoin(self, _, record):
        self.increment_counter('Execution Counts', 'inner mapper calls', 1)
        tokens = record.strip().split('|')
        if tokens[2] in self.countries:
            yield tokens[2], (tokens[0]+'|'+tokens[1]+'|'+self.countries[tokens[2]])
            
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper_innerjoin
                )
              ]
    
if __name__ == '__main__': 
    MRMappersideJoin.run()
                       