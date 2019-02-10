
from mrjob.job import MRJob 
from mrjob.step import MRStep 

class MRMappersideLeftJoin(MRJob):
    
    # Does an left join - append smaller table in memory to transactions data
    
    SORT_VALUES = True
    countries = {}
    
    def mapper_init(self):
        with open('/media/notebooks/SP18-1-maynard242/HW5/Countries.dat', 'r') as f:
            for line in f:
                tokens = line.strip().split('|')
                self.countries[tokens[1]] = tokens[0]
                
    # Iterate over records being read, append country names from memory, emit NULL if not in memory (right table)
               
    def mapper_leftjoin(self, _, record):
        self.increment_counter('Execution Counts', 'left mapper calls', 1)
        tokens = record.strip().split('|')
        if tokens[2] in self.countries:
            yield tokens[2], (tokens[0]+'|'+tokens[1]+'|'+self.countries[tokens[2]])
        else:
            yield tokens[2], (tokens[0]+'|'+tokens[1]+'|'+'NULL')
    
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper_leftjoin
                )
              ]
    
if __name__ == '__main__': 
    MRMappersideLeftJoin.run()  