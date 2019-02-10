
from mrjob.job import MRJob

class MRMappersideRightJoin(MRJob):
    
    countries = {}
    ex_list = []
    
    def mapper_init(self):
        with open('/media/notebooks/SP18-1-maynard242/HW5/Countries.dat', 'r') as f:
            for line in f:
                tokens = line.strip().split('|')
                self.countries[tokens[1]] = tokens[0]
                self.ex_list = self.countries.keys()
                
    # Emit records if keys exist in both tables; collect list of see country keys in ex_list
    
    def mapper(self, _, record):
        self.increment_counter('Execution Counts', 'right mapper calls', 1)
        tokens = record.strip().split('|')
        
        if tokens[2] in self.ex_list:
            self.ex_list.remove(tokens[2])
        
        if tokens[2] in self.countries.keys():
            yield tokens[2], (tokens[0]+'|'+tokens[1]+'|'+self.countries[tokens[2]])
        
    # Compare country list in memory with that see in records, emit null for that which is in memory but not seen
        
    def mapper_final(self):
        self.increment_counter('Execution Counts', 'final mapper calls', 1)
        for key in self.ex_list:
            yield key, ('NULL|NULL|'+self.countries[key])
            
    def reducer(self, key, values):
        cur_key = []
        for value in values:
            if key == cur_key and value.startswith('NULL'):
                pass
            else:
                yield key, value
            cur_key = key
        
        
if __name__ == '__main__': 
    MRMappersideRightJoin.run()    