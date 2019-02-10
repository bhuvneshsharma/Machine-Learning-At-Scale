
from mrjob.job import MRJob
from mrjob.protocol import RawProtocol

class RawProtocolExample(MRJob):
  
    INPUT_PROTOCOL = RawProtocol
    OUTPUT_PROTOCOL = RawProtocol
    INTERNAL_PROTOCOL = RawProtocol
    
    def mapper(self, key, value):
        assert key == "1001"
        assert value == "{\"value\":10}"
        yield "1001", "{\"value\":10}"
    def reducer(self, key, values):
        for value in values:
            assert key == "1001"
            assert value == "{\"value\":10}"
            yield "1001", "{\"value\":10}"

if __name__ == '__main__':
    RawProtocolExample.run()