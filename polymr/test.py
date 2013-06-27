"""
    Test protocol
"""

        
#from polymr.common import Count
from polymr.inout import FileInputReader, MemOutputWriter
from polymr.mapreduce import MapReduce
class Count(MapReduce):
    """
        List the count of messages
    """
    
    def map(self, text):
        self.collect('count',1)
                
    def combine(self, key, values):
        self.compact(key, sum(values))
        
    def reduce(self, key, values):
        self.emit(key, sum(values))
        
if __name__ == '__main__':

    sample_input = FileInputReader("/home/predictiveds/Dropbox/polymr/sample.txt")
    out = MemOutputWriter()

    print "Test engine - Single core"
    Count().run(sample_input,out,debug=True)
    print out.data[0][1][0]
    
    
    print "Test engine - multi core"
    Count().run(sample_input,out)
    print out.data[0][1][0]
    
    print "Test engine - local hadoop"
    Count().run(sample_input,out,engine="local-hadoop")
    print out.data[0][1][0]
       
    print "Test completed"