"""
    Test protocol
"""
import cProfile as profile
from polymr.inout import FileInputReader, MemOutputWriter, MemInputReader
from polymr.mapreduce import MapReduce
import time
from uuid import uuid1

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
        
class CountNoCombine(MapReduce):
    
    def map(self, text):
        self.collect('count',1)
        
    def reduce(self, key, values):
        self.emit(key, sum(values))

class LongCount(MapReduce):
    
    
    def map(self, text):
        time.sleep(1.0/10000.0)
        self.collect('count',1)
                
    def combine(self, key, values):
        self.compact(key, sum(values))
        
    def reduce(self, key, values):
        self.emit(key, sum(values))

class GroupBy(MapReduce):
    
    
    def map(self, text):
        
        for i in range(1000):
            self.collect('count_%s' % str(i),1)
                
    def combine(self, key, values):
        self.compact(key, sum(values))
        
    def reduce(self, key, values):
        self.emit(key, sum(values))

def test(mapred):
    print "Test engine - Single core"
    mapred.run(sample_input,out,debug=True)
    

    print "Test engine - multi core"
    #profile.runctx('mapred.run(sample_input,out)', globals(), locals())
    mapred.run(sample_input,out)
    
    try:
        print "Test engine - local hadoop"
        mapred.run(sample_input,out,engine="local-hadoop")
    except:
        pass

def gen_input_file_sample(path,nloc=1000000):
    print "Generate a sample file of %d lines for %d Mbytes" % (nloc,nloc*19/100000)
    f = open(path,mode='w')
    map(lambda n: f.write(''.join(map(lambda i: str(i),range(100))) + '\n'),range(nloc))
    f.close()
    

if __name__ == '__main__':

    #Generate sample file
    sample_file_path = "/var/tmp/%s" % str(uuid1())
    gen_input_file_sample(sample_file_path, nloc=1000000)
    
    sample_input = FileInputReader(sample_file_path)
   
    
    out = MemOutputWriter()
    
    
    print "Test Count mapred"
    engine, diags = Count().profile(sample_input)
    print "Recommended engine : %s" % engine
    print diags
    test(Count())
    
    print "**********************"
    
    print "Test Long Count mapred"
    engine, diags = LongCount().profile(sample_input)
    print "Recommended engine : %s" % engine
    print diags
    test(LongCount())
    
    print "**********************"
      
    print "Test  Count No combine mapred"
    engine, diags = CountNoCombine().profile(sample_input)
    print "Recommended engine : %s" % engine
    print diags
    test(CountNoCombine())
    
    print "**********************"
    print "Test  GroupBy mapred"
    engine, diags = GroupBy().profile(sample_input)
    print "Recommended engine : %s" % engine
    print diags
    test(GroupBy())
    
    print "Test completed"