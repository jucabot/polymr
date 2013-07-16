"""
    Test protocol
"""
import cProfile as profile
from polymr.inout import FileInputReader, MemOutputWriter, MemInputReader
from polymr.mapreduce import MapReduce
import time
from uuid import uuid1
from pyspark.context import SparkContext

from polymr import load_from_modclassname
import inspect
from multiprocessing import cpu_count

class Count(MapReduce):
    """
        List the count of messages
    """
    
    def map(self, text):
        return [('count',1)]
                
    def combine(self, key, values):
        return (key, sum(values))
        
    def reduce(self, key, values):
        return (key, sum(values))
        
class CountNoCombine(MapReduce):
    
    def map(self, text):
        return [('count',1)]
        
    def reduce(self, key, values):
        return (key, sum(values))

class LongCount(MapReduce):
    
    
    def map(self, text):
        time.sleep(1.0/10000.0)
        return [('count',1)]
                
    def combine(self, key, values):
        return (key, sum(values))
        
    def reduce(self, key, values):
        return (key, sum(values))

class GroupBy(MapReduce):
    
    
    def map(self, text):
        return [('count_%s' % str(i),1) for i in range(1000)]
                
    def combine(self, key, values):
        return (key, sum(values))
        
    def reduce(self, key, values):
        return (key, sum(values))

def test(mapred,test_val,options={}):
    print "Test engine - Single core"
    mapred.run(sample_input,out,debug=True,options=options)
    assert out.data[0][1][0] == test_val

    print "Test engine - multi core"
    #profile.runctx('mapred.run(sample_input,out)', globals(), locals())
    mapred.run(sample_input,out,options=options)
    assert out.data[0][1][0] == test_val
    
    
    print "Test engine - local hadoop"
    mapred.run(sample_input,out,engine="local-hadoop",options=options)
    assert out.data[0][1][0] == test_val
    
    print "Test engine - spark"
    mapred.run(sample_input,out,engine="spark",options=options)
    assert out.data[0][1][0] == test_val
        
    

def gen_input_file_sample(path,nloc=1000000):
    print "Generate a sample file of %d lines for %d Mbytes" % (nloc,nloc*19/100000)
    f = open(path,mode='w')
    map(lambda n: f.write(''.join(map(lambda i: str(i),range(100))) + '\n'),range(nloc))
    f.close()
    

if __name__ == '__main__':

    
    #Generate sample file
    sample_file_path = "/var/tmp/%s" % str(uuid1())
    nloc=1000000
    gen_input_file_sample(sample_file_path, nloc)
    

    
    sample_input = FileInputReader(sample_file_path)
   
    
    out = MemOutputWriter()
    
    
    sc = SparkContext('local[%s]' % cpu_count(),'polymr job')
    options = {'spark-context': sc}
    
    
    print "Test Count mapred"
    engine, diags = Count().profile(sample_input)
    print "Recommended engine : %s" % engine
    print diags
    test(Count(),nloc,options)
    
    
    
    print "**********************"
    
    print "Test Long Count mapred"
    engine, diags = LongCount().profile(sample_input)
    print "Recommended engine : %s" % engine
    print diags
    test(LongCount(),nloc,options)
    
    print "**********************"
      
    print "Test  Count No combine mapred"
    engine, diags = CountNoCombine().profile(sample_input)
    print "Recommended engine : %s" % engine
    print diags
    test(CountNoCombine(),nloc,options)
    
    print "**********************"
    print "Test  GroupBy mapred"
    engine, diags = GroupBy().profile(sample_input)
    print "Recommended engine : %s" % engine
    print diags
    test(GroupBy(),nloc,options)
    
    print "Test completed"