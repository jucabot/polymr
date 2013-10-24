from polymr.mapreduce import MapReduce, FileInputReader, MemOutputWriter, HdfsInputReader,\
    MULTI_CORE, SPARK, HADOOP

from uuid import uuid1
try:
    from pyspark.context import SparkContext
    use_spark = True
except ImportError :
    use_spark = False
    
from multiprocessing import cpu_count


class MyMapRed(MapReduce):
    
    def map(self, text):
        words = text.split(',')
        return [(str(word),1) for word in words]
                
    def combine(self, key, values):
        return (key, sum(values))
        
    def reduce(self, key, values):
        return (key, sum(values))

def gen_input_file_sample(path,nloc=1000000):
    print "Generate a sample file of %d lines for %d Mbytes" % (nloc,nloc*19/100000)
    f = open(path,mode='w')
    map(lambda n: f.write(','.join(map(lambda i: str(i),range(100)))),range(nloc))
    f.close()

if __name__ == '__main__':

    sample_file_path = "/var/tmp/%s" % str(uuid1())
    nloc=1000000
    gen_input_file_sample(sample_file_path, nloc)
    
    
    sample_input = FileInputReader(sample_file_path)
   
    if use_spark:
        sc = SparkContext('local[%s]' % cpu_count(),'polymr job')
    
    print sample_input.count()
    print sample_input.count(engine=MULTI_CORE)
    print sample_input.count(engine=HADOOP)
    
    if use_spark:
        print sample_input.count(engine=SPARK,options={'spark-context': sc})
    
    
    mapred = MyMapRed()
    print sample_input.compute(mapred)
    print sample_input.compute(mapred,engine=MULTI_CORE)
    print sample_input.compute(mapred,engine=HADOOP)
    if use_spark:
        print sample_input.compute(mapred,engine=SPARK,options={'spark-context': sc})
    
    
    