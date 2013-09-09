#Python Polymorphic MapReduce
Polymr is under BSD License

##What the fuck?
In the field of Big data analytics and data science, **some dataset are huge, some are not so big** (many Mb or Gb).
From raw data extractraction to predictive modelling the dataset size and computating requirements are volatile.
Hadoop or Spark are incredible platform to compute large dataset but overkill for Mb-Gb datasets

##What is Polymr?
Polymr provides a MapReduce abstraction over 3 MapReduce engines :
* **In-memory Single process MapReduce** engine for real debugging and dummy maps (ex counting, mean, median, frequency distribution, etc...).
* **In-memory Multi processor MapReduce** engine for mid sized (many Gb) datasets with computational tasks, ex RandomForest, NLP, etc
* **Hadoop streaming** interface for large data set
* **Spark PySpark** interface for master/node processing

**Use the same code, run on different MapReduce engines** to reach the best performance and cost.

Polymr has 32 types input/output providers :
* FileInputReader , FileOutputReader : Text file reader and writer - the job reads or writes data from or to a file
* MemInputReader, MemOutputWriter : Array reader and writer - the job reads or writes data from or to memory for aggregate or small data pipeline
* HdfsInputReader, HdfsOutputWriter : Hdfs file reader and writer - the job (Hadoop only) reads and writes from or to hdfs
You may mix the providers into a MR job

Polymr provides a **profiler to identify the most performant** (time and cost) MapReduce engine, based on the local DRAM limitation and the map function mean duration while analyzing a sample of the dataset (1000 lines)

#Installation

* Download from github
* For Hadoop, install Hadoop 1.2 as Edge node, Single Node or Multi node and set HADOOP_HOME and POLYMR_HOME env variables
* For Spark, install Spark with PySpark

Run on linux only (env var), python 2.6 or above

Note : Polymr is neutral for Hadoop, it is NOT required to install polymr on task nodes


* Dependency (not mandatory, if not exist use json) : cjson - pip install cjson

# Usage

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
       print mapred.profile(sample_input)
       print sample_input.compute(mapred)
       print sample_input.compute(mapred,engine=MULTI_CORE)
       print sample_input.compute(mapred,engine=HADOOP)
       if use_spark:
           print sample_input.compute(mapred,engine=SPARK,options={'spark-context': sc})

