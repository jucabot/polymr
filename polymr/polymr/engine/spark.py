import datetime
from polymr import mem, load_from_modclassname

import inspect

try:
    from pyspark.context import SparkContext
except ImportError:
    pass



class SparkEngine():
    """
        Spark engine
    """
    
    _mapred = None
    sc = None
    
    def __init__(self,mapred,sc):
        self._mapred = mapred
        self.sc = sc    
    
    def run(self,input_reader,output_writer):
        
        start_time = datetime.datetime.now()
        self._mapred.reset()
        
        print "INFO:start job %s on a spark cluster" % self._mapred.__class__.__name__
        
        
        #broadcast params - to do
        
        
        source_file = inspect.getfile(self._mapred.__class__)
        module_name = self._mapred.__class__.__module__
        if module_name == '__main__':
            module_name = inspect.getmodulename(source_file)
       
        class_name = self._mapred.__class__.__name__
        
        mapred_name = self.sc.broadcast((module_name,class_name))
        
       
        
        #Load RDD input
        input_rdd = self.sc.textFile(input_reader.filename)
        
        #Run map
        
        rdd = input_rdd.flatMap(lambda line : load_from_modclassname(mapred_name.value).map(line))
        
        
        if "combine" in dir(self._mapred):
           
            rdd = rdd.groupByKey().map(lambda kv : load_from_modclassname(mapred_name.value).combine(kv[0],kv[1]))
            
        
        if "reduce" in dir(self._mapred):
            rdd = rdd.groupByKey().map(lambda kv : load_from_modclassname(mapred_name.value).reduce(kv[0],kv[1]))
           
             
        if not output_writer.is_memory:
            rdd.map(output_writer.dumps_output_value).saveAsTextFile(output_writer.filename)
        else:
            def load_line(kv):
                key, value = kv
                self._mapred.data_reduced[key] = [value]
            output = rdd.collect()
            map(load_line,output)
            output_writer.write(self._mapred.post_reduce())
        
        print "INFO: end job %s in %s with mem size of %d" % (self._mapred.__class__.__name__, (datetime.datetime.now()-start_time),mem.asizeof(self._mapred))
    
    
    