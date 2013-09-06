from multiprocessing import cpu_count
from polymr.engine.hadoop import HadoopEngine
from polymr.engine.smp import SingleCoreEngine, MultiCoreEngine
import time
from polymr import mem
from polymr.engine.spark import SparkEngine
import datetime
import uuid
import json
import os


try:
    from pyspark.context import SparkContext
except ImportError:
    pass

SINGLE_CORE = "single-core"
MULTI_CORE = "multi-core"
SPARK = "spark"
HADOOP = "hadoop"

class MapReduce():
    """ Abstract class for Map Reduce job
    def map(self, text):
        [...]
        return (key,value) #or return [(key,value),...]

    def combine(self, key, values):
        [...]
        return (key,value) #or return [(key,value),...]
    def reduce(self, key, values):
        [...]
        return (key,value) #or return [(key,value),...]
    """
    
    data = None
    data_reduced = None
    params = {}
    streamming = False
    
    def __init__(self):
        self.data = {}
        self.data_reduced = {}    
        self.check_usage()
    
    def post_reduce(self):
        return self.data_reduced.iteritems()
    
    def run_map(self, input_reader):
        
        def collect_line(kv):
            if isinstance(kv,list):
                map(lambda item : self.collect(item[0],item[1]), kv)
            else:
                key,value = kv
                self.collect(key, value)
            
                  
        map(lambda line : collect_line(self.map(line)),input_reader.read())
        
        input_reader.close()
        
    
    def run_combine(self, data):
        
        def compact_line(kv):
            key,value = kv
            self.compact(key, value)
            
        map(lambda kv : compact_line(self.combine(kv[0],kv[1])),data)
        
       
    
    def run_reduce(self, data):
       
        def reduce_line(kv):
            key,values = kv
            self.emit(key,values)

        map(lambda kv : reduce_line(self.reduce(kv[0],kv[1])),data)
           
            
    def reset(self):
        self.data = {}
        self.data_reduced = {}
        
    def check_usage(self):
        if "map" not in dir(self):
            raise Exception("ERROR: You have to implement a map() method")
        
    def run(self,input_reader,output_writer,engine=None,debug=False,options={}):
        cpu = cpu_count()
        
        self.reset()
        
        if engine == None:
            engine, diags = self.profile(input_reader)
            
        if engine == HADOOP or input_reader.is_distant():
            engine = HadoopEngine(self)
            engine.run(input_reader, output_writer)
        elif engine == SPARK:
            if "spark-context" not in options:
                sc = SparkContext('local','polymr job')
            else:
                sc = options['spark-context']
            engine = SparkEngine(self,sc)
            engine.run(input_reader, output_writer)
        elif cpu == 1 or debug or engine == SINGLE_CORE:
            engine = SingleCoreEngine(self)
            engine.run(input_reader, output_writer)
        elif engine == MULTI_CORE:
            engine = MultiCoreEngine(self)
            engine.run(input_reader, output_writer,cpu-1)
        
            
    
    def collect(self, key, value):
        if self.streamming:
            print "%s;%s" % (key,value)
        else:
            if key == None:
                key = "Undefined"
            
            try:
                self.data[key].append(value)
            except KeyError:
                self.data[key] = [value]
    
            
    def compact(self, key, value):
        if self.streamming:
            print "%s;%s" % (key,value)
        else:
            self.data[key] = [value]

    def emit(self, key, value):
        if self.streamming:
            print "%s;%s" % (key,value)
        else:
            try:
                self.data_reduced[key].append(value)
            except KeyError:
                self.data_reduced[key] = [value]
           

    def profile(self, input_reader,max_memory=1000,core=cpu_count()-1,hadoop_nodes=4):
        """
        Profile the MapReduce job against the input reader and return recommandation + diagnostics
        @param max_memory: SMP memory limit availaible for the job in Mb (default : 1Gb)
        
        @return: recommanded engine name, diagnostic data
        
        """

        
        diagnostics = {}
        
        if input_reader.is_distant():
            return HADOOP, diagnostics
        
        
        total_size,sample_input = input_reader.get_estimated_size()
        sample_size = len(sample_input.data)
        map_delay = 0.0
        
        
        self.reset()
        
        for line in sample_input.read():
            start = time.time()
            self.map(line)
            map_delay += time.time() - start
            
        sample_input.close()
        
        mean_map_delay = map_delay / sample_size
        
        map_data_mem = mem.asizeof(self.data)/1000000.0 * total_size / sample_size
        
        diagnostics['estimated-input-size'] = total_size
        diagnostics['mean-map-delay'] = mean_map_delay
        diagnostics['estimated-mem-size'] = map_data_mem
        
        
        if map_data_mem >= max_memory:
            engine= HADOOP 
            diagnostics['estimated-delay'] = total_size * mean_map_delay / hadoop_nodes
        else:
            
            if mean_map_delay >= 1.0e-4:
                
                if map_data_mem * core >= max_memory:
                    engine = HADOOP 
                    diagnostics['estimated-delay'] = total_size * mean_map_delay / hadoop_nodes
                else:
                    engine = MULTI_CORE
                    diagnostics['estimated-delay'] = total_size * mean_map_delay / (core if core > 0 else 1)
            else:
                engine = SINGLE_CORE 
                diagnostics['estimated-delay'] = total_size * mean_map_delay
        
        return engine,diagnostics
        




class InputReader():
    def __init__(self):
        pass
    
    def read(self,input_file):
        pass
    
    def close(self):
        pass
    
    def is_distant(self):
        return False
    
    def to_file(self):
        pass
    
    
    def get_estimated_size(self):
        pass
    
    def count(self,engine=None,debug=False,options={}):
        out = MemOutputWriter()
        Count().run(self,out,engine,debug,options)
        return out.data[0][1][0]
        

class MemInputReader(InputReader):
    data = None
    
    def __init__(self,data=[]):
        self.data = data
    
    
    @staticmethod 
    def load_from_file(filename):
        start_time = datetime.datetime.now()
        itself = MemInputReader()
        f = open(filename)
        
        for line in f:
            itself.data.append(line)
        f.close()
        print "Load file %s in %s" % (filename, datetime.datetime.now() - start_time)
        return itself
    
    def to_file(self):
        filename = '/var/tmp/%s' % str(uuid.uuid1()) 
        f = open(filename,mode='w')
        map(lambda item : f.write(json.dumps(item)),self.data)
        f.close()
        return filename
    
    def read(self):
        return iter(self.data)
    
    def close(self):
        pass
    
    def get_estimated_size(self):
        return len(self.data), MemInputReader(self.data[:999]) if len(self.data)>1000 else self

class HdfsInputReader(InputReader):
    filename = None
    
    def __init__(self,filename):
        self.filename = filename
    
    def is_distant(self):
        return True
    
    
    
class FileInputReader(InputReader):
    file = None
    filename = None
    
    def __init__(self,filename):
        self.filename = filename
        
    def to_file(self):
        return self.filename
    
    def read(self):
        self.file = open(self.filename)
        return self.file
    
    def close(self):
        if self.file != None:
            self.file.close()
    
    def get_estimated_size(self):
        sample = []
        sample_size = 1000
        file_size = os.stat(self.filename).st_size
        f = open(self.filename)
        count = 0
        line_size = 0
        for line in f:
            count +=1
            line_size += len(line)
            sample.append(line)
            if count >= sample_size:
                break
        
        f.close()
        
        return int(sample_size * float(file_size) / float(line_size)), MemInputReader(sample)

class OutputWriter():
    
    def __init__(self):
        pass
    
    def is_distant(self):
        return False
    
    def is_memory(self):
        return False
    
    def write(self,data):
        pass
    
    def dumps_output_value(self,key,value):
        if len(value) == 1:
            value = value[0]
        if isinstance(value, str):
            return "%s=%s\n" % (str(key),value)
        else:
            return  "%s=%s\n" % (str(key), json.dumps(value))

class HdfsOutputWriter(OutputWriter):
    filename = None
    
    def __init__(self,filename):
        self.filename = filename
        
    def is_distant(self):
        return True

class FileOutputWriter(OutputWriter):
    filename = None
    mode = None
    
    def __init__(self,filename,mode='w'):
        self.filename = filename
        self.mode = mode
        
    def write(self,data):
        f = open(self.filename, self.mode)
        for key,value in data:
            f.write(self.dumps_output_value(key,value))
        f.close()
        
   
class StdIOOutputWriter(OutputWriter): 
    
    def is_memory(self):
        return True
    
    def write(self,data):
        for key,value in data:
            print self.dumps_output_value(key,value)

class MemOutputWriter(OutputWriter):
    data = None
    
    def is_memory(self):
        return True
    
    def write(self,data):
        self.data = []
        for key,value in data:
            self.data.append((key,value))
        
    def get_reader(self):
        return MemInputReader(self.data)


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

    
    
