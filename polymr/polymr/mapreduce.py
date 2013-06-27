from multiprocessing import cpu_count
import cjson
from polymr.engine.hadoop import LocalHadoopEngine
from polymr.engine.smp import SingleCoreEngine, MultiCoreEngine

SINGLE_CORE = "single-core"
MULTI_CORE = "multi-core"
LOCAL_HADOOP = "local-hadoop"

class MapReduce():
    """ Abstract class for Map Reduce job
    def map(self, text):
        [...]
        self.collect(key,value)

    def combine(self, key, values):
        [...]
        self.compact(key,values)
    def reduce(self, key, values):
        [...]
        self.emit(key,values)
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
        map(self.map,input_reader.read())
        input_reader.close()
        
    
    def run_combine(self, data):
        
        def combine_line(kv):
            key,values = kv
            self.combine(key,values)

        map(combine_line,data)
        
    
    def run_reduce(self, data):
       
        def reduce_line(kv):
            key,values = kv
            self.reduce(key,values)

        map(reduce_line,data)
           
            
    def reset(self):
        self.data = {}
        self.data_reduced = {}
        
    def check_usage(self):
        if "map" not in dir(self):
            raise Exception("ERROR: You have to implement a map() method")
        
    def run(self,input_reader,output_writer,engine=None,debug=False):
        cpu = cpu_count()
        
        if engine == LOCAL_HADOOP:
            engine = LocalHadoopEngine(self)
            engine.run(input_reader, output_writer)
        elif cpu == 1 or debug or engine == SINGLE_CORE:
            engine = SingleCoreEngine(self)
            engine.run(input_reader, output_writer)
        else:
            engine = MultiCoreEngine(self)
            engine.run(input_reader, output_writer,cpu)
        
    def collect(self, key, value):
        if self.streamming:
            print "%s;%s" % (str(key),cjson.encode(value))
        else:
            if key == None:
                key = "Undefined"
            
            try:
                self.data[key].append(value)
            except KeyError:
                self.data[key] = [value]
                
    def compact(self, key, value):
        if self.streamming:
            print "%s;%s" % (str(key),cjson.encode(value))
        else:
            self.data[key] = [value]
   
    def emit(self, key, value):
        if self.streamming:
            print "%s;%s" % (str(key),cjson.encode(value))
        else:
            try:
                self.data_reduced[key].append(value)
            except KeyError:
                self.data_reduced[key] = [value]
           

    

