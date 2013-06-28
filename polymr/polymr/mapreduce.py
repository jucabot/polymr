from multiprocessing import cpu_count
import cjson
from polymr.engine.hadoop import LocalHadoopEngine
from polymr.engine.smp import SingleCoreEngine, MultiCoreEngine
from polymr.inout import MemInputReader
import time
from polymr import mem

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
        
        self.reset()
        
        if engine == LOCAL_HADOOP:
            engine = LocalHadoopEngine(self)
            engine.run(input_reader, output_writer)
        elif cpu == 1 or debug or engine == SINGLE_CORE:
            engine = SingleCoreEngine(self)
            engine.run(input_reader, output_writer)
        else:
            engine = MultiCoreEngine(self)
            engine.run(input_reader, output_writer,cpu-1)
        
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
           

    def profile(self, input_reader,max_memory=1000,core=cpu_count()-1,hadoop_nodes=4):
        """
        Profile the MapReduce job against the input reader and return recommandation + diagnostics
        @param max_memory: SMP memory limit availaible for the job in Mb (default : 1Gb)
        
        @return: recommanded engine name, diagnostic data
        
        """
        diagnostics = {}
        total_size,sample = input_reader.get_estimated_size()
        sample_size = len(sample)
        map_delay = 0.0
        is_mem_input = isinstance(input_reader,MemInputReader)
        
        sample_input = MemInputReader(data=sample)
        
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
        diagnostics['is-mem-input'] = is_mem_input
       
        
        if map_data_mem >= max_memory:
            engine= LOCAL_HADOOP #or Hadoop
            diagnostics['estimated-delay'] = total_size * mean_map_delay / hadoop_nodes
        else:
            
            if mean_map_delay >= 1.0e-4:
                
                if map_data_mem * core >= max_memory:
                    engine = LOCAL_HADOOP #or Hadoop
                    diagnostics['estimated-delay'] = total_size * mean_map_delay / hadoop_nodes
                else:
                    engine = MULTI_CORE
                    diagnostics['estimated-delay'] = total_size * mean_map_delay / core
            else:
                engine = SINGLE_CORE 
                diagnostics['estimated-delay'] = total_size * mean_map_delay
        
        return engine,diagnostics
        
        

