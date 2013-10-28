from multiprocessing import cpu_count
from polymr.engine.hadoop import HadoopEngine
from polymr.engine.smp import SingleCoreEngine, MultiCoreEngine
import time
from polymr import mem
import types
from polymr.inout.mem import MemInput

SINGLE_CORE = "single-core"
MULTI_CORE = "multi-core"
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
    
    def __init__(self):
        self.data = {}
        self.data_reduced = {}    
        self.check_usage()
    
    def post_reduce(self):
        return self.data_reduced.iteritems()
    
    def run_map_on_data(self,data):
        input_data = MemInput(data)     
        self.run_map(input_data)
    
    def run_map(self, input_reader):
        
        def collect_line(kv):
            if isinstance(kv,list) or isinstance(kv,types.GeneratorType):
                map(lambda item : self.collect(item[0],item[1]), kv)
            else:
                key,value = kv
                self.collect(key, value)
            
        if 'map' in dir(self):                              
            map(lambda line : collect_line(self.map(line)),input_reader.read())
        elif 'map_partition' in dir(self):
            collect_line(self.map_partition(input_reader.read()))
        else:
            raise Exception("ERROR: You have to implement a map() ou map_partition() method")
        
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
        if "map" not in dir(self) and "map_partition" not in dir(self):
            raise Exception("ERROR: You have to implement a map() or map_partition() method")
        
    def run(self,input_reader,output_writer,engine=None,debug=False,options={}):
        cpu = cpu_count()
        
        self.reset()
        
        if engine == None:
            engine, diags = self.profile(input_reader)
            
        if engine == HADOOP or input_reader.is_distant():
            engine = HadoopEngine(self)
            engine.run(input_reader, output_writer)
        elif cpu == 1 or debug or engine == SINGLE_CORE:
            engine = SingleCoreEngine(self)
            engine.run(input_reader, output_writer)
        elif engine == MULTI_CORE:
            engine = MultiCoreEngine(self)
            engine.run(input_reader, output_writer,cpu-1)
        
            
    
    def collect(self, key, value):
        if key == None:
            key = "Undefined"
        
        try:
            self.data[key].append(value)
        except KeyError:
            self.data[key] = [value]

            
    def compact(self, key, value):
        self.data[key] = [value]

    def emit(self, key, value):
        
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
        
        if 'map' in dir(self):
            for line in sample_input.read():
                start = time.time()
                self.map(line)
                map_delay += time.time() - start
        elif 'map_partition' in dir(self):
            start = time.time()
            self.map_partition(sample_input.read())
            map_delay += time.time() - start
        else:
            raise Exception("ERROR: You have to implement a map() or map_partition() method")

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
        
    
    


