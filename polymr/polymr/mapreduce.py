import datetime
from multiprocessing import cpu_count
import json
from file import parse_filenames
from polymr.inout import FileInputReader, KeyValueOutputWriter
from polymr.cluster.master import MasterClient
from polymr.engine.cluster import MasterWorkerEngine
from polymr.engine.hadoop import LocalHadoopEngine
from polymr.engine.smp import SingleCoreEngine, MultiCoreEngine

SINGLE_CORE = "single-core"
MULTI_CORE = "multi-core"
LOCAL_HADOOP = "local-hadoop"
CLUSTER = "cluster"


class MapReduce():
    """ Abstract class for Map Reduce job
    def map(self, text):
        [...]
        self.collect(key,value)

    def reduce(self, key, values):
        [...]
        self.emit(key,values)
    """
    
    verbose = True
    input_file = None
    output_file = None
    data = None
    data_reduced = None
    flush_limit = 100000
    params = {}
    streamming = False
    
    def __init__(self, input_file=None, output_file=None):
        self.data = {}
        self.data_reduced = {}
       
        self.input_file = input_file
        self.output_file = output_file
        
        self.check_usage()
    
    def post_reduce(self):
        return self.data_reduced.iteritems()
    
    def run_map(self, input_file, input_reader):
        index = 0
       
        start_time = datetime.datetime.now()
        
        input_names = parse_filenames(input_file)
        for input_name in input_names:
            
            f = input_reader.read(input_name)
            for line in f:
                self.map(line)
                if self.verbose and index % self.flush_limit == 0: 
                    print "map #%d in %s" % (index, datetime.datetime.now()-start_time)
                index += 1 
            input_reader.close()
            
        return index
    
    def run_combine(self, data):
        index=0
        for (key, values) in data:
            start_time = datetime.datetime.now()
            self.combine(key,values)
            if self.verbose and index % self.flush_limit == 0: 
                print "combine #%d in %s" % (index, datetime.datetime.now()-start_time)
            index += 1
        return index
       
    
    def run_reduce(self, data):
        index=0
        for (key, values) in data:
            start_time = datetime.datetime.now()
            self.reduce(key,values)
            if self.verbose and index % self.flush_limit == 0: 
                print "reduce #%d in %s" % (index, datetime.datetime.now()-start_time)
            index += 1
        return index
       
    def reset(self):
        self.data = {}
        self.data_reduced = {}
        
    def check_usage(self):
       
        if "map" not in dir(self):
            raise Exception("You have to implement a map() method")
        
    def run(self,engine=None, debug=False,input_reader=FileInputReader(),output_writer=KeyValueOutputWriter()):
        cpu = cpu_count()
        
        
        #check if one master instance is running
        
        master = MasterClient()
        cluster_ready = master.check()
        master.close()
        
            
        if engine == CLUSTER:
            assert(cluster_ready)
            engine = MasterWorkerEngine(self)
            engine.run(input_reader, output_writer)
        elif engine == LOCAL_HADOOP:
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
            print "%s;%s" % (str(key),json.dumps(value))
        else:
            if key == None:
                key = "Undefined"
                
            if key in self.data:
                self.data[key].append(value)
            else:
                self.data[key] = [value]
                
    def compact(self, key, value):
        if self.streamming:
            print "%s;%s" % (str(key),json.dumps(value))
        else:
            self.data[key] = [value]
    """
    def get(self,key):
        return self.data[key]
    
    def set (self,key, values):
        self.data[key] = values
    """    
    def emit(self, key, value):
        if self.streamming:
            print "%s;%s" % (str(key),json.dumps(value))
        else:
            if key in self.data_reduced:
                self.data_reduced[key].append(value)
            else:
                self.data_reduced[key] = [value]
                

    

