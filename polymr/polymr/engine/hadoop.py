
from polymr.inout import FileInputReader
import datetime
import subprocess
import uuid
import cjson
from polymr import mem
import inspect
from os import getenv
  
class LocalHadoopEngine():
    _mapred = None
    _module_name = None
    _class_name = None
    _source_file = None
    
    def __init__(self,mapred):
        self._mapred = mapred
        self._source_file = inspect.getfile(self._mapred.__class__)
        self._module_name = self._mapred.__class__.__module__
        if self._module_name == '__main__':
            self._module_name = inspect.getmodulename(self._source_file)
       
        self._class_name = self._mapred.__class__.__name__
    
    def run(self,input_reader,output_writer):
        
        start_time = datetime.datetime.now()
        
        assert isinstance(input_reader, FileInputReader), "ERROR: input reader has to be FileInputReader"
               
        
        print "INFO: start job %s on local hadoop" % (self._mapred.__class__.__name__)
        
        #store params to broadcast to hadoop
        cache_filename = '/var/tmp/' + str(uuid.uuid1())
        f = open(cache_filename,mode='w')
        f.write(cjson.encode(self._mapred.params))
        f.close()
        
        #dummy hadoop simulation as command pipes
        cmds = "cat %s | polymr/hadoop/streamer.py mapper %s %s %s" % (input_reader.filename,self._module_name,self._class_name,cache_filename)
        if "combine" in dir(self._mapred):
            cmds += "| polymr/hadoop/streamer.py combiner %s %s %s" % ( self._module_name,self._class_name,cache_filename)

        cmds += "|sort"

        if "reduce" in dir(self._mapred):
            cmds += "| polymr/hadoop/streamer.py reducer %s %s %s" % (self._module_name,self._class_name,cache_filename)
        
        

        print "INFO: %s" % cmds
        output =  subprocess.check_output(cmds,shell=True)
        
        def load_line(line):
            key, value = line.split(";")
            self._mapred.data_reduced[key] = [cjson.decode(value)]
            
        map(load_line,output.strip().split("\n"))
            
        
        output_writer.write(self._mapred.post_reduce())
        
        print "INFO: end job %s in %s with mem size of %d"  % (self._mapred.__class__.__name__, (datetime.datetime.now()-start_time),mem.asizeof(self))

class HadoopEngine():
    _mapred = None
    _module_name = None
    _class_name = None
    _source_file = None
    
    def __init__(self,mapred):
        self._mapred = mapred
        self._source_file = inspect.getfile(self._mapred.__class__)
        self._module_name = self._mapred.__class__.__module__
        if self._module_name == '__main__':
            self._module_name = inspect.getmodulename(self._source_file)
       
        self._class_name = self._mapred.__class__.__name__
    
    def run(self,input_reader,output_writer):
        
        start_time = datetime.datetime.now()
        
        assert isinstance(input_reader, FileInputReader), "ERROR: input reader has to be FileInputReader"
               
        print "INFO: start job %s on hadoop" % (self._mapred.__class__.__name__)
        
        #hadoop_home
        hadoop_home = getenv('HADOOP_HOME')
        polymr_home = getenv('POLYMR_HOME')
        
        #store params to broadcast to hadoop
        params_file_id = str(uuid.uuid1())
        cache_filename = '/var/tmp/%s' % params_file_id 
        f = open(cache_filename,mode='w')
        f.write(cjson.encode(self._mapred.params))
        f.close()
        
        #push params
        
        #dummy hadoop simulation as command pipes
        cmds = "cat %s | python -m polymr.hadoop.mapper %s %s %s" % (input_reader.filename,self._module_name,self._class_name,cache_filename)
        if "combine" in dir(self._mapred):
            cmds += "| python -m polymr.hadoop.combiner %s %s %s" % (self._module_name,self._class_name,cache_filename)

        cmds += "|sort"

        if "reduce" in dir(self._mapred):
            cmds += "| python -m polymr.hadoop.reducer %s %s %s" % (self._module_name,self._class_name,cache_filename)
        
        

        print "INFO: %s" % cmds
        output =  subprocess.check_output(cmds,shell=True)
        
        def load_line(line):
            key, value = line.split(";")
            self._mapred.data_reduced[key] = [cjson.decode(value)]
            
        map(load_line,output.strip().split("\n"))
            
        
        output_writer.write(self._mapred.post_reduce())
        
        print "INFO: end job %s in %s with mem size of %d"  % (self._mapred.__class__.__name__, (datetime.datetime.now()-start_time),mem.asizeof(self))
      