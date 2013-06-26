
from polymr.inout import FileInputReader, FileOutputWriter
import datetime
import subprocess
import uuid
from polymr.file import parse_filenames, path
import polymr.mem
import json
from polymr import settings, mem
import sys
import inspect
  
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
        
        assert isinstance(input_reader, FileInputReader), "input reader has to be FileInputReader"
               
        
        print "start job %s on local hadoop" % (self._mapred.__class__.__name__)
        
        #store params to broadcast to hadoop
        cache_filename = path(settings.HADOOP["dcache-directory"]) + "/" + str(uuid.uuid1())
        f = open(cache_filename,mode='w')
        json.dump(self._mapred.params,f)
        f.close()
        
        #dummy hadoop simulation as command pipes
        cmds = "cat %s | python -m polymr.hadoop.mapper %s %s %s" % (path(input_reader.filename),self._module_name,self._class_name,cache_filename)
        if "combine" in dir(self._mapred):
            cmds += "| python -m polymr.hadoop.combiner %s %s %s" % (self._module_name,self._class_name,cache_filename)

        cmds += "|sort"

        if "reduce" in dir(self._mapred):
            cmds += "| python -m polymr.hadoop.reducer %s %s %s" % (self._module_name,self._class_name,cache_filename)
        
        #cmds += "> %s" % path(self._mapred.output_file)

        print cmds
        output =  subprocess.check_output(cmds,shell=True)
        
        for line in output.strip().split("\n"):
            key, value = line.split(";")
            self._mapred.data_reduced[key] = [json.loads(value)]
        
        output_writer.write(self._mapred.post_reduce())
        
        print "end job %s in %s with mem size of %d"  % (self._mapred.__class__.__name__, (datetime.datetime.now()-start_time),mem.asizeof(self))
    