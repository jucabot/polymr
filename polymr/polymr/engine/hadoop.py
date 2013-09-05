
from polymr.inout import FileInputReader
import datetime
import subprocess
import uuid
import json
from polymr import mem
import inspect
from os import getenv
  
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
        if getenv('HADOOP_HOME') is None:
            print "ERROR : $HADOOP_HOME have to be set to Hadoop home directory"
            return
        if getenv('POLYMR_HOME') is None:
            print "ERROR : $POLYMR_HOME have to be set to polymr home directory"
            return
        
        #store params to broadcast to hadoop
        params_file_id = str(uuid.uuid1())
        cache_filename = '/var/tmp/%s' % params_file_id 
        f = open(cache_filename,mode='w')
        f.write(json.dumps(self._mapred.params))
        f.close()
        
        output_id = "output-%s" % str(uuid.uuid1())
        
        #dummy hadoop simulation as command pipes
        cmds = "$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar -archives $POLYMR_HOME/polymr.zip#polymr -files $POLYMR_HOME/streamer.py,%s,%s -input %s -output %s -mapper 'streamer.py mapper %s %s %s'" % (self._source_file, cache_filename, input_reader.filename, output_id, self._module_name,self._class_name,params_file_id)
        if "combine" in dir(self._mapred):
            cmds += " -combiner 'streamer.py combiner %s %s %s'" % (self._module_name,self._class_name,params_file_id)

        if "reduce" in dir(self._mapred):
            cmds += " -reducer 'streamer.py reducer %s %s %s'" % (self._module_name,self._class_name,params_file_id)
        
        
        print "INFO: %s" % cmds
        subprocess.check_output(cmds,shell=True)
        
        print "INFO: end job %s in %s with mem size of %d"  % (self._mapred.__class__.__name__, (datetime.datetime.now()-start_time),mem.asizeof(self))
