
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
        
               
        print "INFO: start job %s on hadoop" % (self._mapred.__class__.__name__)
        
        #hadoop_home
        hadoop = HadoopClient()
        
        if getenv('POLYMR_HOME') is None:
            print "ERROR : $POLYMR_HOME have to be set to polymr home directory"
            raise SystemError("$POLYMR_HOME have to be set to polymr home directory")
        
        #store params to broadcast to hadoop
        params_file_id = str(uuid.uuid1())
        cache_filename = '/var/tmp/%s' % params_file_id 
        f = open(cache_filename,mode='w')
        f.write(json.dumps(self._mapred.params))
        f.close()
        
        #Manage the input types
        if input_reader.is_distant():
            hdfs_input = input_reader.filename        
        else :
            hdfs_input = ".tmp/input-%s" % str(uuid.uuid1())
            hadoop.put_file(input_reader.to_file(), hdfs_input)
            
        
        if output_writer.is_distant():
            output_id = output_writer.fileName
        else:
            output_id = ".tmp/output-%s" % str(uuid.uuid1())
        
        #dummy hadoop simulation as command pipes
        cmds = "$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar -archives $POLYMR_HOME/polymr.zip#polymr -files $POLYMR_HOME/streamer.py,%s,%s -input %s -output %s -mapper 'streamer.py mapper %s %s %s'" % (self._source_file, cache_filename, hdfs_input, output_id, self._module_name,self._class_name,params_file_id)
        if "combine" in dir(self._mapred):
            cmds += " -combiner 'streamer.py combiner %s %s %s'" % (self._module_name,self._class_name,params_file_id)

        if "reduce" in dir(self._mapred):
            cmds += " -reducer 'streamer.py reducer %s %s %s'" % (self._module_name,self._class_name,params_file_id)
        
        
        print "INFO: %s" % cmds
        subprocess.check_output(cmds,shell=True)
        
        # get result
        def load_line(line):
            key, value = line.split(";")
            self._mapred.data_reduced[key] = [json.loads(value)]
        
        
        if output_writer.is_distant():
            pass # nothing to do
        elif output_writer.is_memory():
            output = hadoop.cat("%s/*"% output_id)
            map(load_line,output.strip().split("\n"))
            output_writer.write(self._mapred.post_reduce())
        else :
            hadoop.get_file(output_id, output_writer.filename)

            
        #Clean up
        if not input_reader.is_distant() :
            hadoop.rm(hdfs_input)
        
        if not output_writer.is_distant() :
            hadoop.rm(output_id)
         
        
        print "INFO: end job %s in %s with mem size of %d"  % (self._mapred.__class__.__name__, (datetime.datetime.now()-start_time),mem.asizeof(self))


class HadoopClient():
    
    def __init__(self):
        if getenv('HADOOP_HOME') is None:
            print "ERROR : $HADOOP_HOME have to be set to Hadoop home directory"
            raise SystemError("$HADOOP_HOME have to be set to Hadoop home directory")
    
    def put_file(self,local_path, hdfs_path):
        
        cmds = "$HADOOP_HOME/bin/hadoop fs -put %s %s" % (local_path,hdfs_path)
        print "INFO: %s" % cmds
        subprocess.check_output(cmds,shell=True)
    
    def get_file(self, hdfs_path,local_path):
        
        cmds = "$HADOOP_HOME/bin/hadoop fs -get %s %s" % (hdfs_path,local_path)
        print "INFO: %s" % cmds
        subprocess.check_output(cmds,shell=True)
    
    def cat(self, hdfs_path):
        
        cmds = "$HADOOP_HOME/bin/hadoop fs -cat %s" % (hdfs_path)
        print "INFO: %s" % cmds
        output = subprocess.check_output(cmds,shell=True)
        return output
    
    def rm(self, hdfs_path):
        
        cmds = "$HADOOP_HOME/bin/hadoop fs -rmr %s" % (hdfs_path)
        print "INFO: %s" % cmds
        subprocess.check_output(cmds,shell=True)
        