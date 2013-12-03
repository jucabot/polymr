
import datetime
import subprocess
import uuid
import json
from polymr import mem
import inspect
from os import getenv
import httplib
import urllib
from urlparse import urlparse

  
def format_function(iterator):
    for row in iterator:
        yield row


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
        #hadoop = HadoopClient()
        hdfs_web_url = 'http://sandbox:50070/?user.name=predictiveds' #getenv('HDFS_WEB_URL')
        (scheme,hostport, path,params,query,fragment) = urlparse(hdfs_web_url)
        host,port = hostport.split(':',2)
        hadoop = WebHdfsClient(host,port,query)
        
        if getenv('POLYMR_HOME') is None:
            print "ERROR : $POLYMR_HOME have to be set to polymr home directory"
            raise SystemError("$POLYMR_HOME have to be set to polymr home directory")
        
        
        #Set metadata
        format_class = input_reader.formatter.__class__
        input_source_file = inspect.getfile(format_function)
        input_module_name = format_class.__module__
        input_class_name = format_class.__name__
        
        input_format_source = "format = %s" % inspect.getsource(format_function)
        
        self._mapred.params['_input_meta'] = { 
                'input_class_name': input_class_name,
                'input_module_name' : input_module_name,
                'input_source' : input_source_file,
                'input_options' : input_reader.formatter.options
                } 
        
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
        cmds = "$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/contrib/streaming/hadoop-*streaming*.jar -archives $POLYMR_HOME/polymr.zip#polymr -files $POLYMR_HOME/streamer.py,%s,%s,%s -input %s -output %s -mapper 'streamer.py mapper %s %s %s'" % (self._source_file, cache_filename, input_source_file, hdfs_input, output_id, self._module_name,self._class_name,params_file_id)
        if "combine" in dir(self._mapred):
            cmds += " -combiner 'streamer.py combiner %s %s %s'" % (self._module_name,self._class_name,params_file_id)

        if "reduce" in dir(self._mapred):
            cmds += " -reducer 'streamer.py reducer %s %s %s'" % (self._module_name,self._class_name,params_file_id)
        
        
        print "INFO: %s" % cmds
        subprocess.check_output(cmds,shell=True)
        
        # get result
        def load_line(line):
            key, value = line.split(";",2)
            self._mapred.data_reduced[key] = [json.loads(value)]
        
        
        if output_writer.is_distant():
            pass # nothing to do
        elif output_writer.is_memory():
            output = hadoop.cat("%s"% output_id)
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
        
class WebHdfsClient(object):
     
    host = None
    port = None
    username = None
    home = None
    
    def __init__(self,host,port,username=''):
        self.host = host
        self.port = port
        self.username = username
        
        self.home = self.get_home()

    def get_home(self):
        c = httplib.HTTPConnection(self.host,self.port)
        c.request('GET','/webhdfs/v1/?%s&op=GETHOMEDIRECTORY' % (self.username))
        r = c.getresponse()
        
        response = r.read()
        assert r.status == 200, response

        c.close()
        return json.loads(response)['Path']

    
    def put_file(self,local_path, hdfs_path,overwrite=True):
        c = httplib.HTTPConnection(self.host,self.port)
        c.request('PUT','/webhdfs/v1%s/%s?%s&op=CREATE&overwrite=%s' % (self.home, hdfs_path, self.username, 'true' if overwrite else 'false'))
        r = c.getresponse()
        
        assert r.status == 307, r.reason
        
        redirect = r.getheader('Location')
        
        c.close()
        
        dn_host, dn_port = urlparse(redirect)[1].split(':',2)
        
        c = httplib.HTTPConnection(dn_host,dn_port)
        c.request('PUT',redirect, body=open(local_path,mode='r'))
        r = c.getresponse()
        
        response = r.read()
        c.close()
        
        assert r.status == 201, response

        
    def get_file(self, hdfs_path,local_path):
        out = open(local_path,mode='w')
        self._get_file(hdfs_path, out)
        out.close()
    
    def _get_file(self,hdfs_path, out):
        c = httplib.HTTPConnection(self.host,self.port)
        c.request('GET','/webhdfs/v1%s/%s?op=GETFILESTATUS&%s' % (self.home, hdfs_path, self.username))
        r = c.getresponse()
        
        assert r.status == 200, r.read()
        
        status = json.loads(r.read())
        c.close()
        
        file_type = status['FileStatus']['type']
        
        if file_type == 'FILE':
            self._get(hdfs_path,out)
          
        elif file_type == 'DIRECTORY':
            
            c = httplib.HTTPConnection(self.host,self.port)
            c.request('GET','/webhdfs/v1%s/%s?op=LISTSTATUS&%s' % (self.home, hdfs_path, self.username))
            r = c.getresponse()
            
            assert r.status == 200, r.read()
            
            status = json.loads(r.read())['FileStatuses']['FileStatus']
            c.close()
            
            files = filter(lambda file : file['type'] == 'FILE',status)
            
            for file in files:
                if hdfs_path == "":
                    self._get(file['pathSuffix'],out)
                else:
                    self._get(hdfs_path + '/' + file['pathSuffix'],out)
            
            
    def _get(self,path, out):
        c = httplib.HTTPConnection(self.host,self.port)
        c.request('GET','/webhdfs/v1%s/%s?op=OPEN&%s' % (self.home, path, self.username))
        r = c.getresponse()
        
        assert r.status == 307, r.read()
        
        redirect = r.getheader('Location')
        
        c.close()
        
        dn_host, dn_port = urlparse(redirect)[1].split(':',2)
        
        c = httplib.HTTPConnection(dn_host,dn_port)
        c.request('GET',redirect)
        r = c.getresponse()
        assert r.status == 200, r.read()
        
        out.write(r.read())
        c.close()
        
        

    def cat(self, hdfs_path):
        
        class MemWriter(object):
            data = None
            
            def __init__(self):
                self.data = []
            
            def write(self,value):
                self.data.append(value)
                
            def flush(self):
                return '\n'.join(self.data)
            
        
        out = MemWriter()
        self._get_file(hdfs_path, out)
        return out.flush()
    
        
    
    def rm(self, hdfs_path,recursive=True):
        
        c = httplib.HTTPConnection(self.host,self.port)
        c.request('DELETE','/webhdfs/v1%s/%s?op=DELETE&recursive=%s&%s' % (self.home, hdfs_path, 'true' if recursive else 'false',self.username))
        r = c.getresponse()        
        response = r.read()
        c.close()
        
        assert r.status == 200, response
                


