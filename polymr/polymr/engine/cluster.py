
import polymr.settings
from polymr.inout import FileInputReader, KeyValueOutputWriter
import datetime
import json
from polymr import Command, settings
import polymr.mem
import zmq



class MasterWorkerEngine():
    
    _mapred = None
    cache_line=10000
    _job_id = None
    _context = None
    _master_queue_name = None
    _master_queue = None    
    _result_queue_name = None
    _result_queue = None    
    
    def __init__(self,mapred):
        self._mapred = mapred
        
    
    def _run_map(self,input_reader ):
        
        command = "send-data-map"
        args = {
            'job_id' : self._job_id,
            'data' : self._mapred.input_file ,
            'cache-line' : self.cache_line,
            }
        self._master_queue.send("%s|%s" % (command,json.dumps(args)))
        return self._master_queue.recv()
    
    def _send_master_cmd(self, command, args={}):
        self._master_queue.send("%s|%s" % (command,json.dumps(args)))
        return json.loads(self._master_queue.recv())
    
    def run(self,input_reader=FileInputReader(),output_writer=KeyValueOutputWriter()):
        
        start_time = datetime.datetime.now()
        
        self._context = zmq.Context()
        
        self._master_queue_name = settings.CLUSTER_MASTER #"tcp://*:5550"
        self._master_queue = self._context.socket(zmq.REQ)
        self._master_queue.connect(self._master_queue_name)
    
        self._result_queue_name = settings.CLUSTER_RESULT #"tcp://*:5551"
        self._result_queue = self._context.socket(zmq.SUB)
        self._result_queue.connect(self._result_queue_name)
    
        
        if 'reduce' in dir(self._mapred):
            use_reduce = True
        else:
            use_reduce = False
            
        
        command = "start"
        args = {
                'module_name' : self._mapred.__class__.__module__,
                'class_name' : self._mapred.__class__.__name__ ,
                'params' : self._mapred.params,
                'use-reduce' : use_reduce,
                }
        self._job_id  = self._send_master_cmd(command,args)
        self._result_queue.setsockopt(zmq.SUBSCRIBE, str(self._job_id))
        
        print "start job %s on %s on cluster" % (self._job_id, self._mapred.__class__.__name__)
       
        
        self._run_map(input_reader)
        
        print "Waiting for result..."
        res_cmd = Command(self._result_queue)
        job_id, data = res_cmd.get_command()
        self._mapred.data_reduced = data
        print "Post processing and writing the result..."
        output_writer.write(self._mapred.post_reduce(),self._mapred.output_file)
        
        print "end job %s in %s with mem size of %d"  % (self._mapred.__class__.__name__, (datetime.datetime.now()-start_time),mem.asizeof(self))
        
        self._master_queue.setsockopt(zmq.LINGER, 1)
        self._master_queue.close()
        
        self._result_queue.setsockopt(zmq.LINGER, 1)
        self._result_queue.close()
        
        self._context.destroy()
        
      
        
        