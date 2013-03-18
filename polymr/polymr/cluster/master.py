'''
Created on 26 oct. 2012

@author: predictiveds
'''
import zmq
from string import lower
import cjson
import time
import traceback
import uuid
from polymr import get_bind_address, Command, merge_kv_dict, settings
from polymr.file import parse_filenames
from polymr.inout import FileInputReader



class MasterClient():
    _context = None
    _master_queue_name = None
    _master_queue = None
   
    def __init__(self):
        self._context = zmq.Context()
        self._master_queue_name = settings.CLUSTER_MASTER
        self._master_queue = self._context.socket(zmq.REQ)
       
        self._master_queue.connect(self._master_queue_name)
        
    def call_command(self,command, args={}):
        self.send_command(command, args)
        return cjson.decode(self._master_queue.recv())  
    
    def send_command(self,command, args={}):
        self._master_queue.send("%s|%s" % (command,cjson.encode(args)))
    
    def check(self):

        poll = zmq.Poller()
        poll.register(self._master_queue, zmq.POLLIN)
        
        self.send_command('ping')
           
        socks = dict(poll.poll(1000))
        if socks.get(self._master_queue) == zmq.POLLIN:
            self._master_queue.recv()
            run_cluster = True
        else:
            run_cluster = False
        poll.unregister(self._master_queue)
        return run_cluster
    
    def get_jobs(self):
        return self.send_command("jobs")
    
    def close(self):
        self._master_queue.setsockopt(zmq.LINGER, 0)
        self._master_queue.close()
        self._context.term()


def create_job_id():
    
    return str(uuid.uuid1())
    
def start_master():
    
    print "Start master"
    
    jobs = {}
    
    context = zmq.Context()
    
    master_queue_name = settings.CLUSTER_MASTER #"tcp://127.0.0.1:5550"
    master_queue = context.socket(zmq.REP)
    master_queue.bind(get_bind_address(master_queue_name))
    
    result_queue_name = settings.CLUSTER_RESULT #"tcp://127.0.0.1:5551"
    result_queue = context.socket(zmq.PUB)
    result_queue.bind(get_bind_address(result_queue_name))
    
    command_queue_name = settings.CLUSTER["command_queue"] #"tcp://127.0.0.1:5555"
    command_queue = context.socket(zmq.PUB)
    command_queue.bind(get_bind_address(command_queue_name))
    
    task_queue_name = settings.CLUSTER["task_queue"] #"tcp://127.0.0.1:5554"
    task_queue = context.socket(zmq.PULL)
    task_queue.bind(get_bind_address(task_queue_name))
    
    mapper_in_queue_name = settings.CLUSTER["mapper_in_queue"] #"tcp://127.0.0.1:5557"
    mapper_in_queue = context.socket(zmq.PUSH)
    mapper_in_queue.bind(get_bind_address(mapper_in_queue_name))
    
    mapper_out_queue_name = settings.CLUSTER["mapper_out_queue"] #"tcp://127.0.0.1:5558"
    mapper_out_queue = context.socket(zmq.PULL)
    mapper_out_queue.bind(get_bind_address(mapper_out_queue_name))
    
    reducer_in_queue_name = settings.CLUSTER["reducer_in_queue"] #"tcp://127.0.0.1:5559"
    reducer_in_queue = context.socket(zmq.PUB)
    reducer_in_queue.bind(get_bind_address(reducer_in_queue_name))
    
    reducer_out_queue_name = settings.CLUSTER["reducer_out_queue"] #"tcp://127.0.0.1:5560"
    reducer_out_queue = context.socket(zmq.PULL)
    reducer_out_queue.bind(get_bind_address(reducer_out_queue_name))    
    
    poller = zmq.Poller()
    poller.register(master_queue, zmq.POLLIN)  
    poller.register(mapper_out_queue, zmq.POLLIN)
    poller.register(task_queue, zmq.POLLIN)  
    poller.register(reducer_out_queue, zmq.POLLIN)
    time.sleep(1)
    
    cmd = Command(master_queue)
    worker_cmd = Command(command_queue)
    mapper_data_in_cmd = Command(mapper_in_queue)
    mapper_data_out_cmd = Command(mapper_out_queue)
    task_cmd = Command(task_queue)
    reducer_data_out_cmd = Command(reducer_out_queue)
    res_cmd = Command(result_queue)
    
    while True:
        
        print "Wait for command..."
        socks = dict(poller.poll())

        try:
            if socks.get(master_queue) == zmq.POLLIN:
                command, args = cmd.get_command()
                try:
                    command = lower(command)
                    if command == "start":
                        job_id = create_job_id()
                        args["job_id"] = job_id
                        args["active"] = True
                        args["mappers"] = {}
                        args["map-length"] = 0
                        args["reducers"] = {}
                        args["reduced-data"] = {}
                        args["status"] = "new-born"
                        jobs[job_id] = args
                        worker_cmd.send_command("start", args)
                        cmd.send_response(job_id)
                    elif command == "ping":
                        cmd.send_response(True)
                    elif command == "job":
                        job_id = args["job_id"]
                        cmd.send_response(jobs[job_id])
                    elif command == "jobs":
                        cmd.send_response(jobs)
                    elif command == "exit":
                        cmd.send_response(True)
                        break
                    elif command == "send-data-map":
                        job_id = args["job_id"]
                        data = args["data"]
                        cache_line = int(args["cache-line"])
                        jobs[job_id]["status"] = "submitting-input"
                        
                        
                        
                        map_len = 0
                        lines = []
                        input_reader = FileInputReader()
                        input_names = parse_filenames(data)
                        
                        cmd.send_response(True)
                        
                        for input_name in input_names:
                            f = input_reader.read(input_name)
                            
                            for line in f:
                                lines_len = len(lines)
                                if  lines_len > 0 and lines_len % cache_line == 0:
                                    print "submitting %d lines to the cluster..." % (map_len)
                                    mapper_data_in_cmd.send_command(job_id, lines)                   
                                    lines = []
                               
                                lines.append(line)
                                map_len += 1
                                    
                        input_reader.close()
                        
                        lines_len = len(lines)
                        mapper_data_in_cmd.send_command(job_id,lines)
                        print "submitting %d lines to the cluster..." % (map_len)                            
                        
                        jobs[job_id]["status"] = "end-submitting-input"
                        jobs[job_id]["map-length"] = map_len
                        
                    else:
                        print "Undefined command %s with %s" % (command,cjson.encode(args))
                except Exception,e:
                    print "command exception %s %s %s" % (command,e,traceback.print_exc())
            
            if socks.get(mapper_out_queue) == zmq.POLLIN:
                mapper_task_id, args = mapper_data_out_cmd.get_command()
                
                job_id = args["job_id"]
                data = args["data"]
                
                if jobs[job_id]["use-reduce"]:
                
                    jobs[job_id]["status"] = 'reducer-ventilatoring'
                    reducers = jobs[job_id]["reducers"].keys()
                    n_reducers = len(reducers)
                    rc = Command(reducer_in_queue)
                    for key, value in data.items():    
                        task_id = reducers[hash(key) % n_reducers] 
                        rc.send_command(task_id, {"job_id":job_id, "data": {key:value}})
                        try:
                            jobs[job_id]["emitted-data"] +=1
                        except KeyError:
                            jobs[job_id]["emitted-data"] =1
                            
                    
                    
                else: #no reducer defined
                    jobs[job_id]["reduced-data"] = merge_kv_dict(jobs[job_id]["reduced-data"],data)
                    try:
                        jobs[job_id]["collected-mappers"] += 1
                    except KeyError:
                        jobs[job_id]["collected-mappers"] = 1
                        
                    if jobs[job_id]["collected-mappers"] == len(jobs[job_id]["mappers"].keys()):
                        result = jobs[job_id]["reduced-data"]
                        res_cmd.send_command(job_id,result)
                        
                        jobs[job_id] = {}
                        jobs[job_id]["status"] = "completed"
                        jobs[job_id]["active"] = False
                        
    
            if socks.get(reducer_out_queue) == zmq.POLLIN:
                task_id, args = reducer_data_out_cmd.get_command()
                job_id = args["job_id"]
                data = args["data"]

                jobs[job_id]["reduced-data"] = merge_kv_dict(jobs[job_id]["reduced-data"],data)
                
                try:
                    jobs[job_id]["collected-reducers"] += 1
                except KeyError:
                    jobs[job_id]["collected-reducers"] = 1
                    
                if jobs[job_id]["collected-reducers"] == len(jobs[job_id]["reducers"].keys()):
                    result = jobs[job_id]["reduced-data"]
                    res_cmd.send_command(job_id,result)
                    
                    jobs[job_id] = {}
                    jobs[job_id]["status"] = "completed"
                    jobs[job_id]["active"] = False
                    
         
            if socks.get(task_queue) == zmq.POLLIN:
                command, args = task_cmd.get_command()
                
                if command == 'join': 
                    job_id = args["job_id"]
                    task_id = args["task_id"]
                    if args["type"] == "mapper":
                        jobs[job_id]["mappers"][task_id] = args
                    elif args["type"] == "reducer":
                        jobs[job_id]["reducers"][task_id] = args
                elif command == 'receive-input-data':
                    job_id = args["job_id"]
                    task_id = args["task_id"]
                    map_length = args["map-length"]
                    
                    jobs[job_id]["map-length"] -= map_length
                    
                    if jobs[job_id]["status"] == "end-submitting-input" and jobs[job_id]["map-length"] == 0:
                        worker_cmd.send_command("stop-mapper", {"job_id":job_id})
                elif command == 'receive-mapped-data':
                    job_id = args["job_id"]
                    task_id = args["task_id"]
                    try:
                        jobs[job_id]["receive-mapped-data"] += 1
                    except KeyError:
                        jobs[job_id]["receive-mapped-data"] = 1
                        
                    if jobs[job_id]["receive-mapped-data"] == jobs[job_id]["emitted-data"]:
                        jobs[job_id]["status"] = 'reducing'
                        worker_cmd.send_command('run-reducer', job_id)     
                
        except Exception,e:
            print "reducer exception %s %s" % (e,traceback.print_exc())
    
        
    master_queue.close()
    command_queue.close()
    mapper_in_queue.close()
    reducer_in_queue.close()
    context.destroy()
    
    exit()
if __name__ == '__main__':
    
    start_master()