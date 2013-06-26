'''
Created on 26 oct. 2012

@author: predictiveds
'''
import zmq
import sys
from string import lower
from multiprocessing import cpu_count
import cjson
from multiprocessing.process import Process
import time
import traceback
import uuid
from polymr import Command, load_from_classname, merge_kv_dict, settings
from polymr.inout import MemInputReader


def check_usage():
    if len(sys.argv) != 2:
        print "usage : python -m predictiveds.processing.core.worker master_address"
        return False
    else:
        return True
    

def create_task_id():
    
    return str(uuid.uuid1())

def zmq_run_mapper(command_queue_name, task_queue_name, in_queue_name, out_queue_name):
    
    mapreds = {}
    task_id = create_task_id()
    
    context = zmq.Context()

    task_queue = context.socket(zmq.PUSH)
    task_queue.connect(task_queue_name)
    
    command_queue = context.socket(zmq.SUB)
    command_queue.connect(command_queue_name)
    command_queue.setsockopt(zmq.SUBSCRIBE,"")

    in_queue = context.socket(zmq.PULL)
    in_queue.connect(in_queue_name)
    
    out_queue = context.socket(zmq.PUSH)
    out_queue.connect(out_queue_name)
    
    poller = zmq.Poller()
    poller.register(command_queue, zmq.POLLIN)  
    poller.register(in_queue, zmq.POLLIN)
    
    wc = Command(command_queue)
    tc = Command(task_queue)
    oc = Command(out_queue)
    
    while True:
        socks = dict(poller.poll())

        try:
            if socks.get(in_queue) == zmq.POLLIN:
                job_id, s_data = in_queue.recv().split("|",1)            
                mapred = mapreds[job_id]
                data = cjson.decode(s_data) #json.loads(data)
                map_length = len(data)
                tc.send_command("receive-input-data", {"job_id":job_id,"task_id":task_id,"map-length": map_length})
                input_data = MemInputReader(data)     
                mapred.run_map(input_data)
                
            if socks.get(command_queue) == zmq.POLLIN:
                
                command, args = wc.get_command()
                
                if command == 'start':
                    job_id = args["job_id"]
                    mapred_mod = args["module_name"]
                    mapred_class = args["class_name"]
                    mapred_params = args["params"]
                    
                    mapred = load_from_classname(mapred_mod,mapred_class)
                    mapred.params = mapred_params
                    mapred.verbose = False 
                    mapreds[job_id] = mapred
                    
                    tc.send_command('join', {"job_id": job_id,"task_id": task_id, "type": "mapper" })
                    
                elif command == 'stop-mapper':
                    job_id = args["job_id"]
                    mapred = mapreds[job_id]
                    
                    if "combine" in dir(mapred):
                        mapred.run_combine(mapred.data.items())
                    
                    args["data"] = mapred.data
                    oc.send_command(task_id, args)
                    del mapreds[job_id]
                    
        except Exception, e:
            print "mapper exception  %s %s" % (e,traceback.print_exc())
            
    time.sleep(1)
    
    task_queue.close()
    command_queue.close()
    in_queue.close()
    out_queue.close()
    context.destroy()
    
    exit()

def zmq_run_reducer(command_queue_name, task_queue_name, in_queue_name, out_queue_name):
    
    mapreds = {}
    task_id = create_task_id()

    context = zmq.Context()

    task_queue = context.socket(zmq.PUSH)
    task_queue.connect(task_queue_name)
    
    command_queue = context.socket(zmq.SUB)
    command_queue.connect(command_queue_name)
    command_queue.setsockopt(zmq.SUBSCRIBE, "")

    in_queue = context.socket(zmq.SUB)
    in_queue.connect(in_queue_name)
    in_queue.setsockopt(zmq.SUBSCRIBE, task_id)
    
    out_queue = context.socket(zmq.PUSH)
    out_queue.connect(out_queue_name)
    
    poller = zmq.Poller()
    poller.register(in_queue, zmq.POLLIN)
    poller.register(command_queue, zmq.POLLIN)  
    
    wc = Command(command_queue)
    tc = Command(task_queue)
    ic = Command(in_queue)
    oc = Command(out_queue)

    while True:
        socks = dict(poller.poll())

        try:
            if socks.get(in_queue) == zmq.POLLIN:
                task_id, args = ic.get_command()
                job_id = args["job_id"]
                data = args["data"]
                mapred = mapreds[job_id]
                
                mapred.data = merge_kv_dict(mapred.data,data)
                
                tc.send_command("receive-mapped-data",{"job_id":job_id,"task_id":task_id})
        
            if socks.get(command_queue) == zmq.POLLIN:
                
                command, args = wc.get_command()
                
                if command == 'start':
                    job_id = args["job_id"]
                    mapred_mod = args["module_name"]
                    mapred_class = args["class_name"]
                    mapred_params = args["params"]
                    
                    mapred = load_from_classname(mapred_mod,mapred_class)
                    mapred.params = mapred_params
                    mapred.verbose = False 
                    mapreds[job_id] = mapred
    
                    tc.send_command('join', {"job_id": job_id,"task_id": task_id, "type":"reducer" })
                    
                elif command == 'run-reducer':
                    job_id = args
                    mapred = mapreds[job_id]
                    mapred.run_reduce(mapred.data.items())                    
                    oc.send_command(task_id, {"job_id":job_id, "data":mapred.data_reduced})
                    
                    del mapreds[job_id]
        except Exception,e:
            print "reducer exception %s %s" % (e,traceback.print_exc())
    
    time.sleep(1)
    task_queue.close()
    command_queue.close()
    in_queue.close()
    out_queue.close()
    context.destroy()
    
    exit()
    
def start_worker(master_address):
    
    print "Start worker subscribing to %s" % master_address
    processes = []
    
    num_process = cpu_count()
    command_queue_name = settings.CLUSTER['command_queue']
    task_queue_name = settings.CLUSTER['task_queue']
    mapper_in_queue_name = settings.CLUSTER['mapper_in_queue']
    mapper_out_queue_name = settings.CLUSTER['mapper_out_queue']
    reducer_in_queue_name = settings.CLUSTER['reducer_in_queue']
    reducer_out_queue_name = settings.CLUSTER['reducer_out_queue']
    
    context = zmq.Context()
    controller = context.socket(zmq.SUB)
    controller.connect(master_address)
    controller.setsockopt(zmq.SUBSCRIBE, "")    
    
    time.sleep(1)
        
    
    for i in range(num_process):
        map_process = Process(target=zmq_run_mapper,args=(command_queue_name,task_queue_name, mapper_in_queue_name, mapper_out_queue_name))
        map_process.start()
        processes.append(map_process)
        
        reduce_process = Process(target=zmq_run_reducer,args=(command_queue_name, task_queue_name, reducer_in_queue_name, reducer_out_queue_name))
        reduce_process.start()
        processes.append(reduce_process)
    
    try:
        while True:
            
            print "Wait for command..."
            wc = Command(controller)
            
            command, args = wc.get_command()
            try:
                command = lower(command)
                print "worker command %s" % command
                if command == "exit":
                    for process in processes:
                        process.terminate()
                    break
                else:
                    print "Undefined command %s with %s" % (command,cjson.decode(args))
            except Exception,e:
                print "command exception %s %s %s" % (command,e,traceback.print_exc())
    except Exception:
        pass
    for process in processes:
        process.terminate()
    processes = []
    controller.close()
    context.destroy()
    
    exit()
    
if __name__ == '__main__':
    
    if not check_usage():
        exit()
        
    master_address = sys.argv[1]
    
    start_worker(master_address)