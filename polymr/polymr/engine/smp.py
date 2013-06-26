import traceback
from multiprocessing import cpu_count
import datetime
import math
from polymr import merge_kv_dict, load_from_classname, mem
from polymr.inout import MemInputReader
from multiprocessing.process import Process
from multiprocessing.queues import Queue
from Queue import Empty


class SingleCoreEngine():
    
    _mapred = None
    
    def __init__(self,mapred):
        self._mapred = mapred    
    
    def run(self,input_reader,output_writer):
        
        start_time = datetime.datetime.now()
        self._mapred.reset()
        
        print "start job %s on a single core" % self._mapred.__class__.__name__
        
        map_len = self._mapred.run_map(input_reader)
        #print "map %s lines with mem size of %d" % (map_len,mem.asizeof(self._mapred))
        
        if "combine" in dir(self._mapred):
            combine_len = self._mapred.run_combine(self._mapred.data.items())
            #print "combine %s lines" % (combine_len)
        
        if "reduce" not in dir(self._mapred):
            self._mapred.data_reduced = self._mapred.data
        else:
            reduce_len = self._mapred.run_reduce(self._mapred.data.items())
            #print "reduce %s lines" % (reduce_len)
            
        output_writer.write(self._mapred.post_reduce())
        
        print "end job %s in %s with mem size of %d" % (self._mapred.__class__.__name__, (datetime.datetime.now()-start_time),mem.asizeof(self._mapred))
    
def q_run_mapper(mapred_mod, mapred_class,mapred_params, in_queue, out_queue,log_queue):
    
    try:
        mapred = load_from_classname(mapred_mod,mapred_class)
        mapred.params = mapred_params
        mapred.verbose = False
        
        while True:
            
            data = in_queue.get()
            
            
            if isinstance(data, str) and data == 'STOP':
                #log_queue.put("INFO: receive stop command")
                break
            else:
                #log_queue.put("INFO: map %d lines" % len(data))
                input_data = MemInputReader(data)     
                mapred.run_map(input_data)
        
        if "combine" in dir(mapred):
            mapred.run_combine(mapred.data.items())
                
        out_queue.put(mapred.data)
        
    except Exception,e:
        log_queue.put("FATAL: mapper exception  %s %s" % (e,traceback.print_exc()))
        
def q_run_reducer(mapred_mod,mapred_class,mapred_params,in_queue, out_queue,log_queue):
    try:
        mapred = load_from_classname(mapred_mod,mapred_class)
        mapred.verbose = False
        mapred.params = mapred_params
        
        while True:
            data = in_queue.get()
            
            if isinstance(data, str) and data == "STOP":
                #log_queue.put("INFO: receive stop command")
                break
            else :
                #log_queue.put("INFO: reduce %d lines" % len(data))
                mapred.run_reduce(data.items())
            
        out_queue.put(mapred.data_reduced)
    except Exception,e:
        log_queue.put("FATAL: mapper exception  %s %s" % (e,traceback.print_exc()))

class MultiCoreEngine():
    
    _mapred = None
    
    _out_queue = None
    _in_queue = None
    _log_queue = None
        
    _processes = None
    
        
    def __init__(self,mapred):
        self._mapred = mapred
            
    def _start(self,name,cpu, module_name, class_name, params):
        fn = None
        
        self._processes = []
        self._in_queue = Queue()
        self._out_queue = Queue()
        self._log_queue = Queue()
        

        
        if name == "mapper":
            fn = q_run_mapper
        elif name == "reducer":
            fn = q_run_reducer
        
        for i in range(cpu):
            process = Process(target=fn,args=(module_name, class_name ,params, self._in_queue, self._out_queue, self._log_queue))
            self._processes.append(process)
            process.start()
    
    def _stop(self):
        
        for process in self._processes:
            self._in_queue.put("STOP")
        
        while not self._log_queue.empty():
            print self._log_queue.get()
    
    def _get_data_chunks(self):
        chunks = []
        for process in self._processes:
            chunks.append(self._out_queue.get())
        
        return chunks
    
    def _set_data_chunks(self, chunks):
        
        for chunk in chunks:
            self._in_queue.put(chunk)
        
                        
    def _send_lines(self,lines, cpu, lines_len ):
        line_splits = [lines[i* lines_len / cpu : (i+1)* lines_len / cpu] for i in range(cpu) ]
                    
        for i in range(cpu): 
            self._in_queue.put(line_splits[i])
    
    def _terminate(self):
        for process in self._processes:
            process.join()
            process.terminate()
                
        self._in_queue.close()
        self._out_queue.close()
        self._processes = None
        
    def _force_terminate(self):
        for process in self._processes:
            process.terminate()
            
    def _merge_data(self, data):
       
        self._mapred.data = merge_kv_dict(self._mapred.data,data)
                
    def _merge_reduced_data(self, data):
       
        self._mapred.data_reduced = merge_kv_dict(self._mapred.data_reduced,data)
                
    def _split_data(self, num_splits):
        splits = []
        index = 0
        
        len_data = len(self._mapred.data)
        
        chunk_len = int(math.ceil(len_data / float(num_splits)))
        
        if chunk_len == 0:
            splits.append(self._mapred.data)
        else:        
            for i in range(int(math.ceil(len_data/float(chunk_len)))):
                splits.append({})
                
            for (key, value) in self._mapred.data.items():
                
                i = int(math.floor(index / float(chunk_len)))
                       
                splits[i][key]=value
                
                index = index + 1
        
        return splits
    
    
    def _run_map(self,cpu,cache_line,input_reader ):
        
        map_len = 0
        lines = []

        self._start("mapper",cpu, self._mapred.__class__.__module__,self._mapred.__class__.__name__ ,self._mapred.params)
    
        try:
            f = input_reader.read()
        
            for line in f:
                try:
                    print self._log_queue.get_nowait()
                except Empty:
                    pass
                
                lines_len = len(lines)
                if  lines_len > 0 and lines_len % cache_line == 0:
                    self._send_lines(lines, cpu, lines_len)        
                    lines = []
               
                lines.append(line)
                map_len += 1
                    
            input_reader.close()
            
            lines_len = len(lines)
            self._send_lines(lines, cpu, lines_len)
            
            self._stop()
            
            for data in self._get_data_chunks():
                self._merge_data(data)
                
            self._terminate()
            
        
        except Exception,e:
            print "Exception while mapping : %s\n%s" % (e,traceback.print_exc())
            self._force_terminate()
             
        return map_len
        
    def _run_reduce(self,cpu):
        
        data_splits = self._split_data(cpu)
        self._mapred.data = None
               
        self._start("reducer",cpu, self._mapred.__class__.__module__,self._mapred.__class__.__name__ ,self._mapred.params)
        
        try:
            
            self._set_data_chunks(data_splits)
        
            self._stop()
            
            for data in self._get_data_chunks():
                self._merge_reduced_data(data)
            
            self._terminate()
            
        except Exception,e:
            print "Exception while reducing : %s\n%s" % (e,traceback.print_exc())
            self._force_terminate()
            
        return len(self._mapred.data_reduced.keys())
    
    def run(self,input_reader,output_writer,cpu=cpu_count()-1, cache_line=100000):
        
        start_time = datetime.datetime.now()
        
        print "start job %s on %d cores" % (self._mapred.__class__.__name__, cpu)
        
        map_len = self._run_map(cpu, cache_line, input_reader)
                
        #print "map %s lines in %s with mem size of %d" % (map_len, datetime.datetime.now()-start_time,mem.asizeof(self._mapred))

        if "reduce" not in dir(self._mapred):
            
            self._mapred.data_reduced = self._mapred.data
        
        else:
            if len(self._mapred.data) < cpu:
                reduce_len = self._mapred.run_reduce(self._mapred.data.items())
            else:
                reduce_len = self._run_reduce(cpu)
            #print "reduce %s lines in %s with mem size of %d" % (reduce_len, datetime.datetime.now()-start_time,mem.asizeof(self._mapred))
            
        output_writer.write(self._mapred.post_reduce())
        
        print "end job %s in %s with mem size of %d"  % (self._mapred.__class__.__name__, (datetime.datetime.now()-start_time),mem.asizeof(self))