import subprocess
import math
from glob import iglob
import shutil
import os
import datetime
import settings
import multiprocessing
from multiprocessing.process import Process
from multiprocessing.queues import Queue
from multiprocessing import Pool
import json


def path(filename):
    
    return settings.DATA_ROOT + "/" + filename

def relative_path(filename):
    
    return filename.replace(settings.DATA_ROOT,"")

def parse_filenames(input_file):
    
    input_names = []
    if input_file != None:
        input_names = input_file.split(',')
    else:
        input_names.append(None)
    
    return input_names


def _str_iter(value):
    yield str(value)
    
def convert_keyvalue_csv(in_file, out_file,print_headers=True):
    fi = open(in_file)
    fo = open(out_file,mode="w")
    
    for line in fi:
        try:
            key,value = line.split(";",1)
            json_object = json.loads(value)
            
            if print_headers:
                fo.write("%s;%s\n" % ("key", ";".join(json_object.keys())))
                print_headers = False
            
            fo.write("%s;%s\n" % (key, ";".join([ unicode(value) for value in json_object.values()])))
        except Exception:
            print "ERROR for %s" % line
    fo.close()
    fi.close()
        

def count_file_lines(filename):
    """ Count the number of line of a text file with wc -l command line tool (linux dependency)
    
    filename - absolute local path of the file
    """
    
    result = subprocess.check_output(["wc", "-l",filename])
    return int(result.split()[0]) #return wc count result

def top_file(filename, num_lines):
    """ head of lines a text file with head -n command line tool (linux dependency).
    
    filename - absolute local path of the file
    num_lines - number of lines
    """
    output_filename = filename + ".top" + str(num_lines)
    f = open(path(output_filename),mode='w')
    f.write(subprocess.check_output(["head", "-n", str(num_lines),path(filename)]))
    f.close()
    
    return output_filename
    
    
def split_file(filename, num_blocks, split_pattern=".split-"):
    """ Split by block of lines a text file with split -l command line tool (linux dependency).
    The number of line per file splits is distributed equally.
    The splited filename follow the split_pattern filename + split_pattern + 00 (split increment on 2 digits filled by zero)
    
    filename - absolute local path of the file
    num_blocks - number of splits
    split_pattern - splitted filename sufix (default .split-)
    """
    splits = []
    file_lines = count_file_lines(filename)
    
    
    if file_lines < num_blocks:
        splits.append(filename)
    else:
        num_split_lines = int(math.ceil(file_lines / float(num_blocks)))
        subprocess.call(["split", "-l", str(num_split_lines),"-d",filename,filename + split_pattern])
    
        num_splits = int(math.ceil(file_lines/float(num_split_lines)))
        for i in range(num_splits):
            splits.append(filename + ".split-" + str(i).zfill(2))
            
    return splits
    
def remove_splits(filename,split_pattern=".split-"):
    """ Remove the splited files of the specified file following the split_pattern : filename + split_pattern + *
    
    filename - absolute local path of the file
    split_pattern - splited filename suffix (default .split-)
    """
    for filename in iglob(filename + split_pattern + "*"):
        os.remove(filename)
    
def concat_splits(filename):
    """ Compact splited files as an unique file
    
    filename - absolute local path of the file. The file have to be splited before.
    """
    destination = open(filename, 'w')
    for split in iglob(filename + ".split-*"):
        shutil.copyfileobj(open(split, 'r'), destination)
    destination.close()

def get_file_splits(filename):
    """ List the splited files of a file
    
    filename - absolute local path of the file. The file have to be splited before.
    """
    splits = []
    for filename in iglob(filename + ".split-*"):
        splits.append(filename)
    return splits

"""
*****
"""
def process_line(line):
    #time.sleep(.0000001)
    return 1



def test_single_IOPS(filename):
    count=0
    start_time = datetime.datetime.now()
    print "run on 1 cpu (single core)"
    
    f = open(filename, mode='r')
    
    for line in f:
        count+=process_line(line)
        
    f.close()
    print "%d lines - running in %s" % (count,datetime.datetime.now()-start_time)
    return datetime.datetime.now()-start_time

def file_count(filename,out_queue):
    count=0
    f = open(filename, mode='r')
    
    for line in f:
        count+=process_line(line)
        
    f.close()
    
    out_queue.put(count)

def test_parallel_IOPS(filename, split=True, cpu_count = multiprocessing.cpu_count()-1):
    processes = []
    
    
    print "%d cpu available" % cpu_count
    
    
    out_queue = Queue()
    
    if split:
        start_time = datetime.datetime.now()
        splits = split_file(filename, cpu_count)
        print "split in %d - running in %s" % (len(splits),datetime.datetime.now()-start_time)
    else:
        splits = get_file_splits(filename)
        
    start_time = datetime.datetime.now()
    
    for i in range(len(splits)):
        process = Process(target=file_count, args=(splits[i],out_queue, ))
        processes.append(process)
        process.start()
    
    total = 0
    for process in processes:
            total += out_queue.get()
           
    for process in processes:    
        process.join()

    print "%d lines - running in %s" % (total,datetime.datetime.now()-start_time)
    return datetime.datetime.now()-start_time
    
    
def test_pool_IOPS(filename, cpu=multiprocessing.cpu_count()-1, cache_line=100000):
    
    start_time = datetime.datetime.now()
    
    print "run on %d cpu as multi process" % cpu
    
    pool = Pool(cpu)
    lines = []
    total = 0
    f = open(filename,mode='r')
    subtotal = []
    for line in f:
        lines_len = len(lines)
        if  lines_len > 0 and lines_len % cache_line == 0:
            pool.map_async(process_line,list(lines),callback=subtotal.extend)
            lines = []
       
        lines.append(line)
            
    f.close()
    
    r = pool.map_async(process_line,list(lines),callback=subtotal.extend)    
   
    r.wait()
    total = sum(subtotal)
   
        
    print "count %d - run in %s" % (total,datetime.datetime.now()-start_time)
    
    pool.terminate()
    return datetime.datetime.now()-start_time
 
