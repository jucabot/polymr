#! /usr/bin/env python
import sys
from itertools import groupby
from operator import itemgetter
from polymr import load_from_classname
import cjson
from polymr.hadoop import stream_kv

def read(f):
    for line in f:
        line = line.strip()
        yield line.split(';')
        
def check_usage():
    return len(sys.argv) == 4

if __name__ == '__main__':
    
    if not check_usage():
        print "usage reducer mapred_module mapred_classname params_filename"
        exit()
    
    mod_name = sys.argv[1]
    class_name = sys.argv[2]
    cache_file = sys.argv[3]
     
    mapred = load_from_classname(mod_name,class_name)
    mapred.params = cjson.decode(open(cache_file).read())
    mapred.streamming = True
 
    data = read(sys.stdin)
    
    def reduce_line(kv):
        key,group = kv
        values = [cjson.decode(item[1]) for item in group]
        
        if 'reduce' in dir(mapred):
            kv = mapred.reduce(key,list(values))
        
        else:
            kv = (key,values)
       
        stream_kv(kv)
    
    map(reduce_line, groupby(data, itemgetter(0)))
