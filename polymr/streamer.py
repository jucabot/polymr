#! /usr/bin/env python

import sys
sys.path.append('.')
sys.path.append('./polymr')  

import pickle
import base64
import types
from polymr.inout.mem import StdIOInput


try:
    import cjson
    
    def json_load(str_json):
        return cjson.decode(str_json)
    
    def json_dump(obj):
        return cjson.encode(obj)
except ImportError:

    import json
    def json_load(str_json):
        return json.loads(str_json)
    
    def json_dump(obj):
        return json.dumps(obj)

def load_from_classname(mod_name, class_name):
    mod = __import__(mod_name, fromlist=[class_name])
    klass = getattr(mod, class_name)
    return klass()

def stream_kv(kv):
    
    
    if isinstance(kv,list) or isinstance(kv, types.GeneratorType):
        for item in kv:
            print "%s;%s" % (unicode(item[0]),json_dump(item[1]))
    else:
        key,value = kv
        print "%s;%s" % (unicode(key),json_dump(value))

def read(f):
    for line in f:
        line = line.strip()
        key,value = line.split(';',2)
        yield (key,value)

def mapper(mapred,meta):
    
    formatter = load_from_classname(meta['input_module_name'],meta['input_class_name'])
    formatter.options = meta['input_options']
    
    input_source = StdIOInput()
    input_source.formatter = formatter
    mapred.run_map(input_source)
    
    for kv in mapred.data.items():
        key = unicode(kv[0])
        for value in kv[1]:
            print "%s;%s" % (key,json_dump(value))
    
    #map(lambda line : stream_kv(mapred.map(line)),formatter.format(sys.stdin))
    
    

def group_by_key(data):
    groups = {}
    
    for key,value in data:
        try:
            groups[key].append(value)
        except KeyError:
            groups[key] = [value]
    return groups.items()

def combiner(mapred):
    
    data = read(sys.stdin)

    def combine(kv):
        key,group = kv
        
        values = []
        for item in group:
            values.append(json_load(item))
            
        
        kv = mapred.combine(key,list(values))
            
        stream_kv(kv)

    #exit(group_by_key(data))

    map(combine, group_by_key(data))

def reducer(mapred):
 
    data = read(sys.stdin)
    
    def reduce_line(kv):
        key,group = kv
        values = [json_load(item) for item in group]
        
        kv = mapred.reduce(key,list(values))
        
        stream_kv(kv)
    
    #exit(group_by_key(data))

    map(reduce_line, group_by_key(data))

if __name__ == '__main__':
    
    
    method = sys.argv[1]
    args = sys.argv[1:]

    mod_name = args[1]
    class_name = args[2]
    cache_file = args[3]
        
    
  
    mapred = load_from_classname(mod_name,class_name)
    mapred.params = json_load(open(cache_file).read())
   
    
    if method == "mapper":
        mapper(mapred,mapred.params['_input_meta'])
    elif method == "combiner":
        combiner(mapred)
    elif method == "reducer":
        reducer(mapred)  
    else:
        exit("undefined method %s" % method)
        