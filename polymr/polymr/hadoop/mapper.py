#! /usr/bin/env python
'''
Created on 3 nov. 2012

@author: predictiveds
'''
import sys
from polymr import load_from_classname
import cjson

def check_usage():
    return len(sys.argv) == 4

if __name__ == '__main__':
    
    if not check_usage():
        print "usage mapper mapred_module mapred_classname params_filename"
        exit()
    
    mod_name = sys.argv[1]
    class_name = sys.argv[2]
    cache_file = sys.argv[3]
    
  
    mapred = load_from_classname(mod_name,class_name)
    mapred.params = cjson.decode(open(cache_file).read())
    mapred.streamming = True
    
    map(mapred.map,sys.stdin)
        
   
    
    