#! /usr/bin/env python
'''
Created on 3 nov. 2012

@author: predictiveds
'''
import sys
from polymr import load_from_classname
import json

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
    mapred.params = json.load(open(cache_file))
    mapred.verbose = False
    mapred.streamming = True
    
    for line in sys.stdin:
        mapred.map(line)