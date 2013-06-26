'''
Created on 24 oct. 2012

@author: predictiveds
'''
import json
from polymr.mapreduce import MapReduce
from polymr.file import path
from polymr import merge_kv_dict





class JoinMapRed(MapReduce):
    """
        List the Word matching a filter list
    """
    
    def load_joiner_from_file(self,joiner_file):
       
        f = open(path(joiner_file))
        self._set_joiner(f)
        f.close()
        
    def set_joiner(self,joiner_array):
       
        self._set_joiner(joiner_array)
        
    def _set_joiner(self,variable_iter):
        self.params["joiner"] = {}
        for line in variable_iter:
            key, value = line.split(';',1)
            self.params["joiner"][key] = value
        
    
    def map(self, text):
        data = text.split(";",1)
        
        key = data[0]
        value = json.loads(data[1])
        
        if key in self.params["joiner"]:
            value = merge_kv_dict(self.params["joiner"][key],value)
            
        self.collect(key, value)


class FilterMapRed(MapReduce):
    """
        List the Word matching a filter list
    """
    
    def load_filter_from_file(self,filter_file):
       
        f = open(path(filter_file))
        self._set_filter(f)
        f.close()
        
    def set_filter(self,filter_array):
       
        self._set_filter(filter_array)
        
    def _set_filter(self,variable_iter):
        self.params["filters"] = {}
        for line in variable_iter:
            data = line.split(';')
            key = data[0]
            self.params["filters"][key] = key
        
    
    def map(self, text):
        data = text.split(";")
        
        key = data[0]
        value = json.loads(data[1])
        
        if key in self.params["filters"]:
            self.collect(key, value)

class ExcludeMapRed(FilterMapRed):
    """
        List the Word excluding a filter list
    """

    def map(self, text):
        data = text.split(";")
        
        key = data[0]
        value = json.loads(data[1])
        
        if key not in self.params["filters"]:
            self.collect(key, value)
            
