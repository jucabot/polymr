from polymr.mapreduce import MapReduce
import base64
from uuid import uuid1
import types
import inspect

NA = ''

class Count(MapReduce):
    """
        List the count of messages
    """
    
    def map(self, text):
        return [('count',1)]
                
    def combine(self, key, values):
        return (key, sum(values))
        
    def reduce(self, key, values):
        return (key, sum(values))


class FeaturedFunction(MapReduce):
    def set_function(self,function):
        if type(function) is types.StringType:
            
            if 'function' in function:
                function_code = function
            else:
                function_code = "function = lambda row : %s" % function
            
            
            self.params['function-name'] = 'function'
        elif type(function) is types.FunctionType and function.func_name != '<lambda>':
            filter_function_code = inspect.getsource(function).strip()
            self.params['function-name'] = function.func_name
        else:
            raise TypeError("%s is not a supported function" % function)
        
        self.params['function-code'] = base64.b64encode(function_code)
        
    def get_function(self):
        func_code = base64.b64decode(self.params['function-code'])
        func_name = self.params['function-name']
        
        exec(func_code)
        function = eval(func_name)
        
        return function

class Filter(FeaturedFunction):
        
    def map_partition(self, iterator):
        
        filter_function = self.get_function()
        result = filter(filter_function,iterator)
        
        for line in result:
            yield(uuid1(),line)
            
class Apply(FeaturedFunction):
        
    def map_partition(self, iterator):
        
        apply_function = self.get_function()
        result = map(apply_function,iterator)
        
        for line in result:
            yield(uuid1(),line)