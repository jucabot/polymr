import json

class PassingFormatter(object):
    options = None
    def __init__(self,options={}):
        self.options = options
    
    def format(self,iterator):
        for row in iterator:
            yield row

class AbstractInput(object):
    formatter = None
    
    def __init__(self):
        pass
    
    def read(self,input):
        pass
    
    def close(self):
        pass
    
    def is_distant(self):
        return False
    
    def to_file(self):
        pass
    
    def get_estimated_size(self):
        pass
    
    

class AbstractOutput(object):
    
    def __init__(self):
        pass
    
    def is_distant(self):
        return False
    
    def is_memory(self):
        return False
    
    def write(self,data):
        pass
    
    def dumps_output_value(self,key,value):
        if len(value) == 1:
            value = value[0]
        if isinstance(value, str):
            return "%s=%s\n" % (str(key),value)
        else:
            return  "%s=%s\n" % (str(key), json.dumps(value))
