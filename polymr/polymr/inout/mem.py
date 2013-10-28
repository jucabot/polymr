import json
import uuid
import datetime
from polymr.inout import PassingFormatter, AbstractInput, AbstractOutput
import sys

class MemInput(AbstractInput):
    data = None
    
    def __init__(self,data=[]):
        self.data = data
        self.formatter = PassingFormatter()
    
    
    @staticmethod 
    def load_from_file(filename):
        start_time = datetime.datetime.now()
        itself = MemInput()
        f = open(filename)
        
        for line in f:
            itself.data.append(line)
        f.close()
        print "Load file %s in %s" % (filename, datetime.datetime.now() - start_time)
        return itself
    
    def to_file(self):
        filename = '/var/tmp/%s' % str(uuid.uuid1()) 
        f = open(filename,mode='w')
        map(lambda item : f.write(json.dumps(item)),self.data)
        f.close()
        return filename
    
        
    def read(self):
        return self.formatter.format(iter(self.data))
    
    def sample(self,size=100):
        return self.data[0:size-1]

    def close(self):
        pass
    
    def get_estimated_size(self):
        return len(self.data), MemInput(self.data[:999]) if len(self.data)>1000 else self
    
    def count(self,engine=None,debug=False,options={}):
        return len(self.data)

class StdIOInput(AbstractInput):
    io = None
    
    def __init__(self,io = sys.stdin):
        self.io = io
        self.formatter = PassingFormatter()

    def read(self):
        return self.formatter.format(self.io)

   
class StdIOOutput(AbstractOutput): 
    
    def is_memory(self):
        return True
    
    def write(self,data):
        for key,value in data:
            print self.dumps_output_value(key,value)

class MemOutput(AbstractOutput):
    data = None
    
    def is_memory(self):
        return True
    
    def write(self,data):
        self.data = []
        for key,value in data:
            self.data.append((key,value))
        
    def get_reader(self):
        return MemInput(self.data)

    def __str__(self):
        return "Memory object : %s" % (self.data)

