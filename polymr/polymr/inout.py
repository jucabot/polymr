import json
import settings
import datetime
from redis import StrictRedis
from polymr.file import path

class InputReader():
    def __init__(self):
        pass
    
    def read(self,input_file):
        pass
    
    def close(self):
        pass

class MemInputReader(InputReader):
    data = None
    
    def __init__(self,data=None):
        self.data = data
    
    @staticmethod 
    def load_from_file(filename):
        start_time = datetime.datetime.now()
        itself = MemInputReader(data=[])
        f = open(path(filename))
        
        for line in f:
            itself.data.append(line)
        f.close()
        print "Load file %s in %s" % (filename, datetime.datetime.now() - start_time)
        return itself
    
    def read(self,input_file):
        assert self.data != None
        return iter(self.data)
    
    def close(self):
        pass
    
class FileInputReader(InputReader):
    file = None
    
    def __init__(self):
        pass
    
    def read(self,input_file):
        self.file = open(path(input_file))
        return self.file
    
    def close(self):
        if self.file != None:
            self.file.close()
             

class OutputWriter():
    
    def __init__(self):
        pass
    
    def write(self,data,output_file,mode='w'):
        pass
    
    def dumps_output_value(self,key,value):
        if len(value) == 1:
            value = value[0]
        if isinstance(value, str):
            return "%s;%s\n" % (str(key),value)
        else:
            return  "%s;%s\n" % (str(key), json.dumps(value))
    
    
class KeyValueOutputWriter(OutputWriter):
    
    def write(self,data,output_file, mode='w'):
        f = open(path(output_file), mode)
        for key,value in data:
            f.write(self.dumps_output_value(key,value))
        f.close()
        
   
class StdIOOutputWriter(OutputWriter):
    
        
    def write(self,data,output_file,mode='w'):
        print "********** results ***********"
        for key,value in data:
            print self.dumps_output_value(key,value)

class MemOutputWriter(OutputWriter):
    data = None
    
    def write(self,data,output_file, mode='w'):
        self.data = []
        for key,value in data:
            self.data.append(self.dumps_output_value(key,value))
        
    def get_reader(self):
        return MemInputReader(self.data)


class RedisOutputWriter(OutputWriter):
    redis = None
    def __init__(self):
        self.redis = StrictRedis(host=settings.REDIS['HOST'], port=settings.REDIS['PORT'], db=settings.REDIS['DB'])
        
    def write(self,data,output_file, mode='w'):
       
        for key,value in data:
            self.redis.set(key,json.dumps(value))
            if output_file != None:
                self.redis.sadd(output_file,key)
    
    def write_from_file(self,filename, set_name=None):
        
        mem = MemInputReader.load_from_file(filename)
        data = {}
        for line in mem.read(None):
            line_data = line.split(';')
            data[line_data[0]] = json.loads(line_data[1]) 
        self.write(data.items(), set_name)
        
    def write_from_mem(self,data, set_name=None):
        
        self.write(data, set_name)
        
    def save(self):
        self.redis.save()
        
