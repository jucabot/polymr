import json
import datetime
import os


class InputReader():
    def __init__(self):
        pass
    
    def read(self,input_file):
        pass
    
    def close(self):
        pass
    
    def get_estimated_size(self):
        pass

class MemInputReader(InputReader):
    data = None
    
    def __init__(self,data=[]):
        self.data = data
    
    @staticmethod 
    def load_from_file(filename):
        start_time = datetime.datetime.now()
        itself = MemInputReader()
        f = open(filename)
        
        for line in f:
            itself.data.append(line)
        f.close()
        print "Load file %s in %s" % (filename, datetime.datetime.now() - start_time)
        return itself
    
    def read(self):
        return iter(self.data)
    
    def close(self):
        pass
    
    def get_estimated_size(self):
        return len(self.data), self.data[:999] if len(self.data)>1000 else self.data
    
class FileInputReader(InputReader):
    file = None
    filename = None
    
    def __init__(self,filename):
        self.filename = filename
    
    def read(self):
        self.file = open(self.filename)
        return self.file
    
    def close(self):
        if self.file != None:
            self.file.close()
    
    def get_estimated_size(self):
        sample = []
        sample_size = 1000
        file_size = os.stat(self.filename).st_size
        f = open(self.filename)
        count = 0
        line_size = 0
        for line in f:
            count +=1
            line_size += len(line)
            sample.append(line)
            if count >= sample_size:
                break
        
        f.close()
        
        return int(sample_size * float(file_size) / float(line_size)), sample

class OutputWriter():
    
    def __init__(self):
        pass
    
    def write(self,data):
        pass
    
    def dumps_output_value(self,key,value):
        if len(value) == 1:
            value = value[0]
        if isinstance(value, str):
            return "%s=%s\n" % (str(key),value)
        else:
            return  "%s=%s\n" % (str(key), json.dumps(value))

class FileOutputWriter(OutputWriter):
    filename = None
    mode = None
    
    def __init__(self,filename,mode='w'):
        self.filename = filename
        self.mode = mode
        
    def write(self,data):
        f = open(self.filename, self.mode)
        for key,value in data:
            f.write(self.dumps_output_value(key,value))
        f.close()
        
   
class StdIOOutputWriter(OutputWriter): 
    
    def write(self,data):
        for key,value in data:
            print self.dumps_output_value(key,value)

class MemOutputWriter(OutputWriter):
    data = None
    
    def write(self,data):
        self.data = []
        for key,value in data:
            self.data.append((key,value))
        
    def get_reader(self):
        return MemInputReader(self.data)


