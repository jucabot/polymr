from polymr.inout import PassingFormatter, AbstractInput, AbstractOutput
import os
from polymr.inout.mem import  MemOutput
from polymr.functions.commons import Count, Filter
from polymr.functions.feature_set import FieldFrequency, FieldSummary, Apply
from polymr.functions.feature_set import FeatureSelector
from uuid import uuid1


class FileInput(AbstractInput):
    file = None
    filename = None
    
    def __init__(self,filename):
        self.filename = filename
        self.formatter = PassingFormatter()
    
    def to_file(self):
        return self.filename
    
    def read(self):
        self.file = open(self.filename)
        return self.formatter.format(self.file)
    
    def close(self):
        if self.file != None:
            self.file.close()
    def as_output(self):
        return FileOutput(self.filename)
    
    def get_estimated_size(self,sample_size = 1000):
        file_size = os.stat(self.filename).st_size
        f = open(self.filename)
        count = 0
        line_size = 0
        for line in f:
            count +=1
            line_size += len(line)
            
            if count >= sample_size:
                break
        
        f.close()
        
        return int(sample_size * float(file_size) / float(line_size))
    
    def sample(self,size=100):
        self.file = open(self.filename)
        sample = []
        i=0
        for row in self.file:
            sample.append(row)
            if i>=size:
                break
            else:
                i+=1
        
        return self.formatter.format(sample)
    
    
    def count(self,engine=None,debug=False,options={}):
        out = MemOutput()
        Count().run(self,out,engine,debug,options)
        return out.data[0][1][0]

    def compute(self,mapred,engine=None,debug=False,options={}):
        out = MemOutput()
        mapred.run(self,out,engine,debug,options)
        return out.data
    
    def map_reduce(self,mapred,output,engine=None,debug=False,options={}):
        mapred.run(self,output,engine,debug,options)
    
    def filter(self,filter_function, output=None,engine=None,debug=False,options={}):
        if output is None:
            output = self.as_output()
            output.filename = '/var/tmp/%s' % uuid1()
            
        mapred = Filter()
        mapred.set_function(filter_function)
        mapred.run(self, output, engine, debug, options)
            
        return output.as_input()
    
   
    
class CsvFormatter(PassingFormatter):
    
    def format(self,iterator):
        separator = self.options['separator'] if 'separator' in self.options else ','
        fields = self.options['fields']
        
                    
        if fields is None:
            
            for row in iterator:
                row_values = row.strip().split(separator)
                values = {}
                for i in range(len(row_values)):
                    values[i] = row_values[i]
                yield values
        else:
            for row in iterator:
                row_values = row.strip().split(separator)
                values = {}
                for field in fields.items():
                    name, index = field
                    try:
                        values[name] = row_values[index]
                    except IndexError:
                        pass
                yield values
"""
Structured file separated by separator
Statistical and data management functions
"""                
class CsvFileInput(FileInput):
    separator = None
    fields = None
    use_headers = None
    
    def __init__(self, filename,separator=',',fields=None):
        super(CsvFileInput,self).__init__(filename)
        #self.use_headers = use_headers
        self.separator = separator
        self.formatter = CsvFormatter(options={'separator' : separator,'fields':fields})
        self.fields = fields
        
    def read(self):
        self.file = open(self.filename)
        return self.formatter.format(self.file)
    
    def as_output(self):
        return CsvFileOutput(self.filename,separator=self.separator,fields=self.fields)
    
                
    def select(self,fields):
        return CsvFileInput(self.filename,self.separator,fields)

    def frequency(self,engine=None,debug=False,options={}):
        out = MemOutput()
        FieldFrequency().run(self,out,engine,debug,options)
        return map(lambda kv : (kv[0],kv[1][0]),out.data)
    
    def summary(self,engine=None,debug=False,options={}):
        out = MemOutput()
        FieldSummary().run(self,out,engine,debug,options)
        result = {}
        def _build_result(result,kv):
            result[kv[0]] = kv[1][0]
        map(lambda kv : _build_result(result,kv),out.data)
            
        return result
    
    def print_summary(self,engine=None,debug=False,options={}):
        resume = self.summary(engine, debug, options)
        
        for name, feature in resume.items():
            
            print '\n*** Feature %s ***' % name
            for k,v in sorted(feature.items()):
                print "    %s : %s" % (k,v)
        return resume
    
    def explain(self,target,feature_set=None, engine=None,debug=False,options={}):
        
        if feature_set is None:
            feature_set = self.summary(engine, debug, options)
        
        out = MemOutput()
        mapred = FeatureSelector()
        mapred.set_target(target)
        mapred.set_featureset(feature_set)
        mapred.run(self,out,engine,debug,options)
        result = {}
        
        def _build_result(result,kv):
            result[kv[0]] = kv[1][0]
        map(lambda kv : _build_result(result,kv),out.data)
           
        return result
    
    def print_explain(self,target,feature_set=None,engine=None,debug=False,options={}):
        resume = self.explain(target,feature_set,engine, debug, options)
        
        resume = sorted(resume.items(),key=lambda value: -1*value[1])
        
        print '*** Feature selection to predict %s ***'  % (target)
            
        for (feature,score) in resume:
            
            print 'Feature %s : %f' % (feature,score)
            
        return resume
    
    def add_feature(self,name,feature_function, output=None,engine=None,debug=False,options={}):
        if output is None:
            output = self.as_output()
            output.filename = '/var/tmp/%s' % uuid1()
        
        mapred = Apply()
        self.fields[name] = max(self.fields.values()) + 1
        mapred.set_function(feature_function)
        mapred.set_name(name)
        mapred.run(self, output, engine, debug, options)
        
        return output.as_input() 
    
class FileOutput(AbstractOutput):
    filename = None
    mode = None
    
    def __init__(self,filename,mode='w'):
        self.filename = filename
        self.mode = mode
        
    def write(self,data):
        f = open(self.filename, mode=self.mode)
        for key,value in data:
            f.write(self.dumps_output_value(key,value))
        f.close()
    
    def __str__(self):
        return "file name : %s" % (self.filename)

    def as_input(self):
        return FileInput(self.filename)
    
class CsvFileOutput(FileOutput):
    
    fields = None
    separator = None
    
    def __init__(self,filename,mode='w',separator=',',fields=None):
        super(CsvFileOutput,self).__init__(filename,mode)
        self.separator = separator
        self.fields = fields
    
    def as_input(self):
        fields = {}
        i=0
        #recompute the fields list
        for key in map(lambda kv : kv[0], sorted(self.fields.items(),key=lambda value: value[1])):
            fields[key] = i
            i +=1

        return CsvFileInput(self.filename,self.separator,fields)
    
    def dumps_output_value(self,key,values):
        if self.fields is None:
            return self.separator.join(values[0].values())
        else:
            out = []
            value_list = map(lambda kv : kv[0], sorted(self.fields.items(),key=lambda value: value[1]))
            for key in value_list:
                out.append(str(values[0][key]))
            return self.separator.join(out) + '\n'