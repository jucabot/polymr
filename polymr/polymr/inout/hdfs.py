from polymr.inout import AbstractOutput, PassingFormatter, AbstractInput
from polymr.inout.mem import MemOutput
from polymr.functions.commons import Count


class HdfsInput(AbstractInput):
    filename = None
    
    def __init__(self,filename):
        self.filename = filename
        self.formatter = PassingFormatter()
    
    def is_distant(self):
        return True
    
    def count(self,engine=None,debug=False,options={}):
        out = MemOutput()
        Count().run(self,out,engine,debug,options)
        return out.data[0][1][0]


class HdfsOutput(AbstractOutput):
    filename = None
    
    def __init__(self,filename):
        self.filename = filename
        
    def is_distant(self):
        return True

    def __str__(self):
        return "HDFS file name : %s" % (self.filename)
    
