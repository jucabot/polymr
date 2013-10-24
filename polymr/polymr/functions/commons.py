from polymr.mapreduce import MapReduce

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


