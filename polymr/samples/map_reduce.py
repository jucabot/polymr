from polymr.mapreduce import MapReduce
from polymr.inout.file import FileInput
from polymr.inout.mem import MemOutput


class TicketGrouper(MapReduce):
    
    def map(self, text):
        tickets = text.split(',')[9]
        
        for ticket in tickets.split(' '):
            yield (ticket,1)
        
    def combine(self, key, values):
        return (key, sum(values))
        
    def reduce(self, key, values):
        return (key, sum(values))

def print_result(out):
    result = sorted(map(lambda kv : (kv[0],kv[1][0]),out.data), key=lambda (ticket,count) : -1*count )
    
    for (ticket,count) in result:
        print "%s\t : %d" % (ticket,count)

titanic = FileInput('titanic.csv')

mapred = TicketGrouper()
out = MemOutput()
mapred.run(titanic,out)

print_result(out)
