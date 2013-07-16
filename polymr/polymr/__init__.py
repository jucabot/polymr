import cjson
import datetime
def load_from_classname(mod_name, class_name):
    mod = __import__(mod_name, fromlist=[class_name])
    klass = getattr(mod, class_name)
    return klass()

def load_from_modclassname(mod_class_name):
    mod_name, class_name = mod_class_name
    return load_from_classname(mod_name, class_name)

def load_from_fname(mod_name, function_name):
    mod = __import__(mod_name, fromlist=[function_name])
    function = getattr(mod, function_name)
    return function


def get_bind_address(address):
    protocol, hostname, port = address.split(':')
    
    return "%s://*:%s" % (protocol, port)

class Command():
    
    _queue = None
    
    def __init__(self,queue):
        self._queue = queue

    def get_command(self, use_json=True):
        
        command, s_args = self._queue.recv().split("|",1)
        print "received %s at %s" % (command,datetime.datetime.now())
        if use_json:
            return command, cjson.decode(s_args)
        else:
            return command, str(s_args)
        
    def send_command(self,command, args, use_json=True ):
        if use_json:
            self._queue.send_string("%s|%s" % (command,cjson.encode(args)))
        else:
            self._queue.send_string("%s|%s" % (command, str(args)))
            
    def send_response(self,response):    
        self._queue.send(cjson.encode(response))

def merge_kv_dict(data,data_to_merge):
    for (key, value) in data_to_merge.items():
            if key in data:
                data[key].extend(value)
            else:
                data[key] = value
    return data
