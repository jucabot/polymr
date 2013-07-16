import cjson

def stream_kv(kv):
    
    if isinstance(kv,list):
        for item in kv:
            print "%s;%s" % (str(item[0]),cjson.encode(item[1]))
    else:
        key,value = kv
        print "%s;%s" % (str(key),cjson.encode(value))