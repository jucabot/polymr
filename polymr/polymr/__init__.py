
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


