import settings

def path(filename):
    
    return settings.DATA_ROOT + "/" + filename

def relative_path(filename):
    
    return filename.replace(settings.DATA_ROOT,"")

def parse_filenames(input_file):
    
    input_names = []
    if input_file != None:
        input_names = input_file.split(',')
    else:
        input_names.append(None)
    
    return input_names



        


