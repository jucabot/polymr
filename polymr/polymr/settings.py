## Settings for predictiveds platform ##

DATA_ROOT = '/media/data/predictiveds/data'
REDIS = {
         'HOST' : 'localhost',
         'PORT' : 6379,
         'DB' : 0
         }

#USE_CLUSTER = False
CLUSTER_MASTER = "tcp://127.0.0.1:5550" #client job request
CLUSTER_RESULT = "tcp://127.0.0.1:5551" #master result queue - pub by job id

CLUSTER = {
    'task_queue' : "tcp://127.0.0.1:5554", #heartbeat task channel to master
    'command_queue' : "tcp://127.0.0.1:5555", #command pub-sub channel with task process
    'worker_queue' : "tcp://127.0.0.1:5556", #worker command pub-sub channel
    'mapper_in_queue' : "tcp://127.0.0.1:5557", #push input data to tasks
    'mapper_out_queue' : "tcp://127.0.0.1:5558", #push mapped data to master
    'reducer_in_queue' : "tcp://127.0.0.1:5559",
    'reducer_out_queue' : "tcp://127.0.0.1:5560",

     }
#USE_HADOOP = False
HADOOP = {
          'command' : "cat {data-root}/{input}|python -m predictiveds.processing.core.hadoop.mapper {module} {class} {cache}|sort|python -m predictiveds.processing.core.hadoop.reducer {module} {class} {cache} > {data-root}/{output}",
          'dcache-directory' : '_cache',
          }
