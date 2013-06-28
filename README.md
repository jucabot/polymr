#Polymr|Python Polymorphic MapReduce
BSD License

##What the fuck?
In the field of Big data analytics and data science, **some dataset are huge, some are not so big** (many Mb or Gb).
From raw data extractraction to predictive modelling the dataset size and computating requirements are volatile.
Hadoop or Spark are incredible platform to compute large dataset but overkill for Mb-Gb datasets

##What is Polymr?
Polymr provides a MapReduce abstraction over 3 MapReduce engines :
* **In-memory Single process MapReduce** engine for real debugging and dummy maps (ex counting, mean, median, frequency distribution, etc...).
* **In-memory Multi processor MapReduce** engine for mid sized (many Gb) datasets with computational tasks, ex RandomForest, NLP, etc
* **Hadoop streaming** interface for large data set - currently local simulation interface

**Use the same code, run on different MapReduce engines** to reach the best performance and cost.

Polymr has 2 types input/output providers :
* FileInputReader , FileOutputReader : Sequence file reader and writer - the job reads or writes data from or to a file
* MemInputReader, MemOutputWriter : Sequence file reader and writer - the job reads or writes data from or to memory for aggregate or small data pipeline
You may mix the providers into a MR job

Polymr provides a **profiler to identify the most performant** (time and cost) MapReduce engine, based on the local DRAM limitation and the map function mean duration while analyzing a sample of the dataset (1000 lines)

#Usage
todo - see test.py

#Installation
Run on linux only (env var), python 2.7 (may be 2.5 not tested)

* Dependency : cjson - pip install cjson

#Benchmark
todo
