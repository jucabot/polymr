from polymr.mapreduce import MapReduce
from polymr.functions.type_analyzer import TypeConverter, TEXT_TYPE
import numpy as np
from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import ExtraTreesClassifier,ExtraTreesRegressor
from numpy.ma.core import mean

class FeatureSet(object):
    metadata = None
    
    def __init__(self,metadata):
        self.metadata = metadata
    
    def get_feature_names(self):
        return self.metadata.keys()
    
    def get_feature(self,name):
        return self.metadata[name]
    
    def get_dataset(self,iterator,target_name=None):
        
        feature_names = self.get_feature_names()
        X = []
        y=[]
        converter = TypeConverter()
        for row_value in iterator:
            row = {}
            for name in feature_names:
                if name == target_name:
                    y.append(converter.cast(self.metadata[name]['type'],row_value[name]))
                else:
                    row[name] = converter.cast(self.metadata[name]['type'],row_value[name])
            X.append(row)

        
        vec = DictVectorizer()
        dataset = vec.fit_transform(X)
        feature_names = vec.get_feature_names()
        return y, dataset.toarray(), feature_names
        
    
    def get_table(self,iterator,target_name=None):
        
        feature_names = self.get_feature_names()

        X = []
        y=[]
        converter = TypeConverter()
        for row_value in iterator:
            row = []
            for name in feature_names:
                if name == target_name:
                    y.append(converter.cast(self.metadata[name]['type'],row_value[name]))
                else:
                    row.append(converter.cast(self.metadata[name]['type'],row_value[name]))
            X.append(row)
        return np.array(X),np.array(y)

class FieldMapReduce(MapReduce):
    """
    Compute Frequency distribution for each column/field
    """ 
   
    def _select_fields(self,row):
        for name, value in row.items():    
            yield (name ,value)

    def map(self, row):
        return self._select_fields(row)

class FieldFrequency(FieldMapReduce):
    """
    Compute Frequency distribution for each column/field
    """ 
   
   
    
    def _freq_count(self,values):
        freqs = {}
        for value in values:
            try:
                freqs[unicode(value)] += 1
            except KeyError:
                freqs[unicode(value)] = 1
        
        return freqs
    
    def _freq_merge(self,freq_values):
        freqs = {}
        for freq in freq_values:
            for value,count in freq.items():
                try:
                    freqs[unicode(value)] += count
                except KeyError:
                    freqs[unicode(value)] = count

        return freqs

    
    def combine(self, key, values):
        return (key,self._freq_count(values))

    def reduce(self, key, values):
        return (key, self._freq_merge(values))


def centile_by_frequency(frequency,value_count,centile=0.5):

    #sort frequency list by value
    frequency = sorted(frequency.iteritems(), key=lambda (k,v): float(k))
    limit = int(centile * value_count)
    i=0
    for (value, freq) in frequency:
        i += freq
        
        if i >= limit:
            return value
             
    return value
    
    

class FieldSummary(FieldFrequency):
        
    def combine(self, key, values):
        resume = {}
        tc = TypeConverter()
        
        #type data
        values = map(tc.type,values)
        
        resume['num-values'] = len(values)
        resume['frequency'] = self._freq_count(values)
        try:
            resume['N/A'] = resume['frequency']['']
            del resume['frequency']['']
        except KeyError:
            resume['N/A'] = 0
             
        resume['type'] = tc.get_type(values)
        
        if resume['type'] == 'int' or resume['type'] == 'float':
            defined_values = filter(lambda v : v != '',values)
            resume['min'] = min(defined_values)
            resume['max'] = max(defined_values)
            resume['sum'] = sum(defined_values)
        
        return (key,resume)

    def _merge_resume(self,resumes):
        global_resume = {}
        
        global_resume['frequency'] = {}
        global_resume['N/A'] = 0
        global_resume['num-values'] = 0
        global_resume['num-defined-values'] = 0
        global_resume['num-unique-values'] = 0
        global_resume['type'] = None
        
        for resume in resumes:
            global_resume['N/A'] += resume['N/A']
            global_resume['type'] = resume['type'] if resume['type'] != TEXT_TYPE else TEXT_TYPE 
            global_resume['num-values'] += resume['num-values']        
                
            #merge frequencies
            for value,count in resume['frequency'].items():
                try:
                    global_resume['frequency'][value] += count
                except KeyError:
                    global_resume['frequency'][value] = count


        global_resume['is-sparse'] = global_resume['N/A'] > 0
        global_resume['num-defined-values'] = global_resume['num-values'] - global_resume['N/A']
        
        global_resume['num-unique-values'] = len(global_resume['frequency'].keys())
        
        sorted_fd = sorted(global_resume['frequency'].iteritems(), key=lambda (k,v): v*-1)
        common_value, common_rank = sorted_fd[0]
        global_resume['common-value'] = common_value
        
        global_resume['is-factor'] = (1.0 - (float(global_resume['num-unique-values']) / float(global_resume['num-values']))) > 0.99
        
        if global_resume['type'] == 'int' or global_resume['type'] == 'float':
            min_values = map(lambda r : r['min'],resumes)
            max_values = map(lambda r : r['max'],resumes)
            sum_values = map(lambda r : r['sum'],resumes)
            
            global_resume['min'] = min(min_values)
            global_resume['max'] = max(max_values)
            global_resume['mean'] = sum(sum_values) / float(global_resume['num-values'] - global_resume['N/A'] )
            global_resume['1%-centile'] = centile_by_frequency(global_resume['frequency'], global_resume['num-defined-values'], centile=.01)
            global_resume['25%-centile'] = centile_by_frequency(global_resume['frequency'], global_resume['num-defined-values'], centile=.25)
            global_resume['50%-centile'] = centile_by_frequency(global_resume['frequency'], global_resume['num-defined-values'], centile=.5)
            global_resume['75%-centile'] = centile_by_frequency(global_resume['frequency'], global_resume['num-defined-values'], centile=.75)
            global_resume['99%-centile'] = centile_by_frequency(global_resume['frequency'], global_resume['num-defined-values'], centile=.99)
        
        
        
        return global_resume
        

    def reduce(self, key, values):
        return (key, self._merge_resume(values))


class FeatureSelector(MapReduce):
    
    
    def set_target(self,target):
        self.params['target-field'] = target
    def get_target(self):
        return self.params['target-field']
        
    def set_featureset(self,featureset):
        self.params['featureset'] = featureset
    
    def get_featureset(self):
        return self.params['featureset']
    
    
    def map_partition(self, iterator):
        target_name = self.get_target()
        featureset = FeatureSet(self.get_featureset())
        y,X,feature_names = featureset.get_dataset(iterator,target_name=target_name)
        
        
        clf = ExtraTreesClassifier(compute_importances=True)
        
        if featureset.get_feature(target_name)['is-factor']:
            clf = ExtraTreesClassifier(compute_importances=True)
        else:
            clf = ExtraTreesRegressor(compute_importances=True)
    
        clf.fit(X,y)
        feature_importance = clf.feature_importances_
        for i in range(len(feature_names)):
            yield (feature_names[i],feature_importance[i])
                
    def combine(self, key, values):
        return (key, mean(values))
        
    def reduce(self, key, values):
        return (key, mean(values))
    