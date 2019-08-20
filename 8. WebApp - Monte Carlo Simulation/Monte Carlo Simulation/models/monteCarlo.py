import pandas as pd
import numpy as np
import random

# create: Monte Carlo Simulator
def monteCarlo_simulation(number_of_samples, avg, std):
    
    list_monteCarlo_simulation = []
    
    for i in range(number_of_samples):
        list_monteCarlo_simulation.append(random.gauss(avg,std))
    
    return list_monteCarlo_simulation

# create: buckets out of distribution
def monteCarlo_distribution(distribution_array):
    """
    Notes (3):
    1) np.hist returns a async length of lists:
        v[0] is freq at length=l
        v[1] is buckets at length=l+1
    
    2) This func to make equal lengths:
        ['buckets'][0] will be removed
      
    """
    
    list_monteCarlo_distribution = []

    v = np.histogram(distribution_array, 
                     bins=100)  
        
    for i in range(len(v[0])):

        dict_bucket = {
            "frequency": [],
            "bucket": []
        }

        dict_bucket["frequency"] = float(v[0][i])
        dict_bucket["bucket"] = float(v[1][i+1])
        
        list_monteCarlo_distribution.append(dict_bucket)
        
    return list_monteCarlo_distribution

def monteCarlo_list(distribution_array):
    
    list_sorted = sorted(distribution_array)
    monteCarlo_list = []
    
    for i in range(len(list_sorted)):
        dict_full = {
            "index": [],
            "return": []
        }
        
        dict_full["index"] = i
        dict_full["return"] = float(list_sorted[i])
        
        monteCarlo_list.append(dict_full)

    return monteCarlo_list