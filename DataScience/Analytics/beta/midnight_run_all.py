# -*- coding: utf-8 -*-
"""
Created on Wed May 10 11:37:44 2017

:author: ivyONS

script to automatically run baselines on most of the datasets during the night

requires sami's scripts run_baseline & check_performance available for import (in the same directory)

"""

from shutil import copy
import os
import time
from run_baseline import run_all_baselines
from check_performance import main as check_performance
from default_param import DEFAULT_CONFIG

tdata_path = '//tdata8/AddressIndex/Beta_Results/'

uri_version = 'test'          # currently accepting strings: 'test' or 'branch'
new_folder_name =  time.strftime("%B_%d_") +  uri_version  + '_unitdebug'     # change the explanatory name !
wait_hours = 0                  # wait 5 hours before firing the queries 

datasets=['EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC', 'PatientRecords', 'WelshGov'] 
#or just the big ones:  datasets=[ 'PatientRecords', 'WelshGov']#, 'CompaniesHouse']                    


def main(datasets=datasets, new_folder_name= new_folder_name, tdata_path=tdata_path):
    print('Current time: ' + time.strftime("%H:%M:%S"))
    time.sleep(60*60*wait_hours)   
    report = dict([[dataset, 'failed'] for dataset in datasets]) 
    for dataset in datasets:
        print('Starting dataset ' + dataset + ' at ' + time.strftime("%H:%M:%S"))
        new_folder_path = tdata_path + dataset + '/'+ new_folder_name
        if not os.path.exists(new_folder_path): 
            # if the destination doesn't exist: create new folder and copy data (_minimal.csv) 
            os.makedirs(new_folder_path)            
            copy(tdata_path + 'DataSets/' + dataset + '_minimal.csv', new_folder_path)  
        try: 
            # try running the baseline scripts            
            run_all_baselines(directory = new_folder_path, uri_version = uri_version, batch_size=6000, 
                              param_config = DEFAULT_CONFIG, matchthreshold = 0)
            check_performance(directory = new_folder_path, ivy = True)
            report[dataset] = 'successful'
        except:
            pass
    print('Finished at ' + time.strftime("%H:%M:%S"))
    for dataset in datasets:
        print(dataset + ': ' + report[dataset])

if __name__ == '__main__':
    main()