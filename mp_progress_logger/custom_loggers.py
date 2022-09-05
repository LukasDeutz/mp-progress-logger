'''
Created on 4 Sept 2022

@author: lukas
'''

from os import mkdir
from os import path

from mp_progress_logger import ProgressLogger             
            
class ParameterGridLogger(ProgressLogger):
    '''
    Custom class to log status and progress of given task function. 
    The task function will be run for every parameter dictionary 
    in the given ParameterGrid instance.  
    '''

    def __init__(self, 
                 PG,
                 log_dir, 
                 experiment_spec = 'Not specified'):                                
        '''                
        :param PG (parameter_scan.ParameterGrid): 
        :param log_dir (str): directory for log files
        :param experiment_spec (str): experiment specification  
        '''

        if not path.isdir(log_dir): mkdir(log_dir)

        log_info_path = path.join(log_dir, PG.filename + '_info.log')    
        log_err_path =  path.join(log_dir, PG.filename + '_err.log')

        super().__init__(log_info_path, log_err_path)
        
        self.PG = PG
        self.experiment_spec = experiment_spec
        
        return

    def run_pool(self, N_worker, task, *init_args, **init_kwargs):
                            
        super().run_pool(N_worker, 
                         task, 
                         list(zip(self.PG.param_arr, 
                                  self.PG.hash_arr)), 
                         *init_args, 
                         **init_kwargs)
                
        return
                                        
    def _log_pool(self, main_logger, N_worker, N_jobs):
        
        super()._log_pool(main_logger, N_worker, N_jobs)
        
        log_dir = path.dirname(self.log_info_path)

        fp = self.PG.save(log_dir)
        
        main_logger.info(f'Experiment: {self.experiment_spec}')
        main_logger.info(f'Saved parameter grid dictionary to {fp}')
                                
        return
