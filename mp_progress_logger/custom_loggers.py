'''
Created on 4 Sept 2022

@author: lukas
'''

from os import mkdir
from os import path
import numpy as np

from mp_progress_logger import ProgressLogger             
            
class PGProgressLogger(ProgressLogger):
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
                            
        outputs  = super().run_pool(N_worker, 
                                    task, 
                                    list(zip(self.PG.param_mask_arr, self.PG.hash_mask_arr)), 
                                    *init_args, 
                                    **init_kwargs)
                
        return outputs
                                        
    def _log_pool(self, main_logger, N_worker, N_jobs):
        
        super()._log_pool(main_logger, N_worker, N_jobs)
        
        log_dir = path.dirname(self.log_info_path)

        fp = self.PG.save(log_dir)
        
        main_logger.info(f'Experiment: {self.experiment_spec}')
        main_logger.info(f'Saved parameter grid dictionary to {fp}')
                                
        return
    
    
class FWException(Exception):
    
    def __init__(self, pic_arr, pic_rate, T, dt, t):
        '''
        
        :param pic_arr (np.array):
        :param pic_rate (float):        
        :param T (float):
        :param dt (float):        
        :param t (float):
        '''
        
        self.pic = pic_arr
        self.pic_rate = pic_rate
        self.T = T
        self.dt = dt
        self.t = t
        
        return    
    
    
class FWProgressLogger(PGProgressLogger):  
    '''
    Forward worm progress logger
    
    :param PGProgressLogger:
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

        super().__init__(PG, log_dir, experiment_spec)
        
        return
    
    def _log_results(self, main_logger, outputs):
                
        for i, output in enumerate(outputs):                        
        
            exit_status = output['exit_status']
            
            # If simulation has failed, log relevant information stored
            # in customized exception
            if isinstance(output['result'], Exception):
                e = output['result']
                pic_rate = e.pic_rate                
                t = e.t                
                T = e.T                 
                fstr = f"{{:.{len(str(e.dt).split('.')[-1])}f}}" 
                                
                main_logger.info(f'Task {i}, exit status: {exit_status}; PIC rate: {pic_rate}; simulation failed at t={fstr.format(t)}; expected simulation time was T={T}')
            # If simulation has finished succesfully, log relevant results            
            else:
                result = output['result']
                pic = result['pic']                
                pic_rate = np.sum(pic) / len(pic)                
                main_logger.info(f'Task {i}, exit status: {exit_status}; PIC rate: {pic_rate}')
                                                                                
        return

    
    
    
