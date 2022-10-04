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
                 pbar_to_file = False,
                 pbar_path = './pbars/pbars.txt',
                 task_spec = 'Not specified',
                 debug = False):
        '''        
        :param PG (parameter_scan.ParameterGrid): 
        :param log_dir (str): directory for log files
        :param pbar_to_file (bool): If true, progressbars are outputted to file 
        :param pbar_path (str): Progressbar file path        
        :param debug (bool): If True, then error handling is deactivated                
        '''

        if not path.isdir(log_dir): mkdir(log_dir)

        log_info_path = path.join(log_dir, PG.filename + '_info.log')    
        log_err_path =  path.join(log_dir, PG.filename + '_err.log')

        super().__init__(log_info_path, log_err_path, pbar_to_file, pbar_path, debug)
        
        self.PG = PG
        self.task_spec = task_spec
        
        return

    def run_pool(self, N_worker, task, *init_args, **init_kwargs):
                                       
        inputs = list(zip(self.PG.param_mask_arr, self.PG.hash_mask_arr))
                            
        outputs  = super().run_pool(N_worker, task, inputs, *init_args, **init_kwargs)
                
        return outputs
                                        
    def _log_task_queue(self, N_worker, N_task):
        
        super()._log_task_queue(N_worker, N_task)
        
        log_dir = path.dirname(self.log_info_path)

        fp = self.PG.save(log_dir)
        
        self.main_logger.info(f'Experiment: {self.task_spec}')
        self.main_logger.info(f'Saved parameter grid dictionary to {fp}')
                                
        return
        
class FWProgressLogger(PGProgressLogger):  
    '''
    Forward worm progress logger
    
    :param PGProgressLogger:
    '''

    def __init__(self, 
                 PG,
                 log_dir, 
                 pbar_to_file = False,
                 pbar_path = './pbars/pbars.txt',
                 exper_spec = 'Not specified:',
                 debug = False):                                

        '''
        :param PG (parameter_scan.ParameterGrid): 
        :param log_dir (str): directory for log files
        :param pbar_to_file (bool): If true, progressbars are outputted to file 
        :param pbar_path (str): Progressbar file path        
        :param exper_spec (str): Experiment specificition        
        :param debug (bool): If True, then error handling is deactivated                        
        '''             
        super().__init__(PG, log_dir, pbar_to_file, pbar_path, exper_spec, debug)
                                
        return
        
    def run_pool(self, N_worker, task, *init_args, **init_kwargs):
        
        self.N =  self.PG.base_parameter['N']
        self.dt = self.PG.base_parameter['dt']
        
        return PGProgressLogger.run_pool(self, N_worker, task, *init_args, **init_kwargs)
        
    def iterative_run_pool(self, N_worker, task, N_arr, dt_arr, *init_args, **init_kwargs):
        
        self.init_pool(N_worker, init_args, init_kwargs)
                  
        self.PG.base_parameter['N'] = N_arr[0]
        self.PG.base_parameter['dt'] = dt_arr[0]
        
        self.N = N_arr[0]
        self.dt = dt_arr[0]
                               
        i = 1
        N_iter = len(N_arr)
     
        while True:
                                    
            outputs = super().run_pool(N_worker, task)                                
            exit_status_list = [output['exit_status'] for output in outputs]            
            # Get task indices of tasks which failed
            idx_arr = np.array(exit_status_list) == 1
            
            # If all simulations succeeded, break out of while loop             
            if np.all(np.logical_not(idx_arr)):
                exit_status = 0
                break

            # If the maximum resolution has been tryed, break out of while loop            
            if i == N_iter:
                exit_status = 1
                break
                        
            # Increase spatial and temporal resolution for grid points
            # in the parameter grid for which the simulation failed
            hash_mask_arr = np.array(self.PG.hash_mask_arr)[idx_arr]      
            self.PG.apply_mask(hash_mask_arr, N = N_arr[i], dt = dt_arr[i])            
            self.N, self.dt = N_arr[i], dt_arr[i]
            
            i += 1

        log_dir = path.dirname(self.log_info_path)                
        fp = self.PG.save(log_dir)
        
        self.main_logger.info(f'Finished iterative task Queue: ' + '-'*ProgressLogger.N_dash)        
        self.main_logger.info(f'Exit status: {exit_status}')        
        self.main_logger.info(f'Saved parameter grid dictionary to {fp}')
         
        self.close()
            
        return 
    
    def _log_task_queue(self, N_worker, N_task):
        
        # Call ProgressLoggers method directly
        ProgressLogger._log_task_queue(self, N_worker, N_task)
                
        self.main_logger.info(f'Experiment: {self.task_spec}')
        self.main_logger.info(f'Resolution: N={self.N}, dt={self.dt}')        
        
        log_dir = path.dirname(self.log_info_path)
        fp = self.PG.save(log_dir)
        
        self.main_logger.info(f'Saved parameter grid dictionary to {fp}')
                                
        return
        
    def _log_results(self, outputs):
                
        for i, output in enumerate(outputs):                        
        
            exit_status = output['exit_status']
            
            # If simulation has failed, log relevant information stored
            # in customized exception
            if isinstance(output['result'], Exception):
                e = output['result']
                pic_rate = np.sum(e.pic) / len(e.pic)                
                t = e.t
                T = e.T                 
                fstr = f"{{:.{len(str(e.dt).split('.')[-1])}f}}" 
                                
                self.main_logger.info(f'Task {i}, exit status: {exit_status}; PIC rate: {pic_rate}; simulation failed at t={fstr.format(t)}; expected simulation time was T={T}')
            # If simulation has finished succesfully, log relevant results            
            else:
                result = output['result']
                pic = result['pic']                
                pic_rate = np.sum(pic) / len(pic)                
                self.main_logger.info(f'Task {i}, exit status: {exit_status}; PIC rate: {pic_rate}')
                                                                                
        return

class FWException(Exception):
    
    def __init__(self, pic, T, dt, t):
        '''        
        :param pic (np.array):
        :param pic (np.array):
        
        :param T (float):
        :param dt (float):        
        :param t (float):
        '''
        
        self.pic = pic
        self.T = T
        self.dt = dt
        self.t = t
        
        return        
            
    def __str__(self):
    
        if len(self.pic) > 10: pic = self.pic[-10:]
        else: pic = self.pic
    
        return f'FWException("PIC: {pic}", "T={self.T}", t={self.t}, dt={self.dt})'        


        
    
    
    
    
