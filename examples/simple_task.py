from mp_progress_logger import ProgressLogger 
import numpy as np
import time

def dummy_task(fail_rate, pbar, logger, task_number):

    time.sleep(np.random.rand())

    logger.info(f'Hello from within task function: The task number is {task_number} and the failure rate is {np.round(fail_rate,2)}')
            
    np.random.seed()
    
    N = int(1e3)  
    pbar.total = N           
            
    for i in range(N):
    
        time.sleep(0.01)
        pbar.update(1)
                        
        if i == int(N/2):            
            # Exception gets logged in th wrapper function
            if np.random.rand() <= fail_rate:        
                assert False
                                    
    return 
    
if __name__ == '__main__':
        
    pl = ProgressLogger('./logs/simple_info.log', './logs/simple_err.log')
    
    fail_rates = np.linspace(0, 1, 10)
    N_worker = 4
                                        
    pl.run_pool(N_worker, dummy_task, fail_rates)



