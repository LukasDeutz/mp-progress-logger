import time
from os import path
import pickle
import numpy as np
import matplotlib.pyplot as plt

from mp_progress_logger import ParameterGridLogger
from parameter_scan import ParameterGrid 
from parameter_scan.util import load_file_grid

def dummy_base_parameter():

    base_parameter = {}
    
    # These are all simulation parameters
    base_parameter['a'] = 1.0
    base_parameter['b'] = 2.0
    base_parameter['c'] = 3.0
    base_parameter['d'] = 0.0
    
    return base_parameter

def example_2d_grid():

    base_parameter = dummy_base_parameter()

    # These are the parameters we want to vary
    a_param = {'v_min': -0.5, 'v_max': 0.5, 'N': 5, 'step': None, 'round': 1}
    b_param = {'v_min': -0.5, 'v_max': 0.5, 'N': 5, 'step': None, 'round': 1}

    grid_param = {'a': a_param, 'b': b_param}
    
    PG = ParameterGrid(base_parameter, grid_param)

    return PG

def dummy_task(_input, pbar, logger, task_number, output_dir = 'simulations', overwrite = True):
    
    
    param_dict = _input[0]
    hash = _input[1]
    
    file_path = path.join(output_dir, hash + '.dat')
    
    if path.isfile(file_path):
        if not overwrite:
            logger.info(f'Output file already exists: {file_path}')
            return
            
    M = 100
    pbar.total = M
    
    success = True # success status 
    fail_rate = 0.25 # fail rate                

    y_arr = np.zeros(M)
    x_arr = np.linspace(-1, 1, M)

    try:                                
        np.random.seed()        
        time.sleep(np.random.rand())
                        
        # Dummy task
        for i, x in enumerate(x_arr):
            
            y_arr[i] = param_dict['a']*x**3 + param_dict['b']*x**2 + param_dict['c']*x + param_dict['d'] 
            
            time.sleep(0.05)
            pbar.update(1)
            
            # Something went wrong, raise error
            if i == int(M/2):            
                if np.random.rand() < fail_rate:                                    
                    assert False    
    
    except Exception:                                    
            success = False
            # Reraise exception to pass it upstream to the wrapper function
            raise 
        
    finally:    
        
        with open(file_path, 'wb') as f:
            
            # Write output to file
            output = {}
            output['success'] = success
            output['y_arr'] = y_arr            
            output['x_arr'] = x_arr
            
            pickle.dump(output, f)        
    return
    
def plot_output(PG, output_dir):
                                
    file_grid = load_file_grid(PG.hash_grid, output_dir)

    for data, _hash in zip(file_grid.flatten(), PG.hash_arr):
        
        if data['success']:
        
            plt.plot(data['x_arr'], data['y_arr'], label = _hash)
            
    plt.legend()
    
    plt.show()
        
    return

if __name__ == '__main__':

    N_worker = 4    
    log_dir = './logs'
    output_dir = './simulations'
                       
    PG = example_2d_grid()


    PGL = ParameterGridLogger(PG,log_dir, experiment_spec = 'Dummy experiment')
                        
    PGL.run_pool(N_worker, 
                 dummy_task, 
                 output_dir = output_dir, 
                 overwrite = True)
                    
    plot_output(PG, output_dir)
    