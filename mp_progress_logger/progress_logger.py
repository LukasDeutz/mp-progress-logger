'''
Created on 27 Aug 2022

@author: lukas
'''
# Build-in import
import logging
from logging.handlers import QueueHandler
import multiprocessing as mp
import itertools as it
import time

# Thid-party import
from tqdm import tqdm

class ProgressLogger():
    '''
    Class to log and display progressbars for multiprocessing.Pool class. 
    
    ProgressLogger initializes and configures a logger for the main-process and
    loggers for every worker-process. It initializes a top-level progressbar which displays the 
    perecentage of tasks which have been already finished. It also initializes a progressbar 
    for each worker-process which shows the progress of the task its currently working on.        
    '''    
    log_info_path = None
    log_err_path = None
        
    N_dash = 50             
                             
    def __init__(self, 
                 log_info_path, 
                 log_err_path):
        '''
        
        :param log_info_path (str): info log filepath
        :param log_err_path (str): error log filepath
        '''
 
        # log_info_path, and log_err_path need to be 
        # static variables to be accesible from within the static methods
        ProgressLogger.log_info_path  = log_info_path
        ProgressLogger.log_err_path = log_err_path 
            
        return
        
    @staticmethod
    def _init_worker(queue, lock, init_args, init_kwargs):
        '''
        Initializes a logger for each worker process in pool. Sets a lock to 
        protect progressbars in the terminal from being updated concurrently. 
        
        :param queue (mp.Queue): Logger queue
        :param lock (mp.RLock): Lock for progressbars
        :param init_args (list): argument list 
        :param init_kwargs (dict): keyword argument dictionary
        '''
         
        # init logger   
        global inner_logger 
        
        global worker_number         
        worker_number = mp.current_process()._identity[0] - 2 
        
        inner_logger = logging.getLogger(f'Worker {str(worker_number).zfill(2)}')        
        inner_logger.addHandler(logging.handlers.QueueHandler(queue))
        inner_logger.setLevel(logging.INFO)
        inner_logger.propagate = False
        
        # Set lock to avoid conflicts between parallel progress bars
        tqdm.set_lock(lock)
                
        global args
        args = init_args
        
        global kwargs
        kwargs = init_kwargs
                
        return
            
    @staticmethod
    def _init_output_loggers():
        '''
        Initializes and configures the output loggers which is used in the logger process.
        '''
                
        info_logger = logging.getLogger('Info')                        
        formatter = logging.Formatter('%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s')
         
        # Info logger logs progress
        info_handler = logging.FileHandler(ProgressLogger.log_info_path, 'w')
        info_handler.setFormatter(formatter)
        info_handler.setLevel(logging.INFO)
        info_logger.addHandler(info_handler)
        
        error_logger = logging.getLogger('Error')
                
        error_handler = logging.FileHandler(ProgressLogger.log_err_path, 'w')
        error_handler.setFormatter(formatter)
        error_handler.setLevel(logging.ERROR)
        error_logger.addHandler(error_handler)
                                        
        # StreamHandler can be used to log to the console
        # However, logging to the console will mess with
        # the progressbar and is thus disabled for now                  
        #c_handler = logging.StreamHandler()
        #c_handler.setFormatter(formatter)        
        #logger.addHandler(c_handler)
    
        error_logger.setLevel(logging.INFO)
              
        return info_logger, error_logger
           
    @staticmethod       
    def _start_logger_proccess():
        '''
        Creates queue and starts logger process
        '''
        
        # Create logger process. The logger process's job is to 
        # log the logs which arrive in the queue. The queue 
        # is necessary for inter-process communication because 
        # processes do not share memory
        manager = mp.Manager()
        queue = manager.Queue()                                    
        logger_process = mp.Process(target = ProgressLogger.log_workers, args = (queue, ))
        logger_process.start()
        
        return logger_process, queue
           
    @staticmethod       
    def _init_main_logger(queue):
        '''
        Initializes main-thread logger
        '''
                        
        # Initialize and configure main-thread logger                                            
        main_logger = logging.getLogger('main')
        main_logger.addHandler(logging.handlers.QueueHandler(queue))
        main_logger.setLevel(logging.INFO)
                                
        return main_logger
        
    @staticmethod       
    def log_workers(queue):
        '''
        Function which runs in the logger process. The logger process 
        waits for log records to be added to the queue by main or by 
        the worker processes and logs them using the output logger.
        
        After the worker pool has finished all of its jobs, a None item 
        is added to the queue which determinates the logger process. 
                                        
        :param queue (multiprocessing.Queue): record queue 
        '''
        
        info_logger, error_logger = ProgressLogger._init_output_loggers()
                
        while True:
            record = queue.get()
            if record is None:
                break
                                    
            if record.levelname == 'INFO':
                info_logger.handle(record)                
            elif record.levelname == 'ERROR':                        
                error_logger.handle(record)
                                                        
        return    
    
    @staticmethod
    def _log_exception(e, i):

        inner_logger.info(f'Task {i} failed!')                                                                                      
        inner_logger.error(f'Exception occured in task {i}:')                
        inner_logger.exception(str(e) + '\n')
        #inner_logger.error('')
        
        return
            
    @staticmethod
    def _task_wrapper(input_tuple):
        '''
        Wraps the task function. Logs the start, the end of the each task 
        and exceptions raised within the task function.
        
        :param input_tuple (tuple); (task i, _input)
                
            :param task (function): task function
            :param i (integer): task number
            :param _input: task function input
        '''               
        task, i, _input = input_tuple
                
        global inner_logger 
        global args
        global kwargs
                       
        pbar = tqdm(desc = f'TASK_{str(i).zfill(3)}:', position = worker_number, leave = False)

        inner_logger.info(f'Start task {i} ...')
                   
        try:                            
            task(_input, 
                 pbar, 
                 inner_logger,
                 i,                 
                 *args,
                 **kwargs)
        
        except Exception as e:              
            ProgressLogger._log_exception(e, i)                                                                                    
            exit_status = 1                    
        else:
            inner_logger.info(f'Finished task {i} ...')
            exit_status = 0
        finally:
            pbar.close()
        
        return exit_status 
              

    def _log_pool(self, main_logger, N_worker, N_jobs):
        '''
        Logs general informations at the start of the simulation.
        
        :param main_logger (logging.Logger): main-process logger
        :param N_worker: Number of worker-processes
        :param N_jobs: Number of tasks
        :param args: Additional positional arguments 
        :param kwargs: Additional keyword arguments        
        '''

        main_logger.info(f'Initialize worker pool and assign tasks') 
        main_logger.info(f'Number of workers: {N_worker}')
        main_logger.info(f'Total number of tasks: {N_jobs}')
                 
        return
    
    
    def _log_results(self, main_logger, results):
        
        for i, exit_status in enumerate(results):                        
            
            main_logger.info(f'Task {i}, exit status: {exit_status}')

        return

        
                                                  
    def run_pool(self, N_worker, task, inputs, *init_args, **init_kwargs):
        '''
        Creates a worker pool which executes the task function for all 
        parameter in inputs.
                                        
        :param N_worker (int): Number of worker processes
        :param task (func): task function 
        :param inputs (list): list of inputs
        :param *init_args: additional positional arguments which will be passed to the task function           
        :param **kwargs: additional keyword arguments which will be passed to the task function                        
        
        At the moment, only task functions are supported which take 4 fixed positional 
        arguments, an abitrary number of additional positional arguments and 
        an arbitrary number of additional additional keyword arguments. 
                
        task(input, pbar, logger, task_number, *args, **kwargs) 
        
        with input being one of the elements in inputs, 
        with pbar being a tqdm.tqdm progressbar object,
        with logger being a logging.logger object,
        and task_number being an integer
                
        Additional positional and keyword arguments are kept fixed, i.e. their values 
        will be the same for all tasks.                                                         
        '''
        N_tasks = len(inputs)

        # Start logger process
        logger_process, queue = self._start_logger_proccess()
                           
        # Initialize main logger                           
        main_logger = self._init_main_logger(queue)                        

        # Overwrite and customize if needs be  
        self._log_pool(main_logger, N_worker, N_tasks)
                        
        # Initialize the top-level progressbar .         
        # The lock prevents progressbars to be updated concurrently.
        lock = mp.RLock()
        tqdm.set_lock(lock)                                                                                    
        pbar = tqdm(total = N_tasks, desc = f'ALL TASKS:', position = 0, miniters = 1)
        
        # Create the pool and assign tasks
        pool = mp.Pool(N_worker, ProgressLogger._init_worker, initargs = (queue, lock, init_args, init_kwargs))
                                                                                                                                                
        results = []

        main_logger.info(f'Start work: ' + '-'*ProgressLogger.N_dash)
        
        # Do the work
        for exit_status in pool.imap(ProgressLogger._task_wrapper, zip(it.repeat(task), range(N_tasks), inputs)):
                            
            pbar.update(1)
            results.append(exit_status)
            time.sleep(0.1)
            
            
        # Can be overwritten and customized            
        main_logger.info(f'Results: ' + '-'*ProgressLogger.N_dash)
        
        # Overwrite and customize if needs be
        self._log_results(main_logger, results)
            
        pool.close()
        pbar.close()

        queue.put_nowait(None)                
        logger_process.join()
                
        return

    
    
        
        

    
    
    
