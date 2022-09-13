'''
Created on 27 Aug 2022

@author: lukas
'''
# Build-in import
from os import path
from os import mkdir
import logging
from logging.handlers import QueueHandler
import multiprocessing as mp
import itertools as it
import time
import io
import numpy as np

# Thid-party import
from tqdm import tqdm

class ProgressLogger():
    '''
    Class to log progress and display progressbars for multiprocessing.Pool class. 
    
    ProgressLogger initializes and configures a logger for the main-process and loggers 
    for every worker-process. It initializes a top-level progressbar which displays the 
    progress of of the taske queue, i.e. perecentage of already finished tasks. It also 
    initializes a progressbar for each worker-process displaying the progress of the 
    task each worker is currently working on.        
    '''    
    log_info_path = None
    log_err_path = None
    pbar_to_file = False 
    pbar_path = None 
    ncols = 75
                
    N_dash = 50             
                                             
    def __init__(self, 
                 log_info_path, 
                 log_err_path,
                 pbar_to_file = False,
                 pbar_path = './pbars/pbars.txt'):
        '''
        
        :param log_info_path (str): info log filepath
        :param log_err_path (str): error log filepath
        :param pbar_to_file (bool): if True, progressbars output will be written to file        
        :param pbar_path (str): pbar filepath
        '''
 
        # log_info_path, and log_err_path need to be 
        # static variables to be accesible from within the static methods
        ProgressLogger.log_info_path  = log_info_path
        ProgressLogger.log_err_path = log_err_path 
        ProgressLogger.pbar_to_file = pbar_to_file
        ProgressLogger.pbar_path = pbar_path

        self.has_pool = False
                        
        return
        
    @staticmethod
    def _init_worker(queue, lock, worker_counter, init_args, init_kwargs):
        '''
        Initializes a logger for each worker process in pool. Sets a lock to 
        protect progressbars in the terminal from being updated concurrently. 
        
        :param queue (mp.Queue): Logger queue
        :param lock (mp.RLock): Lock for progressbars
        :param pbar_to_file (bool):         
        :param init_args (list): argument list 
        :param init_kwargs (dict): keyword argument dictionary
        '''
         
        # init logger   
        global inner_logger 
        
        global worker_number         
        worker_number = worker_counter.value 
        worker_counter.value += 1
        
        inner_logger = logging.getLogger(f'Worker {str(worker_number).zfill(2)}')        
        inner_logger.addHandler(logging.handlers.QueueHandler(queue))
        inner_logger.setLevel(logging.INFO)
        inner_logger.propagate = False
        
        # Set lock to avoid conflicts between parallel progress bars
        tqdm.set_lock(lock)
                 
        if ProgressLogger.pbar_to_file:
            f = open(ProgressLogger.pbar_path, 'r+')
            writer = TqdmToFile(f, worker_number)
        else: 
            writer = None
                 
        global pbar_writer
        pbar_writer = writer
                                     
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
        
        print(f'Info Logger: {path.abspath(ProgressLogger.log_info_path)}')
        print(f'Error Logger: {path.abspath(ProgressLogger.log_err_path)}')
                        
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
        logger_process = mp.Process(target = ProgressLogger._log_workers, args = (queue, ))
        logger_process.start()
        
        return logger_process, queue
           
           
        
    @staticmethod       
    def _log_workers(queue):
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
        '''
        
        :param e (Exception): arbitrary exception
        :param i (int): task number
        '''
        
        inner_logger.info(f'Task {i} failed!')                                                                                      
        inner_logger.error(f'Exception occured in task {i}:')                
        inner_logger.exception(e)
        
        return
            
    @staticmethod
    def _task_wrapper(input_tuple):
        '''
        Wraps the task function. Logs the start, finish and exit status of 
        each task. Logs exceptions raised within the task function. 
        Equips every task with its on progressbar.
        
        :param input_tuple (tuple); (task i, _input)
                
            :param task (function): task function
            :param i (integer): task number
            :param _input: task function input
        '''               
        task, i, _input = input_tuple
                
        global inner_logger 
        global worker_number
        global args
        global kwargs
        global pbar_writer
        
        pbar = tqdm(desc = f'TASK__{str(i).zfill(3)}:', 
                    file = pbar_writer, 
                    position = worker_number, 
                    ncols = ProgressLogger.ncols,
                    leave = False)

        inner_logger.info(f'Start task {i} ...')
        
        output = {}
                   
        try:                            
            result = task(_input, 
                          pbar, 
                          inner_logger,
                          i,                 
                          *args,
                          **kwargs)
        
        except Exception as e:              
            ProgressLogger._log_exception(e, i)                                                                                                
            output['exit_status'] = 1
            output['result'] = e
        else:            
            inner_logger.info(f'Finished task {i} ...')
            output['exit_status'] = 0
            output['result'] = result
        finally:                                    
            pbar.close()
        
        return output
              
    def _init_main_logger(self):
        '''
        Initializes main-thread logger
        '''
                        
        # Initialize and configure main-thread logger                                            
        main_logger = logging.getLogger('main')
        main_logger.addHandler(logging.handlers.QueueHandler(self.queue))
        main_logger.setLevel(logging.INFO)
                                
        return main_logger
    
    def _init_pbar_writer(self, N_bars):
                
        pbar_dir = path.dirname(ProgressLogger.pbar_path)
        
        if not path.isdir(pbar_dir):
            mkdir(pbar_dir)
                        
        f = open(ProgressLogger.pbar_path, 'w')            
        f.writelines([' ' * TqdmToFile.max_char + '\n']*N_bars)

        writer = TqdmToFile(f, 0)                     
        
        return writer, f

    def _log_task_queue(self, N_worker, N_task):
        '''
        Logs general informations at the start of the simulation.
        
        :param N_worker: Number of worker-processes
        :param N_task: Number of tasks
        '''

        self.main_logger.info(f'Task Queue: ' + '-'*ProgressLogger.N_dash)
        self.main_logger.info(f'Initialize worker pool and assign tasks') 
        self.main_logger.info(f'Number of workers: {N_worker}')
        self.main_logger.info(f'Total number of tasks: {N_task}')
                 
        return
    
    
    def _log_results(self, outputs):
        '''
        Logs ouput information returned by tasks. The exit status indicates if 
        the task succeded (0) or if it failed (1). This function can be 
        overwritten to provide additional information about output of the task 
        function. 
        
        :param outputs (list): list of outputs returned task queue
        '''
        
        for i, output in enumerate(outputs):                        
            
            self.main_logger.info(f"Task {i}, exit status: {output['exit_status']}")

        return

    def init_pool(self, N_worker, init_args, init_kwargs):

        # Start logger process
        self.logger_process, self.queue = self._start_logger_proccess()
        # Initialize main logger                           
        self.main_logger = self._init_main_logger()                        

        # Initialize the top-level progressbar .         
        # The lock prevents progressbars to be updated concurrently.
        self.lock = mp.RLock()

        worker_counter = mp.Value('l', 1)

        # Create the pool and assign tasks
        self.pool = mp.Pool(N_worker, ProgressLogger._init_worker, initargs = (self.queue, self.lock, worker_counter, init_args, init_kwargs))
                                                                                  
        self.has_pool = True

        return
                                                                      
    def run_pool(self, N_worker, task, inputs, *init_args, **init_kwargs):
        '''
        Creates a worker pool which executes the task function for every 
        input in inputs.
                                        
        :param N_worker (int): Number of worker processes
        :param task (func): task function 
        :param inputs (list): list of inputs
        :param close (bool): If true, pool and logger process are closed        
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
        
        if not self.has_pool:
            self.init_pool(N_worker, init_args, init_kwargs)
                
        N_tasks = len(inputs)

        tqdm.set_lock(self.lock)                                                                                    
                
        # If true, pbar will be written to file
        if self.pbar_to_file:
            pbar_writer, pbar_file = self._init_pbar_writer(N_worker + 1)
        # Otherwise, pbar is directed to standard output stream
        else: 
            pbar_writer = None
            pbar_file = None
            
        pbar = tqdm(total = N_tasks, 
                    desc = f'ALL TASKS:', 
                    file = pbar_writer, 
                    position = 0, 
                    miniters = 1,
                    ncols = ProgressLogger.ncols)
        
        pbar.refresh()
                        
        # Overwrite and customize to provide problem specific information
        self._log_task_queue(N_worker, N_tasks)
                                                                                                                                                                                                        
        outputs = []

        self.main_logger.info(f'Start work: ' + '-'*ProgressLogger.N_dash)
        
        # Do the work    
        for output in self.pool.imap(ProgressLogger._task_wrapper, zip(it.repeat(task), range(N_tasks), inputs)):
                            
            pbar.update(1)
            outputs.append(output)
            time.sleep(0.1)
                        
        self.main_logger.info(f'Results: ' + '-'*ProgressLogger.N_dash)
        
        # Overwrite and customize to provide problem specific information
        self._log_results(outputs)
        
        # Close progressbar
        pbar.close()
        if pbar_file is not None: pbar_file.close()
                
        return outputs

    def close(self):
        '''
        Clean up: Closes pool and logger process.
        '''

        self.pool.close()
        self.queue.put_nowait(None)                
        self.logger_process.join()
        
        self.has_pool = False

        return

class TqdmToFile(io.StringIO):
    '''
    Redirects progressbar updates to given file. The progressbar 
    is written to specified line number. This is useful if we want
    to write multiple progressbars to the same line
    '''
    
    buf = ''
    max_char = 200
        
    def __init__(self, file, line = 0):
        '''                    
        :param file (TextIOWrapper): progressbar log file 
        :param position (int): progressbar get written to specified line 
        '''
        
        self.file = file
        self.position = line*(TqdmToFile.max_char + 1)
        self._comparable = True
        
        super(TqdmToFile, self).__init__()
    
    def write(self, buf):        
        self.buf = buf.strip('\r\n\t ')                        
                
    def flush(self):                
        if self.buf.startswith('TASK') or self.buf.startswith('ALL'):        
            self.file.seek(self.position)
            self.file.write(self.buf)
    
    
    
    
        
        

    
    
    
