# Python multiprocessing progress logger  

This is a python package to log and display the status and progress of a multiprocessing.Pool. A process pool consists of a fixed number of worker processes. Tasks which are assigned to the pool get queued and then distributed to free 
worker processes. 

The `mp_progress_logger.MPProgressLogger` class is designed to log the status and progress of the main process and the worker process into a shared logfile. It creates a top-level progressbar which displays the overall progress of the task queue. It also creates a progressbar for each individual worker process in the pool which is used to display the progress of tasks each worker is currently working on. 

#Installation

First, you need a python 3.x environment. The only third-party package required is tqdm which e.g. be installed via Conda. To run the `paramer_grid_pool.py` example, you need to install the `parameter_scan` python package which is available via [https://github.com/LukasDeutz/parameter-scan.git](https://github.com/LukasDeutz/parameter-scan.git).

The `mp_progress_logger` package can be installed in the active python environment using setup.py. From the mp-progress-logger package directory run

```bash
# The e option adds a symoblic link to the python package library 
pip install -e . 
```

#Usage

The main API is provided by the `MPProgressLogger` class in the `mp_progress_logger.py` module. The `util.py` module provides some additional helper functions to interface with the `parameter_scan` python package. Example use cases can be found in the `/example/directory. 


