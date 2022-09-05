'''
Created on 23 Jun 2022

@author: lukas
'''
import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = 'mp_progress_logger',
    version = '0.1',
    author = 'Lukas Deutz',
    author_email = 'scld@leeds.ac.uk',
    description = 'Package to log and display the progress of multiprocessing task queues',
    long_description = read('README.md'),
    url = 'https://github.com/LukasDeutz/mp-progress-logger.git',
    packages = ['mp_progress_logger']
)



                    