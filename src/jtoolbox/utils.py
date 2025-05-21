import contextlib
import datetime
import logging
import time

import h5py
import joblib

@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument
    ARGS:
    tqdm_object: instance of tqdm reporting the progress
    RETURNS:
    Modified joblib.parallel.BatchCompletionCallBack to update the tqdm bar with the batch size
    
    EXAMPLE:
    ```python
    from math import sqrt
    from joblib import Parallel, delayed

    with tqdm_joblib(tqdm(desc="My calculation", total=10)) as progress_bar:
        Parallel(n_jobs=16)(delayed(sqrt)(i**2) for i in range(10))
    ```
    """
    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()
        
# constants
bytes_dict = {
    "B": 1,
    "KB": 1024,
    "MB": 1024**2,
    "GB": 1024**3,
    "TB": 1024**4,
}

# functions
def numpy_memory_size(numpy_array, units="MB"):
    """Get the memory size of a numpy array"""
    return numpy_array.nbytes / bytes_dict[units]