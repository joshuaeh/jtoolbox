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

def check_if_in_h5(path, key):
    # check that file exists
    if not os.path.exists(path):
        return False
    with h5py.File(path, "r") as f:
        try:
            f[key]
            return True
        except KeyError:
            return False
        
# classes
class h5_logger:
    """Class to log data to an hdf5 file. The data is stored in datasets with the key being the name of the dataset."""
    # TODO: group logger?
    # TODO: assert that data values are numpy arrays /  handle scalars automatically
    # TODO: add buffer for more-efficient writing and writing when file is locked
    def __init__(self, filename, existing=True):
        self.filename = filename
        if not existing:
            with h5py.File(self.filename, "w") as file:
                file.attrs["datetime"] = str(datetime.datetime.now())

    def _maxshape(self, data):
        return (None,) + data.shape

    def _init_dataset(self, file, dataset_name, data):
        try:
            file.create_dataset(dataset_name, data=data[None], maxshape=self._maxshape(data))
        except BlockingIOError:
            logging.error("BlockingIOError: Retrying")
            time.sleep(1)
            file.create_dataset(dataset_name, data=data[None], maxshape=self._maxshape(data))

    def _append_to_dataset(self, file, dataset_name, data):
        try:
            file[dataset_name].resize((file[dataset_name].shape[0] + 1, *file[dataset_name].shape[1:]))
            file[dataset_name][-1] = data
        except BlockingIOError:
            logging.error("BlockingIOError: Retrying")
            time.sleep(1)
            file[dataset_name].resize((file[dataset_name].shape[0] + 1, *file[dataset_name].shape[1:]))
            file[dataset_name][-1] = data
            
    def _del_dataset(self, file, dataset_name):
        del file[dataset_name]
        
    def recursive_del(self, key):
        with h5py.File(self.filename, "r+") as file:
            for k in file[key].keys():
                if isinstance(file[key][k], h5py.Group):
                    self.recursive_del(f"{key}/{k}")
                else:
                    del file[key][k]
            del file[key]

    def log_attribute(self, key, value, replace=False):
        """Does not add an extra dimension, designed to be set once."""
        if check_if_in_h5(self.filename, key):
            if not replace:
                AttributeError(f"Key {key} already exists. Use replace=True to overwrite.")
            else:
                with h5py.File(self.filename, "a") as file:
                    del file[key]
                    file[key] = value
        else:        
            with h5py.File(self.filename, "a") as file:
                file[key] = value
        
    
    def log_value(self, data_key, data_value, file=None):
        if file is not None:
            if data_key not in file.keys():
                self._init_dataset(file, data_key, data_value)
            else:
                self._append_to_dataset(file, data_key, data_value)
        else:
            with h5py.File(self.filename, "a") as file:
                if data_key not in file.keys():
                    self._init_dataset(file, data_key, data_value)
                else:
                    self._append_to_dataset(file, data_key, data_value)

    def log_dict(self, data_dict):
        with h5py.File(self.filename, "a") as file:
            for key, value in data_dict.items():
                self.log_value(key, value, file=file)        

    def open_log(self):
        return h5py.File(self.filename, "a")

    def get_dataset(self, dataset_name):
        with h5py.File(self.filename, "r") as file:
            return file[dataset_name][:]

    def get_keys(self, *args):
        largs = len(args)
        assert largs <= 1, f"Expected 0 or 1 arguments, received {largs}"
        if len(args) == 0:
            with h5py.File(self.filename, "r") as file:
                return list(file.keys())
        else:
            with h5py.File(self.filename, "r") as file:
                return list(file[args[0]].keys())
            
        
    def get_group_keys(self, group):
        """depricated now"""
        # deprication warning
        logging.warning("h5_logger.get_group_keys() is depricated. Use h5_logger.get_keys() instead.")
        with h5py.File(self.filename, "r") as file:
            return list(file[group].keys())
        
    def get_multiple(self, given_keys):
        with h5py.File(self.filename, "r") as file:
            return {k: file[k][()] for k in given_keys}

    def get_group(self, group_name):
        with h5py.File(self.filename, "r") as file:
            results = {}
            for key in file[group_name].keys():
                if isinstance(file[group_name][key], h5py.Dataset):
                    results[key] = file[group_name][key][()]
                elif isinstance(file[group_name][key], h5py.Group):
                    results[key] = self.get_group(f"{group_name}/{key}")
            return results
    
    def check_key(self, key):
        return check_if_in_h5(self.filename, key)
    
    def get_group_keys(self, group):
        with h5py.File(self.filename, "r") as file:
            return list(file[group].keys())
        
    def rm_key(self, key):
        with h5py.File(self.filename, "r+") as file:
            del file[key]
            
    def move_key(self, key, new_key):
        with h5py.File(self.filename, "r+") as file:
            file.move(key, new_key)
            
    def move_group(self, source, destination):
        with h5py.File(self.filename, "r+") as file:
            file.move(source, destination)
            
    def get_unique_key(self, base_key):
        counter = 0
        key = base_key
        if base_key.endswith("/"):
            base_key = base_key[:-1]
            suffix = "/"
        else:
            suffix = ""
            
        while self.check_key(key):
            counter += 1
            key = f"{base_key}_{counter}{suffix}"
        return key
    
    def append_group_name(self, group_header, suffix=None):
        if suffix is None:
            suffix = "_"
        else:
            suffix = f"_{suffix}"
        if group_header.endswith("/"):
            header_parts = group_header.split("/")[:-1]
        else:
            header_parts = group_header.split("/")
        new_base_name = "/".join(header_parts) + suffix
        unique_base_name = self.get_unique_key(new_base_name)
        self.move_group(group_header, unique_base_name)