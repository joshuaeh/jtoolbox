"""Module for logging data to an hdf5 file."""

import datetime
import logging
import os
import time
import warnings

import h5py
import numpy as np

logger = logging.getLogger(__name__)


class H5Logger:
    """Class to log data to an hdf5 file. The data is stored in datasets with the key being the name of the dataset."""

    # TODO: group logger?
    # TODO: removing key
    # TODO: handle blocking IO errors more gracefully
    def __init__(self, filename, overwrite=False, **kwargs):
        """Initialize the logger.
        Args:
            filename (path-like): The path to the file to log to.
            overwrite (bool): If True, overwrite an existing file. If False, append to an existing file (if one exists) (default: False).
        """
        self.filename = filename
        # check if file exists, create if desired
        if not os.path.exists(self.filename) or overwrite:
            with h5py.File(self.filename, "w") as file:
                pass

        # if existing is a kwarg, warn that it is depricated
        if "existing" in kwargs.keys():
            warnings.warn(
                "The 'existing' parameter is deprecated. Use 'overwrite' instead.",
                DeprecationWarning,
            )

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
        # TODO: change this to be the `.attrs` property of a group or dataset
        if self.check_key(key):
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
        if not isinstance(data_value, np.ndarray):
            data_value = np.array([data_value])
            logger.debug(f"Converted data_value in {data_key} to numpy array")
        elif data_value.ndim == 0:
            data_value = np.array([data_value])
            logger.debug(f"Converted 0-dim ndarray in {data_key} to 1-d numpy array")
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

    def open_log(self, open_type="a"):
        return h5py.File(self.filename, open_type)

    def get_dataset(self, dataset_name):
        try:
            with h5py.File(self.filename, "r") as file:
                if not file[dataset_name].shape:
                    return file[dataset_name][()]
                else:
                    return file[dataset_name][:]
        except KeyError:
            logging.error(f"KeyError: Dataset {dataset_name} not found.")
            return None

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
        logger.warning("h5_logger.get_group_keys() is depricated. Use h5_logger.get_keys() instead.")
        with h5py.File(self.filename, "r") as file:
            return list(file[group].keys())

    def get_attributes(self, group):
        """get all attributes of a group as a dictionary"""
        NotImplementedError("get_attributes() is not implemented yet.")

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
        with h5py.File(self.filename, "r") as file:
            try:
                file[key]
                return True
            except KeyError:
                return False

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
