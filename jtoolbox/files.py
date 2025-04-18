import os
from pathlib import Path
import re
import shutil
import sys

def rm_substring(dir, substring):
    root = Path(dir)
    counter = 0
    for path in root.glob("**/*"+substring+"*"):
        if not os.path.exists(path.with_name(path.name.replace(substring, ""))):
            try:
                path.rename(path.with_name(path.name.replace(substring, "")))
            except Exception as e:
                print(f"Error: {e}")
            counter += 1
    return counter

def rename_dirs(root, remove_text=None, suffix=""):
    for path in Path(root).rglob("*"):
        if path.is_dir():
            if remove_text is not None:
                if not os.path.exists(path.with_name(path.name.replace(remove_text, "").strip() + suffix)):
                    path.rename(path.with_name(path.name.replace(remove_text, "").strip() + suffix))
                else:
                    print(f"Directory {path} already exists")
            else:
                if not os.path.exists(path.with_name(path.name.strip() + suffix)):
                    path.rename(path.with_name(path.name.strip() + suffix))
                else:
                    print(f"Directory {path} already exists")

def rename_files_with_pattern(dir_path, pattern):
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            ext = os.path.splitext(file)[1]
            digits = re.findall(pattern, file)
            if len(digits) > 0:
                new_name = digits[0] + ext
                old_path = os.path.join(root, file)
                new_path = os.path.join(root, new_name)
                if not os.path.exists(new_path):
                    os.rename(old_path, new_path)
                    # print(f"Renamed {old_path} to {new_path}")
                else:
                    print(f"File {new_path} already exists")

def get_unique_filename(filepath):
    name, ext = os.path.splitext(filepath)
    suffix = 0
    if os.path.exists(filepath):
        while True:
            suffix += 1
            filepath = f"{name}({suffix}).{ext}"
            if not os.path.exists(filepath):
                return filepath
    else:
        return filepath

def check_duplicate(flie1, file2):
    """check duplicates using phash"""
    

def safe_rename(old_fpath, new_fpath, overwrite=False, check_duplicate=True):
    """rename. """
    # check if duplicate if exists
    new_fpath = get_unique_filename(new_fpath)
    shutil.move(old_fpath, new_fpath)
    return

def remove_empty_dirs(dir_path):
    for root, dirs, files in os.walk(dir_path):
        if not dirs and not files:
            os.rmdir(root)
                    
def move_files_to_top(dir_path, del_empty_dirs=False, duplicate_suffix=True):
    """move all files in subdirectories to the top level directory"""
    for root, dirs, files in os.walk(dir_path):
        if root == dir_path:
            dirs0 = dirs
            continue
        for file in files:
            old_path = os.path.join(root, file)
            new_path = os.path.join(dir_path, file)
            if not os.path.exists(new_path):
                shutil.move(old_path, new_path)
                # print(f"Moved {old_path} to {new_path}")
            elif duplicate_suffix:
                new_path = get_unique_filename(new_path)
            else:
                print(f"File {new_path} already exists")
                shutil.move(old_path, new_path)
    dirs0 = [d for d in os.listdir(dir_path) if os.path.isdir(os.path.join(dir_path, d))]
    if del_empty_dirs:
        for d in dirs0:
            # try:
            #     os.rmdir(os.path.join(dir_path, d))
            #     print(f"Deleted directory {d}")
            # except:
            #     print(f"Directory {d} not empty")
            shutil.rmtree(os.path.join(dir_path, d))
            
def get_tree_size(path):
    """Return total size of files in given path and subdirs."""
    total = 0
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            total += get_tree_size(entry.path)
        else:
            total += entry.stat(follow_symlinks=False).st_size
    return total