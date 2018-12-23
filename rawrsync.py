#!/usr/bin/env python3

import argparse
import time
import multiprocessing

from copymanager import CopyManager

def process_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', required=True)
    parser.add_argument('-d', '--destination', required=True)
    parser.add_argument('-i', '--source_input', required=False, default='found_dirs.csv')
    parser.add_argument('-r', '--recursion_depth', required=False, default=4)

    default_thread_count = multiprocessing.cpu_count() * 4
    parser.add_argument('-t', '--thread_count', required=False, default=default_thread_count)
    args = parser.parse_args()

    return args

if __name__ == "__main__":
    with CopyManager(vars(process_args())) as copy_manager:
        results = copy_manager.process_transfer()
        print('Completed')
