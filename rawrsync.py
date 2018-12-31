#!/usr/bin/env python3

import argparse
import multiprocessing
import logging
import time

from copymanager import CopyManager

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def process_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', required=True)
    parser.add_argument('-d', '--destination', required=True)
    parser.add_argument(
        '-i', 
        '--interactive', 
        type=str2bool, 
        nargs='?',
        const=True, 
        default=False,
        help="Activate nice mode.")

    default_thread_count = multiprocessing.cpu_count() * 4
    parser.add_argument('-t', '--thread_count', required=False, default=default_thread_count)
    args = parser.parse_args()

    return args

if __name__ == '__main__':
    args = process_args()
    logging.basicConfig(
        filename='rawrsync.log',
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s:%(message)s'
    )
    start_time = time.time()
    with CopyManager(vars(args)) as copy_manager:
            results = copy_manager.process_transfer()
            if results is not None:
                for task_id, root_path, source_path, destination_path, change_time, status_change in results:
                    logging.warning('Remaining task [{0}] in state [{1}]'.format(source_path, status_change))

    end_time = time.time()
    execution_time = end_time - start_time
    msg = 'Ran in [{0:03.2f}] seconds'.format(execution_time)
    logging.info(msg)
    print(msg)
