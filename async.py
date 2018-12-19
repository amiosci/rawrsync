#!/usr/bin/env python3

import os
import argparse
import asyncio
import time
from itertools import chain, groupby
from pathlib import PurePath
from collections import namedtuple
import multiprocessing
import fileinput
import atexit

# Data objects
DirectoryResult = namedtuple('DirectoryResult','source target status')

# def save_progress():
#     print('saving results')

process_results = list()
input_file = 'found_dirs.csv'

def process_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--source', required=True)
    parser.add_argument('-d', '--destination', required=True)
    # parser.add_argument('-i', '--source_input', required=False, default='found_dirs.csv')
    parser.add_argument('-r', '--recursion_depth', required=False, default=4)
    parser.add_argument('-t', '--thread_count', required=False, default=512)
    args = parser.parse_args()

    return args

async def worker(name, queue):
    while True:
        root_dir, from_dir, copy_to = await queue.get()
        result = await process_dir(root_dir, from_dir, copy_to)
        queue.task_done()
        process_results.append((from_dir, copy_to, result["status"]))
        # persist_dir(from_dir, copy_to, result["status"], input_file)
        print(f'Processed {from_dir} with result {result["status"]}')

# asyncio.sendfile
async def copy_dir(root_dir, from_dir, to_dir):
    # --bwlimit=RATE
    # --list-only
    # --progress
    # --compress
    # --dry-run
    # --super
    # --preallocate 
    
    # updates the path to trim the parent dir
    from_path = from_dir.replace(root_dir, f'{root_dir}/.')
    to_parent = from_dir.replace(root_dir, to_dir)
    copy_cmd = f"mkdir -p {to_parent}; rsync -aqudzR --ignore-existing {from_path}/* {to_dir}"
    # await asyncio.sleep(1)
    # print(copy_cmd)
    process = await asyncio.create_subprocess_shell(copy_cmd)
    await process.wait()
    stdout, stderr = await process.communicate()

    return {
        'returncode': process.returncode,
        'stdout': stdout.decode(),
        'stderr': stderr.decode()
    }

def get_immediate_subdirectories(path):
    return [os.path.join(path, name) for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]

async def get_dirs(current_dir, depth=4):
    if depth == 0:
        return [current_dir]

    subdirectories = get_immediate_subdirectories(current_dir)
    if len(subdirectories) == 0:
        return [os.path.dirname(current_dir)]

    tasks = [asyncio.create_task(get_dirs(child_dir, depth - 1)) for child_dir in subdirectories]
    dirs = await asyncio.gather(*tasks)
    return chain.from_iterable(dirs)

def group_levels(path):
    return len(PurePath(path).parts) - 1

# update to use results as first class
async def process_dir(copy_root, copy_from, copy_to):
    process_result = ''
    try:
        copy_result = await copy_dir(copy_root, copy_from, copy_to)
        process_result = 'completed'
    except:
        process_result = 'errored'

    return {
        'source': copy_from,
        'status': process_result
    }

async def process_dir_level(root_dir, dirs, level, copy_to, queue):
    print(f'dir count: [{len(dirs)}] depth: [{level}]')
    
    tasks = [queue.put((root_dir, from_dir, copy_to)) for from_dir in dirs]
    await asyncio.gather(*tasks)

# load dirs as csv into file
def load_dirs(persist_file_name):
    with open(persist_file_name, 'r') as persist_file_dirs:
        return [DirectoryResult(line.split(',')[0],line.split(',')[1],line.split(',')[2].rstrip('\n').lower()) for line in persist_file_dirs.readlines()]

def persist_results(results):
    with open(input_file, 'r') as f:
        content = f.read()
        for copy_from, copy_to, status in results:
            search  = f'{copy_from},{copy_to},discovered\n'
            replace = f'{copy_from},{copy_to},{status}\n'
            content=content.replace(search, replace)
    with open(input_file, 'w') as f:
        f.write(content)

# save dirs as csv into file
def persist_dirs(dirs, copy_to):
    with open(input_file, 'r+') as persist_file_dirs:
        existing_results = [DirectoryResult(line.split(',')[0],line.split(',')[1],line.split(',')[2].lower()) for line in persist_file_dirs.readlines()]
        existing_paths = [result.source for result in existing_results]
        new_dirs = [path for path in dirs if path not in existing_paths]
        for new_dir in new_dirs:
            # persist directories
            new_result = f'{new_dir},{copy_to},discovered\n'
            persist_file_dirs.write(new_result)

def should_process(directory_result, copy_to):
    return directory_result.status not in ['completed'] and directory_result.target == copy_to

async def discover_paths(copy_from, copy_to, depth):
    # if args.discover:
    dirs = set(await get_dirs(copy_from, depth))
    persist_dirs(dirs, copy_to)    
    previous_dirs = load_dirs(input_file)
    
    # filter out previously processed dirs
    return [result.source for result in previous_dirs if should_process(result, copy_to)]

async def process_transfer(args):
    copy_from = args.source
    copy_to = args.destination
    depth = args.recursion_depth
    # input_file = args.source_input
    # use a worker per core and queue up directories based on job count parameter
    queue = asyncio.Queue(maxsize=200)
    dirs = await discover_paths(copy_from, copy_to, depth)
    cpu_count = multiprocessing.cpu_count()

    # order paths deepest first
    dirs_by_level = groupby(sorted(dirs,key=group_levels, reverse=False), group_levels)
    tasks = [asyncio.create_task(worker(f'worker-{i}', queue)) for i in range(cpu_count)]
    [await process_dir_level(copy_from, list(dir_group), level, copy_to, queue) for level, dir_group in dirs_by_level]

    await queue.join()
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    args = process_args()
    # start = time.clock()
    open(input_file, 'a').close()
    # atexit.register(save_progress)
    results = loop.run_until_complete(process_transfer(args))
    persist_results(process_results)

    # stop = time.clock()
    # print(f'run time: {stop - start}')
