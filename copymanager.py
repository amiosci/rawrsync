#!/usr/bin/env python3

import os
import asyncio
import logging
from itertools import chain
from concurrent.futures import ThreadPoolExecutor

from taskpersistence import TaskPersistence
from copyrunner import RsyncRunner, NullRunner
from copyui import CopyUI

class CopyManager:
    def __init__(self, args):
        self.source = args['source']
        self.destination = args['destination']
        self.interactive = args['interactive']
        self.thread_count = args['thread_count']
        self.discover = True
        self.force = False
        self.__runner = RsyncRunner()

    def __enter__(self):
        self.__loop = asyncio.new_event_loop()
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        self.__loop.close()

    # public
    def process_transfer(self):
        results = self.__loop.run_until_complete(self.__process_transfer())
        return results

    # private
    async def __process_transfer(self):
        with ThreadPoolExecutor(4) as executor:
            with CopyUI(self.interactive) as copy_ui:
                with TaskPersistence(self.__loop) as store:
                    store.reset_state()
                    tasks = [asyncio.ensure_future(self.__process_discovery(store, executor), loop=self.__loop)]
                    running = True
                    ui_task = asyncio.ensure_future(self.__loop.run_in_executor(executor, lambda: copy_ui.run(store, lambda: running)))
                    tasks.extend([asyncio.ensure_future(self.__worker('worker-{0}'.format(i), store), loop=self.__loop) for i in range(self.thread_count)])
                    await asyncio.gather(*tasks, loop=self.__loop)
                    running = False
                    ui_task.cancel()
                    store.print_stats()
                    return store.get_remaining_tasks()

    async def __process_discovery(self, store, executor):
        store.set_discovery_phase('Started')
        if self.discover:
            discovery_task = asyncio.ensure_future(self.__loop.run_in_executor(executor, lambda: self.__add_dirs(self.source, lambda x: store.add_task(self.source, x, self.destination))))

        if discovery_task is not None:
            await discovery_task

        store.set_discovery_phase('Completed')        

        
    def __add_dirs(self, current_dir, on_found):
        subdirectories = [os.path.join(current_dir, name) for name in os.listdir(current_dir) if os.path.isdir(os.path.join(current_dir, name))]
        if len(subdirectories) == 0:
            on_found(os.path.dirname(current_dir))
            return

        dirs = set([self.__add_dirs(child_dir, on_found) for child_dir in subdirectories])
        [on_found(dir) for dir in dirs]

    async def __worker(self, name, store):
        worker_list = 10
        worker_sleep = 3
        with ThreadPoolExecutor(worker_list) as executor:
            task_list = await store.get_unclaimed_tasks(worker_list)
            while store.is_discovering():
                if len(task_list) > 0:
                    tasks = [asyncio.ensure_future(self.__loop.run_in_executor(executor, self.__construct_task_processor(store, task))) for task in task_list]
                    await asyncio.gather(*tasks, loop=self.__loop)
                else:
                    await asyncio.sleep(worker_sleep)
                task_list = await store.get_unclaimed_tasks(worker_list)

    def __construct_task_processor(self, store, task):
        task_id, event_id, root_path, source_path, destination_path, _, _ = task
        def process_task():
            logging.debug('Processing [{0}] [{1}] {2}'.format(task_id, event_id, source_path))
            status, result = self.__runner.process_dir(root_path, source_path, destination_path)
            store.update_task([(task_id, status)])
            store.add_task_result(task_id, result['returncode'], result['stdout'], result['stderr'])
            logging.debug('Processed [{0}] [{1}] {2} with result {3}'.format(task_id, event_id, source_path, status))
        
        return process_task