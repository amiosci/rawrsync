#!/usr/bin/env python3
from abc import ABC, abstractmethod
import asyncio
import subprocess

class AbstractRunner(ABC):
    @abstractmethod
    def process_dir(self, copy_root, copy_from, copy_to):
        raise NotImplementedError

class NullRunner(AbstractRunner):
    def process_dir(self, copy_root, copy_from, copy_to):
        print(f'Copying [{copy_from}] to [{copy_to}]')
        return ('no-op', copy_from,)

class RsyncRunner(AbstractRunner):
    def process_dir(self, copy_root, copy_from, copy_to):
        copy_result = self.__copy_dir_sync(copy_root, copy_from, copy_to)
        print(copy_result)
        process_result = 'completed' if copy_result['returncode'] == 0 else 'errored'
        return (process_result, copy_result)

    def __copy_dir_sync(self, root_dir, from_dir, to_dir):
        # --bwlimit=RATE
        # --list-only
        # --progress
        # --compress
        # --dry-run
        # --super
        # --preallocate 
        
        # updates the path to trim the parent dir
        from_path = from_dir.replace(root_dir, f'{root_dir}/.')
        copy_cmd = f"rsync -aqudzR --exclude '/' --ignore-existing {from_path}/ {to_dir}/"

        process = subprocess.run(
            copy_cmd, 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)
        print(process)
        return {
            'returncode': process.returncode,
            'stdout': str(process.stdout),
            'stderr': str(process.stderr)
        }