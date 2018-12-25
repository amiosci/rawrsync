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
        print('Copying [{0}] to [{1}]'.format(copy_from, copy_to))
        return ('no-op', copy_from,)

class RsyncRunner(AbstractRunner):
    def process_dir(self, copy_root, copy_from, copy_to):
        copy_result = self.__copy_dir_sync(copy_root, copy_from, copy_to)
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
        # --exclude '/'
        
        # updates the path to trim the parent dir
        from_path = from_dir.replace(root_dir, '{0}/.'.format(root_dir))
        copy_cmd = "rsync -aqudzR -f '- /*/*/' --ignore-existing '{0}/' '{1}/'".format(from_path, to_dir)

        process = subprocess.run(
            copy_cmd, 
            shell=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)

        return {
            'returncode': process.returncode,
            'stdout': str(process.stdout),
            'stderr': str(process.stderr)
        }
