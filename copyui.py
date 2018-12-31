#!/usr/bin/env python3
import curses
import time

class CopyUI:
    def __init__(self, enabled=True):
        self._enabled = enabled

    def __enter__(self):
        if self._enabled:
            self._screen = curses.initscr()
            self._screen.keypad(True)
            curses.noecho()
            curses.cbreak()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._enabled:
            curses.nocbreak()
            self._screen.keypad(False)
            curses.echo()
            curses.endwin()
    
    def run(self, store, is_running_func):
        if not self._enabled:
            return

        sleep_length = 5
        while is_running_func():
            self._screen.clear()
            self._screen.addstr('Update Time: [{0}]'.format(time.strftime("%H:%M:%S")))
            active_dirs = [str(task[2]) for task in store.get_active_tasks()]
            task_count, update_count, _, remaining_count, error_count = store.get_stats()
            self._screen.addstr('''
Total Tasks:        {0}
Total Updates:      {1}
Total Active:       {2}
Total Remaining:    {3}
Total Error:        {4}
            '''.format(task_count, update_count, len(active_dirs), remaining_count, error_count))
            self._screen.addstr('\nActive Transfers')
            self._screen.addstr('\n----------------\n{0}'.format('\n'.join(active_dirs)))
            self._screen.refresh()
            time.sleep(sleep_length)
