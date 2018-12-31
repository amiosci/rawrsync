#!/usr/bin/env python3

import sqlite3
import asyncio
import logging
from threading import Lock

class TaskPersistence:
    def __init__(self, loop):
        self.__store_name = 'task_store.db'
        self.__loop = loop

    def __enter__(self):
        self.__create_structure()
        self.__lock = asyncio.Lock(loop=self.__loop)
        self.__thread_lock = Lock()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn().commit()
        self.conn().close()

    async def get_unclaimed_tasks(self, count=10):
        async with self.__lock:
            return self.__get_unclaimed_tasks(count)

    def conn(self):
        return sqlite3.connect(self.__store_name)

    def add_task(self, root_path, source_path, destination_path):
        self.__thread_lock.acquire()
        try:
            c = self.conn()
            c.execute('INSERT OR IGNORE INTO tasks(root_path, source_path, destination_path) VALUES (?,?,?)', (root_path, source_path, destination_path))
            c.commit()
        finally:
            self.__thread_lock.release()

    def update_task(self, status_update):
        self.__thread_lock.acquire()
        try:
            c = self.conn()
            c.executemany("INSERT INTO task_event(task_id, status_change, change_time) VALUES (?,?,DATETIME('now'))", status_update).rowcount
            c.commit()
        finally:
            self.__thread_lock.release()

    def add_task_result(self, task_id, task_returncode, task_stdout, task_stderr):
        self.__thread_lock.acquire()
        try:
            c = self.conn()
            c.execute("INSERT OR IGNORE INTO task_result(task_id, exitcode, stdout, stderr, result_time) VALUES (?,?,?,?,DATETIME('now'))", (task_id, task_returncode, task_stdout, task_stderr))
            c.commit()
        finally:
            self.__thread_lock.release()

    def print_stats(self):
        task_count, update_count, _, remaining_count, _ = self.get_stats()
        print('Total tasks\t[{0}]\nTotal events\t[{1}]\nRemaining tasks\t[{2}]'.format(task_count, update_count, remaining_count))

    def get_stats(self):
        task_count = self.conn().execute('SELECT COUNT(*) FROM tasks').fetchone()[0]
        update_count = self.conn().execute('SELECT COUNT(*) FROM task_event').fetchone()[0]
        active_count  = self.conn().execute('SELECT COUNT(*) FROM active_tasks').fetchone()[0]
        remaining_count = self.conn().execute('SELECT COUNT(*) FROM remaining_tasks').fetchone()[0]
        error_count = self.conn().execute('SELECT COUNT(*) FROM error_tasks').fetchone()[0]
        return (task_count, update_count, active_count, remaining_count, error_count)

    def get_remaining_tasks(self):
        return [task for task in self.conn().execute('SELECT * FROM remaining_tasks')]

    def get_error_tasks(self):
        return [task for task in self.conn().execute('SELECT * FROM error_tasks')]

    def get_active_tasks(self):
        return [task for task in self.conn().execute('SELECT * FROM active_tasks')]

    def reset_tasks(self):
        pass

    def set_discovery_phase(self, status):
        self.__thread_lock.acquire()
        try:
            c = self.conn()
            c.execute("UPDATE state SET value=? WHERE key='discovery'", (status,))
            c.commit()
        finally:
            self.__thread_lock.release()

    def is_discovering(self):
        self.__thread_lock.acquire()
        try:
            discovering = self.conn().execute("SELECT value FROM state WHERE key='discovery'").fetchone()[0] != "Completed"
            return discovering
        finally:
            self.__thread_lock.release()

    def reset_state(self):
        self.__thread_lock.acquire()
        try:
            self.conn().executescript('''
                INSERT OR IGNORE INTO state(key, value) VALUES ('discovery','Not Started');
            ''')
        finally:
            self.__thread_lock.release()
        
    # private
    def __get_unclaimed_tasks(self, count=1):
        task_list = [task for task in self.conn().execute('SELECT * FROM unclaimed_task LIMIT {0}'.format(count))]
        if len(task_list) == 0:
            return task_list

        task_ids = [task[0] for task in task_list]
        self.update_task([(task_id, 'progress') for task_id in task_ids])
        return task_list

    def __create_structure(self):
        self.conn().executescript('''
            CREATE TABLE IF NOT EXISTS state (
                key TEXT NOT NULL PRIMARY KEY,
                value TEXT NOT NULL
            );

            INSERT OR IGNORE INTO state(key, value) VALUES ('discovery','Not Started');

            CREATE TABLE IF NOT EXISTS tasks (
                root_path TEXT NOT NULL,
                source_path TEXT NOT NULL,
                destination_path TEXT NOT NULL,
                PRIMARY KEY (root_path, source_path, destination_path)
            );

            CREATE TABLE IF NOT EXISTS task_event (
                task_id INTEGER NOT NULL,
                status_change TEXT NOT NULL,
                change_time TEXT,
                FOREIGN KEY (task_id) REFERENCES tasks(id)
            );

            CREATE TABLE IF NOT EXISTS task_result (
                task_id INTEGER NOT NULL,
                exitcode INTEGER NOT NULL,
                stdout TEXT NOT NULL,
                stderr TEXT NOT NULL,
                result_time TEXT NOT NULL,
                FOREIGN KEY (task_id) REFERENCES tasks(id)
            );

            CREATE TRIGGER IF NOT EXISTS new_task
            AFTER INSERT
                ON 
                    tasks
            BEGIN
                INSERT INTO task_event(task_id, status_change, change_time) VALUES (NEW.rowid, 'discovered', DATETIME('now'));
            END;

            CREATE VIEW IF NOT EXISTS unclaimed_task
            AS
            SELECT
                r.task_id,
                r.event_id,
                r.root_path,
                r.source_path,
                r.destination_path,
                r.change_time,
                r.status_change
            FROM
                (
                    SELECT
                        t.rowid as task_id,
                        e.rowid as event_id,
                        t.root_path,
                        t.source_path,
                        t.destination_path,
                        e.change_time,
                        e.status_change
                    FROM
                        tasks t
                    INNER JOIN task_event e on e.task_id = t.rowid
                    GROUP BY t.rowid
                    HAVING MAX(e.rowid)
                    ORDER BY e.rowid
                ) r
            WHERE r.status_change IN ('discovered');

            CREATE VIEW IF NOT EXISTS remaining_tasks
            AS
            SELECT
                r.task_id,
                r.root_path,
                r.source_path,
                r.destination_path,
                r.change_time,
                r.status_change
            FROM
                (
                    SELECT
                        t.rowid as task_id,
                        t.root_path,
                        t.source_path,
                        t.destination_path,
                        e.change_time,
                        e.status_change
                    FROM
                        tasks t
                    INNER JOIN task_event e on e.task_id = t.rowid
                    GROUP BY t.rowid
                    HAVING MAX(e.rowid)
                    ORDER BY e.rowid
                ) r
            WHERE r.status_change NOT IN ('completed');

            CREATE VIEW IF NOT EXISTS active_tasks
            AS
            SELECT
                r.task_id,
                r.root_path,
                r.source_path,
                r.destination_path,
                r.change_time,
                r.status_change
            FROM
                (
                    SELECT
                        t.rowid as task_id,
                        t.root_path,
                        t.source_path,
                        t.destination_path,
                        e.change_time,
                        e.status_change
                    FROM
                        tasks t
                    INNER JOIN task_event e on e.task_id = t.rowid
                    GROUP BY t.rowid
                    HAVING MAX(e.rowid)
                    ORDER BY e.rowid
                ) r
            WHERE r.status_change IN ('progress');

            CREATE VIEW IF NOT EXISTS error_tasks
            AS
            SELECT
                r.task_id,
                r.root_path,
                r.source_path,
                r.destination_path,
                r.change_time,
                r.status_change
            FROM
                (
                    SELECT
                        t.rowid as task_id,
                        t.root_path,
                        t.source_path,
                        t.destination_path,
                        e.change_time,
                        e.status_change
                    FROM
                        tasks t
                    INNER JOIN task_event e on e.task_id = t.rowid
                    GROUP BY t.rowid
                    HAVING MAX(e.rowid)
                    ORDER BY e.rowid
                ) r
            WHERE r.status_change IN ('errored');
        ''')