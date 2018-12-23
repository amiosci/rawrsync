#!/usr/bin/env python3

import sqlite3
import asyncio
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

    def add_tasks(self, tasks):
        self.__thread_lock.acquire()
        try:
            c = self.conn()
            task_count = c.executemany("INSERT OR IGNORE INTO tasks(root_path, source_path, destination_path) VALUES (?,?,?)", tasks).rowcount
            c.commit()
            print(f'Added {task_count} tasks')
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

    def print_stats(self):
        task_count = self.conn().execute("SELECT MAX(ROWID) FROM tasks").fetchone()[0]
        update_count = self.conn().execute("SELECT MAX(ROWID) FROM task_event").fetchone()[0]
        print(f'Total tasks  [{task_count}]\nTotal events [{update_count}]')

    def get_remaining_tasks(self):
        return [task for task in self.conn().execute(f"SELECT * FROM remaining_tasks")]

    def reset_tasks(self):
        pass
        
    # private
    def __get_unclaimed_tasks(self, count=1):
        task_list = [task for task in self.conn().execute(f"SELECT * FROM unclaimed_task LIMIT {count}")]
        if len(task_list) == 0:
            return task_list

        task_ids = [task[0] for task in task_list]
        self.update_task([(task_id, 'progress') for task_id in task_ids])
        return task_list

    def __create_structure(self):
        self.conn().executescript('''
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

            CREATE TRIGGER IF NOT EXISTS new_task
            AFTER INSERT
                ON 
                    tasks
            BEGIN
                INSERT INTO task_event(task_id, status_change, change_time) VALUES (NEW.rowid, 'discovered', DATETIME('now'));
            END;
        ''')