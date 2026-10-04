#!/usr/bin/env python3
"""Process-local FIFO admission for explicit SQLite writers, not a database lock.

SQLite remains authoritative for external writers and transaction durability.
Readers and implicit writes retain SQLite semantics. Idle connections retain no
queue entry. One daemon per state file is still required.
"""
from collections import deque
import os
import sqlite3
import threading
import time


_LOCK=threading.Lock()
_QUEUES={}


class Queue:
    def __init__(self):
        self.condition=threading.Condition()
        self.waiters=deque()
        self.active=False
        self.users=0


def _forget(key,queue):
    with _LOCK:
        queue.users-=1
        if queue.users==0 and _QUEUES.get(key) is queue:
            del _QUEUES[key]


def acquire(key,seconds):
    with _LOCK:
        queue=_QUEUES.get(key)
        if queue is None:
            queue=Queue()
            _QUEUES[key]=queue
        queue.users+=1
    ticket=object()
    deadline=time.monotonic()+max(0,float(seconds))
    acquired=False
    try:
        with queue.condition:
            queue.waiters.append(ticket)
            try:
                while queue.active or queue.waiters[0] is not ticket:
                    remaining=deadline-time.monotonic()
                    if remaining<=0:
                        exc=sqlite3.OperationalError('database is locked')
                        exc.sqlite_errorcode=sqlite3.SQLITE_BUSY
                        exc.sqlite_errorname='SQLITE_BUSY'
                        raise exc
                    queue.condition.wait(remaining)
                queue.waiters.popleft()
                queue.active=True
                acquired=True
            finally:
                if not acquired:
                    queue.waiters.remove(ticket)
                    queue.condition.notify_all()
        return queue
    finally:
        if not acquired:
            _forget(key,queue)


def release(key,queue):
    with queue.condition:
        queue.active=False
        queue.condition.notify_all()
    _forget(key,queue)


class FairConnection(sqlite3.Connection):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        path=super().execute('PRAGMA database_list').fetchone()[2]
        self._fair_key=os.path.realpath(path) if path else object()
        self._fair_queue=None

    def _fair_finish(self):
        if self._fair_queue is not None:
            queue=self._fair_queue
            self._fair_queue=None
            release(self._fair_key,queue)

    def execute(self,sql,*args,**kwargs):
        words=sql.strip().rstrip(';').upper().split() if sql.lstrip()[:5].upper()=='BEGIN' else []
        begin=words in [['BEGIN','IMMEDIATE'],['BEGIN','IMMEDIATE','TRANSACTION'],
                        ['BEGIN','EXCLUSIVE'],['BEGIN','EXCLUSIVE','TRANSACTION']]
        restore_timeout=None
        remaining=None
        if begin and not self.in_transaction and self._fair_queue is None:
            timeout=super().execute('PRAGMA busy_timeout').fetchone()[0]
            started=time.monotonic()
            self._fair_queue=acquire(self._fair_key,timeout/1000)
            remaining=max(0,timeout-int((time.monotonic()-started)*1000))
            if remaining<timeout:
                restore_timeout=timeout
        try:
            if restore_timeout is not None:
                super().execute('PRAGMA busy_timeout='+str(remaining))
            return super().execute(sql,*args,**kwargs)
        finally:
            try:
                if restore_timeout is not None:
                    super().execute('PRAGMA busy_timeout='+str(restore_timeout))
            finally:
                if not self.in_transaction:
                    self._fair_finish()

    def commit(self):
        try:
            return super().commit()
        finally:
            if not self.in_transaction:
                self._fair_finish()

    def rollback(self):
        try:
            return super().rollback()
        finally:
            if not self.in_transaction:
                self._fair_finish()

    def executescript(self,*args,**kwargs):
        try:
            return super().executescript(*args,**kwargs)
        finally:
            if not self.in_transaction:
                self._fair_finish()

    def __exit__(self,*args):
        try:
            return super().__exit__(*args)
        finally:
            if not self.in_transaction:
                self._fair_finish()

    def close(self):
        try:
            return super().close()
        finally:
            self._fair_finish()
