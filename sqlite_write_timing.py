#!/usr/bin/env python3
"""Opt-in process-local timing of explicit SQLite writer transactions.

No SQL text, parameters, database paths or row values are retained. These are
wall-clock observations, including SQLite/OS scheduling and COMMIT durability,
not estimates of exclusive CPU work or attribution of another process's lock.
"""
import sqlite3
import sys
import threading
import time


class Collector:
    def __init__(self,limit=128):
        self.limit=max(1,min(int(limit),128))
        self.lock=threading.Lock()
        self.buckets={}
        self.active={}
        self.next_id=0
        self.excluded_scripts=0
        self.excluded_holds=0
        self.active_unlisted=0

    def connection_id(self):
        with self.lock:
            self.next_id+=1
            return self.next_id

    def record(self,operation,phase,outcome,seconds):
        key=(operation,phase,outcome)
        with self.lock:
            if key not in self.buckets and len(self.buckets)>=self.limit-1:
                key=('overflow','mixed','mixed')
            bucket=self.buckets.setdefault(key,dict(count=0,total_seconds=0.0,max_seconds=0.0))
            bucket['count']+=1
            bucket['total_seconds']+=seconds
            bucket['max_seconds']=max(bucket['max_seconds'],seconds)

    def snapshot(self):
        now=time.monotonic()
        with self.lock:
            return dict(enabled=True,scope='process_lifetime',
                coverage='Connection.execute BEGIN IMMEDIATE/EXCLUSIVE through COMMIT/ROLLBACK; cursor, deferred and implicit writes excluded',
                hold_includes_commit=True,excluded_scripts=self.excluded_scripts,
                excluded_holds=self.excluded_holds,active_unlisted=self.active_unlisted,
                operations=[dict(operation=key[0],phase=key[1],outcome=key[2],**value)
                            for key,value in sorted(self.buckets.items())],
                active=[dict(operation=value[0],elapsed_seconds=max(0,now-value[1]))
                        for _,value in sorted(self.active.items())])


def _operation():
    frame=sys._getframe(2)
    try:
        while frame is not None:
            module=frame.f_globals.get('__name__','unknown').split('.')[-1]
            function=frame.f_code.co_name
            if module not in {'contextlib','sqlite_write_timing'} and function not in {'transaction','state_transaction'}:
                return module+':'+function
            frame=frame.f_back
        return 'unknown'
    finally:
        del frame


def _outcome(exc):
    return ('busy' if (getattr(exc,'sqlite_errorcode',0)&255)==sqlite3.SQLITE_BUSY
            else 'error')


class TimingConnection(sqlite3.Connection):
    def __init__(self,*args,collector=None,**kwargs):
        super().__init__(*args,**kwargs)
        self.timing=collector if collector is not None else PROCESS
        self.timing_id=self.timing.connection_id()
        self.writer=None

    def _finish(self,outcome):
        if self.writer is None:
            return
        operation,started=self.writer
        ended=time.monotonic()
        self.writer=None
        with self.timing.lock:
            if self.timing.active.pop(self.timing_id,None) is None:
                self.timing.active_unlisted-=1
        self.timing.record(operation,'hold',outcome,ended-started)

    def execute(self,sql,*args,**kwargs):
        words=sql.strip().rstrip(';').upper().split()
        begin=words in [['BEGIN','IMMEDIATE'],['BEGIN','IMMEDIATE','TRANSACTION'],
                        ['BEGIN','EXCLUSIVE'],['BEGIN','EXCLUSIVE','TRANSACTION']]
        end=words in [['COMMIT'],['COMMIT','TRANSACTION'],['END'],['END','TRANSACTION'],
                      ['ROLLBACK'],['ROLLBACK','TRANSACTION']]
        if not begin and not end:
            try:
                return super().execute(sql,*args,**kwargs)
            finally:
                if self.writer is not None and not self.in_transaction:
                    self._finish('automatic_end')
        started=time.monotonic()
        operation=_operation() if begin else None
        was_active=self.in_transaction
        try:
            result=super().execute(sql,*args,**kwargs)
        except BaseException as exc:
            if begin:
                self.timing.record(operation,'acquire',_outcome(exc),time.monotonic()-started)
            elif self.writer is not None:
                self.timing.record(self.writer[0],'end_call',_outcome(exc),time.monotonic()-started)
            if self.writer is not None and not self.in_transaction:
                self._finish('automatic_end')
            raise
        if begin and not was_active:
            acquired=time.monotonic()
            self.writer=(operation,acquired)
            with self.timing.lock:
                if len(self.timing.active)<128:
                    self.timing.active[self.timing_id]=self.writer
                else:
                    self.timing.active_unlisted+=1
            self.timing.record(operation,'acquire','success',acquired-started)
        elif end and not self.in_transaction:
            self._finish('rollback' if words[0]=='ROLLBACK' else 'commit')
        return result

    def commit(self):
        started=time.monotonic()
        try:
            result=super().commit()
        except BaseException as exc:
            if self.writer is not None:
                self.timing.record(self.writer[0],'end_call',_outcome(exc),time.monotonic()-started)
                if not self.in_transaction:
                    self._finish('automatic_end')
            raise
        if not self.in_transaction:
            self._finish('commit')
        return result

    def rollback(self):
        result=super().rollback()
        if not self.in_transaction:
            self._finish('rollback')
        return result

    def executescript(self,*args,**kwargs):
        with self.timing.lock:
            self.timing.excluded_scripts+=1
            # executescript may commit the existing transaction before running
            # arbitrary transaction controls. Do not attribute its full time to
            # the old holder or pretend to instrument transactions in scripts.
            if self.writer is not None:
                self.timing.excluded_holds+=1
                if self.timing.active.pop(self.timing_id,None) is None:
                    self.timing.active_unlisted-=1
                self.writer=None
        return super().executescript(*args,**kwargs)

    def close(self):
        result=super().close()
        self._finish('close')
        return result


PROCESS=Collector()
