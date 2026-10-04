#!/usr/bin/env python3
"""Real Git safeguards for the isolated comparison driver."""
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from tools import performance_abba as abba


class AbbaTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)/'repo';self.root.mkdir()
        self.git('init','-q');self.git('config','user.email','synthetic@example.invalid')
        self.git('config','user.name','Synthetic')
        (self.root/'data').write_text('A');self.git('add','data');self.git('commit','-qm','A')
        self.a=self.git('rev-parse','HEAD').strip()
        (self.root/'data').write_text('B');self.git('commit','-qam','B')
        self.b=self.git('rev-parse','HEAD').strip()
        self.directory=Path(self.temp.name)/'new-results'

    def git(self,*args):
        return subprocess.check_output(['git',*args],cwd=self.root,text=True,stderr=subprocess.DEVNULL)

    def test_port_guard_rejects_listener_but_accepts_closed_service_time_wait(self):
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            listener.bind(('127.0.0.1',0));port=listener.getsockname()[1]
            listener.listen()
            with self.assertRaises(OSError):abba.require_free_ports((port,))
            with socket.create_connection(('127.0.0.1',port)) as client:
                accepted,_=listener.accept()
                accepted.shutdown(socket.SHUT_RDWR);accepted.close()
                self.assertEqual(client.recv(1),b'')
        abba.require_free_ports((port,))

    def test_exact_abba_and_same_revision_control(self):
        plan=abba.experiment_plan(self.a,self.b,self.directory,root=self.root)
        self.assertEqual([item['revision'] for item in plan['order']],[self.a,self.b,self.b,self.a])
        self.assertFalse(plan['certification']);self.assertFalse(self.directory.exists())
        self.assertEqual(plan['parameters']['memory_mb'],4096)
        self.assertTrue(abba.experiment_plan(self.a,self.a,self.directory,root=self.root)['same_revision_control'])

    def test_mutable_missing_or_noncommit_revision_rejected(self):
        for value in ('main','HEAD',self.a[:12],'f'*40):
            with self.assertRaises((ValueError,subprocess.CalledProcessError)):
                abba.experiment_plan(value,self.b,self.directory,root=self.root)
        blob=self.git('hash-object','data').strip()
        with self.assertRaises(subprocess.CalledProcessError):abba.resolve_revision(blob,self.root)

    def test_dirty_checkout_old_directory_and_nested_results_rejected(self):
        (self.root/'data').write_text('uncommitted')
        with self.assertRaises(RuntimeError):abba.experiment_plan(self.a,self.b,self.directory,root=self.root)
        self.git('checkout','--','data');self.directory.mkdir()
        with self.assertRaises(ValueError):abba.experiment_plan(self.a,self.b,self.directory,root=self.root)
        with self.assertRaises(ValueError):abba.experiment_plan(self.a,self.b,self.root/'results',root=self.root)
        with self.assertRaises(ValueError):abba.experiment_plan(self.a,self.b,Path(self.temp.name)/'x','million',root=self.root)


if __name__=='__main__':unittest.main()
