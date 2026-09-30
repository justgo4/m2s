#!/usr/bin/env python3
"""Fresh-process regression for allocator/worker virtual reservations vs RSS."""
import os
import resource
from pathlib import Path
import subprocess
import sys
import threading

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import j4


def worker():
    with j4.catalog_variable_scope(dict(CDC_RESOURCE_MEMORY_MB='512',
                                       CDC_RESOURCE_CPU_CORES='1',
                                       CDC_RESOURCE_IONICE='off')):
        policy = j4.read_resource_policy()
    j4.apply_resource_policy(policy)
    ready, release, started = [], threading.Event(), threading.Event()

    def hold():
        # glibc arenas and thread stacks reserve virtual memory beyond RSS.
        block = bytearray(256 * 1024)
        ready.append(block)
        if len(ready) == 32:
            started.set()
        release.wait(10)

    threads = []
    try:
        for _ in range(32):
            thread = threading.Thread(target=hold)
            thread.start()
            threads.append(thread)
        if not started.wait(5) or len(ready) != 32:
            raise AssertionError('worker startup failed with low resident usage')
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        if rss >= 512:
            raise AssertionError('test exceeds logical resident budget')
        if policy['memory_mb'] != 512:
            raise AssertionError('logical memory budget was changed')
        print('RESOURCE STARTUP PASS workers=32 logical_rss_budget_mb=512', flush=True)
    finally:
        release.set()
        for thread in threads:
            thread.join()


def main():
    if '--worker' in sys.argv:
        worker()
    else:
        subprocess.run([sys.executable, __file__, '--worker'], check=True, timeout=40,
                       env=dict(os.environ, OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1'))


if __name__ == '__main__':
    main()
