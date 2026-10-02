#!/usr/bin/env python3
from pathlib import Path
import os
import sys
import tempfile
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/"tools"))

import process_resource_probe


def stat_line(pid,ppid,utime,stime,starttime):
    fields=[
        "S",str(ppid),"0","0","0","0","0","0","0","0",
        "0",str(utime),str(stime),"0","0","0","0","0",
        "0",str(starttime),"0","0","0","0","0","0","0",
        "0","0","0","0","0","0","0","0","0","0","0",
        "0","0","0","0","0","0","0","0","0","0","0"
    ]
    return str(pid)+" (worker process) "+" ".join(fields)+"\n"


def write_process(root,pid,ppid,utime,stime,starttime,rss_kb,read_bytes,write_bytes):
    directory=root/str(pid)
    directory.mkdir()
    (directory/"stat").write_text(
        stat_line(
            pid,ppid,utime,stime,starttime),
        encoding="utf-8")
    (directory/"status").write_text(
        "Name:\ttest\nVmRSS:\t%d kB\n" % rss_kb,
        encoding="utf-8")
    (directory/"io").write_text(
        "read_bytes: %d\nwrite_bytes: %d\n"
        % (read_bytes,write_bytes),
        encoding="utf-8")


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-proc-probe-"
    ) as td:
        proc=Path(td)
        write_process(
            proc,100,1,100,50,1000,
            1024,4096,8192)
        write_process(
            proc,101,100,20,10,1100,
            512,1024,2048)
        write_process(
            proc,102,999,999,999,1200,
            4096,9999,9999)
        with patch.object(
            process_resource_probe.os,
            "sysconf",return_value=100
        ):
            sample=process_resource_probe.process_tree_sample(
                100,proc_root=proc)
        assert sample["process_count"]==2
        assert sample["rss_bytes"]==1536*1024
        by_id={
            item["identity"]:item
            for item in sample["processes"]
        }
        assert by_id["100:1000"]["cpu_seconds"]==1.5
        assert by_id["101:1100"]["cpu_seconds"]==0.3
        assert by_id["100:1000"]["read_bytes"]==4096
        assert by_id["101:1100"]["write_bytes"]==2048

        tracker=process_resource_probe.new_tracker()
        process_resource_probe.observe(
            tracker,sample)
        second=dict(
            root_present=True,
            process_count=1,
            rss_bytes=2*1024*1024,
            processes=[
                dict(
                    identity="100:1000",
                    cpu_seconds=2.0,
                    read_bytes=5000,
                    write_bytes=9000,
                    rss_bytes=2*1024*1024),
                dict(
                    identity="103:1300",
                    cpu_seconds=.5,
                    read_bytes=100,
                    write_bytes=200,
                    rss_bytes=0),
            ])
        process_resource_probe.observe(
            tracker,second)
        result=process_resource_probe.report(
            tracker)
        assert result["supported"]
        assert result["samples"]==2
        assert result["process_identities_seen"]==3
        assert result["peak_processes"]==2
        assert result["peak_rss_bytes"]==2*1024*1024
        assert abs(result["cpu_seconds"]-2.8)<1e-9
        assert result["read_bytes"]==5000+1024+100
        assert result["write_bytes"]==9000+2048+200

    unsupported=process_resource_probe.new_tracker()
    process_resource_probe.observe(
        unsupported,None)
    result=process_resource_probe.report(
        unsupported)
    assert not result["supported"]
    assert result["samples"]==0

    print(
        "process_resource_probe_test ok descendants restart_identity "
        "peak_rss cumulative_cpu_io privacy_aggregate_only",
        flush=True,
    )


if __name__=="__main__":
    main()
