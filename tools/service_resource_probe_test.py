#!/usr/bin/env python3
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/"tools"))

import service_resource_probe


def stat_line(pid,ppid,utime,stime,starttime):
    fields=[
        "S",str(ppid),"0","0","0","0","0","0","0","0",
        "0",str(utime),str(stime),"0","0","0","0","0",
        "0",str(starttime),"0","0","0","0","0","0","0",
        "0","0","0","0","0","0","0","0","0","0","0",
        "0","0","0","0","0","0","0","0","0","0","0"
    ]
    return (
        str(pid)+" (mysql worker) "
        +" ".join(fields)+"\n"
    )


def write_process(proc,pid=100):
    directory=proc/str(pid)
    directory.mkdir(
        parents=True,exist_ok=True)
    (directory/"stat").write_text(
        stat_line(pid,1,100,50,1000),
        encoding="utf-8")
    (directory/"status").write_text(
        "Name:\tmysqld\nVmRSS:\t2048 kB\n",
        encoding="utf-8")
    (directory/"io").write_text(
        "read_bytes: 4096\n"
        "write_bytes: 8192\n",
        encoding="utf-8")
    (directory/"fd").mkdir(
        exist_ok=True)
    (directory/"fd"/"5").symlink_to(
        "socket:[12345]")


def write_listener(proc,port=3306):
    (proc/"net").mkdir(
        parents=True,exist_ok=True)
    header=(
        "  sl  local_address rem_address   st "
        "tx_queue rx_queue tr tm->when retrnsmt "
        "uid timeout inode\n"
    )
    line=(
        "0: 0100007F:%04X 00000000:0000 0A "
        "00000000:00000000 00:00000000 "
        "00000000 1000 0 12345 1\n"
    ) % int(port)
    (proc/"net"/"tcp").write_text(
        header+line,encoding="utf-8")
    (proc/"net"/"tcp6").write_text(
        header,encoding="utf-8")


def write_cgroup(
        proc,cgroup_root,
        cpu_usec,memory,
        read_bytes,write_bytes,
        path="/system.slice/mysql.service"):
    (proc/"100"/"cgroup").write_text(
        "0::"+path+"\n",
        encoding="utf-8")
    if path=="/":
        directory=cgroup_root
    else:
        directory=(
            cgroup_root/path.lstrip("/"))
    directory.mkdir(
        parents=True,exist_ok=True)
    (directory/"cpu.stat").write_text(
        "usage_usec %d\n" % int(cpu_usec),
        encoding="utf-8")
    (directory/"memory.current").write_text(
        str(int(memory))+"\n",
        encoding="utf-8")
    (directory/"io.stat").write_text(
        "8:0 rbytes=%d wbytes=%d rios=1 wios=2\n"
        % (int(read_bytes),int(write_bytes)),
        encoding="utf-8")


def main():
    with tempfile.TemporaryDirectory(
        prefix="m2s-service-probe-"
    ) as td:
        root=Path(td)
        proc=root/"proc"
        cgroup=root/"cgroup"
        proc.mkdir()
        cgroup.mkdir()
        write_process(proc)
        write_listener(proc)
        write_cgroup(
            proc,cgroup,
            cpu_usec=5_000_000,
            memory=10_000,
            read_bytes=1000,
            write_bytes=2000)

        selection=(
            service_resource_probe
            .resolve_service_pid(
                port=3306,proc_root=proc))
        assert selection==dict(
            pid=100,
            source="listen_port",
            reason="resolved")

        sample=(
            service_resource_probe
            .service_sample(
                100,proc_root=proc,
                cgroup_root=cgroup))
        assert sample["mode"]=="cgroup_v2"
        assert sample["memory_bytes"]==10_000
        assert sample["cpu_seconds"]==5.0
        assert sample["read_bytes"]==1000
        assert sample["write_bytes"]==2000
        assert len(
            sample["identity"])==64
        assert (
            "system.slice" not in
            sample["identity"])

        tracker=(
            service_resource_probe
            .new_tracker(selection))
        service_resource_probe.observe(
            tracker,sample)
        write_cgroup(
            proc,cgroup,
            cpu_usec=8_500_000,
            memory=20_000,
            read_bytes=1600,
            write_bytes=2900)
        service_resource_probe.observe(
            tracker,
            service_resource_probe.service_sample(
                100,proc_root=proc,
                cgroup_root=cgroup))
        result=service_resource_probe.report(
            tracker)
        assert result["supported"]
        assert result["scope_stable"]
        assert result["mode"]=="cgroup_v2"
        assert result["samples"]==2
        assert result["peak_memory_bytes"]==20_000
        assert abs(
            result["cpu_seconds"]-3.5)<1e-9
        assert result["read_bytes"]==600
        assert result["write_bytes"]==900
        assert (
            result["selection_source"]
            =="listen_port")

        explicit=(
            service_resource_probe
            .resolve_service_pid(
                explicit_pid=100,
                port=9999,proc_root=proc))
        assert explicit["pid"]==100
        assert (
            explicit["source"]
            =="explicit_pid")

        # A unified root cgroup is deliberately not accepted as a
        # service scope. Fall back to the bounded process tree.
        write_cgroup(
            proc,cgroup,
            cpu_usec=99_000_000,
            memory=999_000_000,
            read_bytes=999_000,
            write_bytes=999_000,
            path="/")
        with patch.object(
            service_resource_probe
                .process_resource_probe.os,
            "sysconf",return_value=100
        ):
            fallback=(
                service_resource_probe
                .service_sample(
                    100,proc_root=proc,
                    cgroup_root=cgroup))
        assert fallback["mode"]=="process_tree"
        assert (
            fallback["memory_bytes"]
            ==2048*1024)
        assert (
            fallback["process_count"]==1)
        fallback_tracker=(
            service_resource_probe.new_tracker(
                dict(
                    source="explicit_pid",
                    reason="resolved")))
        service_resource_probe.observe(
            fallback_tracker,fallback)
        (proc/"100"/"stat").write_text(
            stat_line(100,1,160,70,1000),
            encoding="utf-8")
        (proc/"100"/"status").write_text(
            "Name:\tmysqld\nVmRSS:\t3072 kB\n",
            encoding="utf-8")
        (proc/"100"/"io").write_text(
            "read_bytes: 6000\n"
            "write_bytes: 10000\n",
            encoding="utf-8")
        with patch.object(
            service_resource_probe
                .process_resource_probe.os,
            "sysconf",return_value=100
        ):
            service_resource_probe.observe(
                fallback_tracker,
                service_resource_probe.service_sample(
                    100,proc_root=proc,
                    cgroup_root=cgroup))
        fallback_report=(
            service_resource_probe.report(
                fallback_tracker))
        assert abs(
            fallback_report["cpu_seconds"]-.8)<1e-9
        assert fallback_report["read_bytes"]==1904
        assert fallback_report["write_bytes"]==1808
        assert (
            fallback_report["peak_memory_bytes"]
            ==3072*1024)

        missing=(
            service_resource_probe
            .resolve_service_pid(
                explicit_pid=999,
                proc_root=proc))
        assert missing["pid"] is None
        assert (
            missing["reason"]
            =="explicit_pid_not_visible")

        # A duplicate owner for one listening inode is ambiguous and
        # must not be guessed.
        write_process(proc,pid=101)
        duplicate=(
            service_resource_probe
            .resolve_service_pid(
                port=3306,proc_root=proc))
        assert duplicate["pid"] is None
        assert (
            duplicate["reason"]
            =="listener_ambiguous")
        assert duplicate["candidates"]==2

    print(
        "service_resource_probe_test ok listener_pid explicit_pid "
        "cgroup_v2_delta root_cgroup_rejected process_tree_fallback "
        "ambiguous_fail_closed privacy_scope_hash",
        flush=True,
    )


if __name__=="__main__":
    main()
