#!/usr/bin/env python3
"""Linux /proc process-tree resource accounting for long-haul evidence.

Samples a root process and every currently reachable descendant. The tracker
keeps the maximum cumulative CPU/I/O counters seen for each process identity
(pid,starttime), so daemon restarts and child replacement do not reset totals.
Only aggregate counters are emitted; PIDs/command lines are not reported.
"""
import os
from pathlib import Path


def _read_text(path):
    try:
        return Path(path).read_text(
            encoding="utf-8",errors="replace")
    except OSError:
        return None


def _stat(path):
    text=_read_text(path)
    if not text:
        return None
    close=text.rfind(")")
    if close<0:
        return None
    head=text[:close+1]
    tail=text[close+2:].split()
    try:
        pid=int(head.split("(",1)[0].strip())
        return dict(
            pid=pid,
            ppid=int(tail[1]),
            utime_ticks=int(tail[11]),
            stime_ticks=int(tail[12]),
            starttime_ticks=int(tail[19]),
        )
    except (ValueError,IndexError):
        return None


def _rss_bytes(path):
    text=_read_text(path)
    if not text:
        return 0
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            parts=line.split()
            if len(parts)>=2:
                try:
                    return int(parts[1])*1024
                except ValueError:
                    return 0
    return 0


def _io_bytes(path):
    text=_read_text(path)
    if not text:
        return 0,0
    values={}
    for line in text.splitlines():
        if ":" not in line:
            continue
        key,value=line.split(":",1)
        try:
            values[key.strip()]=int(value.strip())
        except ValueError:
            continue
    return (
        int(values.get("read_bytes",0)),
        int(values.get("write_bytes",0)),
    )


def process_tree_sample(root_pid,proc_root="/proc"):
    root_pid=int(root_pid)
    proc_root=Path(proc_root)
    if root_pid<=0 or not proc_root.exists():
        return None

    stats={}
    try:
        entries=list(proc_root.iterdir())
    except OSError:
        return None
    for entry in entries:
        if not entry.name.isdigit():
            continue
        value=_stat(entry/"stat")
        if value is not None:
            stats[value["pid"]]=value
    if root_pid not in stats:
        return None

    children={}
    for value in stats.values():
        children.setdefault(
            value["ppid"],[]).append(value["pid"])
    selected=[]
    pending=[root_pid]
    seen=set()
    while pending:
        pid=pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        value=stats.get(pid)
        if value is None:
            continue
        selected.append(value)
        pending.extend(children.get(pid,()))

    try:
        ticks=float(os.sysconf("SC_CLK_TCK"))
    except (ValueError,OSError,AttributeError):
        ticks=100.0
    if ticks<=0:
        ticks=100.0

    processes=[]
    total_rss=0
    for value in selected:
        base=proc_root/str(value["pid"])
        read_bytes,write_bytes=_io_bytes(base/"io")
        rss=_rss_bytes(base/"status")
        total_rss+=rss
        processes.append(dict(
            identity=(
                str(value["pid"])+":"
                +str(value["starttime_ticks"])),
            cpu_seconds=(
                value["utime_ticks"]
                +value["stime_ticks"])/ticks,
            read_bytes=read_bytes,
            write_bytes=write_bytes,
            rss_bytes=rss,
        ))
    return dict(
        root_present=True,
        process_count=len(processes),
        rss_bytes=int(total_rss),
        processes=processes,
    )


def new_tracker():
    return dict(
        supported=None,
        samples=0,
        peak_rss_bytes=0,
        peak_processes=0,
        identities={},
    )


def observe(tracker,sample):
    if sample is None:
        if tracker["supported"] is None:
            tracker["supported"]=False
        return tracker
    tracker["supported"]=True
    tracker["samples"]+=1
    tracker["peak_rss_bytes"]=max(
        int(tracker["peak_rss_bytes"]),
        int(sample.get("rss_bytes",0)))
    tracker["peak_processes"]=max(
        int(tracker["peak_processes"]),
        int(sample.get("process_count",0)))
    identities=tracker["identities"]
    for item in sample.get("processes",()):
        key=str(item["identity"])
        current=identities.get(key)
        value=dict(
            cpu_seconds=max(
                0.0,float(item.get("cpu_seconds",0))),
            read_bytes=max(
                0,int(item.get("read_bytes",0))),
            write_bytes=max(
                0,int(item.get("write_bytes",0))),
        )
        if current is None:
            identities[key]=value
            continue
        current["cpu_seconds"]=max(
            current["cpu_seconds"],
            value["cpu_seconds"])
        current["read_bytes"]=max(
            current["read_bytes"],
            value["read_bytes"])
        current["write_bytes"]=max(
            current["write_bytes"],
            value["write_bytes"])
    return tracker


def report(tracker):
    identities=tracker.get("identities",{})
    return dict(
        supported=bool(tracker.get("supported")),
        samples=int(tracker.get("samples",0)),
        process_identities_seen=len(identities),
        peak_processes=int(
            tracker.get("peak_processes",0)),
        peak_rss_bytes=int(
            tracker.get("peak_rss_bytes",0)),
        cpu_seconds=sum(
            float(value.get("cpu_seconds",0))
            for value in identities.values()),
        read_bytes=sum(
            int(value.get("read_bytes",0))
            for value in identities.values()),
        write_bytes=sum(
            int(value.get("write_bytes",0))
            for value in identities.values()),
    )
