#!/usr/bin/env python3
"""Bounded Linux service resource accounting for P11 source/sink evidence.

A service can be selected by an explicit PID or, when the server is in the
same network namespace, by a unique LISTEN socket.  cgroup v2 is preferred
because it accounts the whole service scope, including descendants.  Root
cgroups are rejected to avoid accidentally charging the whole host.  When a
usable cgroup is unavailable the probe falls back to the existing process-tree
accounting.

Only aggregate counters and a one-way scope fingerprint are reported; PIDs,
cgroup paths and command lines are deliberately omitted from reports.
"""
import hashlib
import os
from pathlib import Path

import process_resource_probe


def _read_text(path):
    try:
        return Path(path).read_text(
            encoding="utf-8",errors="replace")
    except OSError:
        return None


def _integer_file(path):
    text=_read_text(path)
    if text is None:
        return None
    try:
        value=int(text.strip())
    except ValueError:
        return None
    return value if value>=0 else None


def _memory_limit_bytes(cgroup):
    text=_read_text(
        Path(cgroup)/"memory.max")
    if text is None:
        return None
    value=text.strip()
    if value=="max":
        return None
    try:
        value=int(value)
    except ValueError:
        return None
    return value if value>0 else None


def _cpu_quota_cores(cgroup):
    text=_read_text(
        Path(cgroup)/"cpu.max")
    if text is None:
        return None
    parts=text.split()
    if len(parts)<2 or parts[0]=="max":
        return None
    try:
        quota=float(parts[0])
        period=float(parts[1])
    except ValueError:
        return None
    if quota<=0 or period<=0:
        return None
    return quota/period


def _scope_fingerprint(value):
    return hashlib.sha256(
        str(value).encode("utf-8")).hexdigest()


def _unified_cgroup_path(pid,proc_root="/proc"):
    text=_read_text(
        Path(proc_root)/str(int(pid))/"cgroup")
    if not text:
        return None
    paths=[]
    for line in text.splitlines():
        parts=line.split(":",2)
        if len(parts)!=3:
            continue
        hierarchy,controllers,path=parts
        if hierarchy=="0" and controllers=="":
            path=path.strip()
            if path.startswith("/"):
                paths.append(path)
    if len(set(paths))!=1:
        return None
    path=paths[0]
    # "/" would measure the whole machine in a common bare-host setup.
    if path=="/":
        return None
    return path


def _cpu_usage_seconds(cgroup):
    text=_read_text(Path(cgroup)/"cpu.stat")
    if not text:
        return None
    values={}
    for line in text.splitlines():
        parts=line.split()
        if len(parts)!=2:
            continue
        try:
            values[parts[0]]=int(parts[1])
        except ValueError:
            continue
    if "usage_usec" in values:
        return max(0,float(values["usage_usec"])/1_000_000.0)
    if "usage_nsec" in values:
        return max(0,float(values["usage_nsec"])/1_000_000_000.0)
    return None


def _io_bytes(cgroup):
    text=_read_text(Path(cgroup)/"io.stat")
    if text is None:
        return None
    read_bytes=0
    write_bytes=0
    parsed=False
    for line in text.splitlines():
        parts=line.split()
        if len(parts)<2:
            continue
        fields={}
        for item in parts[1:]:
            if "=" not in item:
                continue
            key,value=item.split("=",1)
            try:
                fields[key]=int(value)
            except ValueError:
                continue
        if "rbytes" in fields or "wbytes" in fields:
            parsed=True
            read_bytes+=max(0,int(fields.get("rbytes",0)))
            write_bytes+=max(0,int(fields.get("wbytes",0)))
    if not parsed:
        # An existing but empty io.stat is a valid zero-I/O baseline for
        # a freshly created cgroup. Missing/unreadable files returned above.
        return 0,0
    return int(read_bytes),int(write_bytes)


def cgroup_v2_sample(
        pid,proc_root="/proc",
        cgroup_root="/sys/fs/cgroup"):
    path=_unified_cgroup_path(
        pid,proc_root=proc_root)
    if path is None:
        return None
    relative=path.lstrip("/")
    cgroup=Path(cgroup_root)/relative
    memory=_integer_file(cgroup/"memory.current")
    memory_limit=_memory_limit_bytes(cgroup)
    cpu=_cpu_usage_seconds(cgroup)
    cpu_quota=_cpu_quota_cores(cgroup)
    io=_io_bytes(cgroup)
    if memory is None or cpu is None or io is None:
        return None
    return dict(
        mode="cgroup_v2",
        identity=_scope_fingerprint(
            "cgroup-v2:"+path),
        memory_metric="cgroup_memory_current",
        memory_bytes=int(memory),
        memory_limit_bytes=memory_limit,
        cpu_quota_cores=cpu_quota,
        cpu_seconds=float(cpu),
        read_bytes=int(io[0]),
        write_bytes=int(io[1]),
    )


def _listen_inodes(port,proc_root="/proc"):
    port=int(port)
    if not 1<=port<=65535:
        raise ValueError("port must be 1..65535")
    wanted="%04X" % port
    inodes=set()
    for name in ("tcp","tcp6"):
        text=_read_text(
            Path(proc_root)/"net"/name)
        if not text:
            continue
        for line in text.splitlines()[1:]:
            parts=line.split()
            if len(parts)<10:
                continue
            local=parts[1]
            state=parts[3].upper()
            if ":" not in local or state!="0A":
                continue
            if local.rsplit(":",1)[1].upper()!=wanted:
                continue
            inode=parts[9]
            if inode.isdigit():
                inodes.add(inode)
    return inodes


def listener_pids(port,proc_root="/proc"):
    proc_root=Path(proc_root)
    inodes=_listen_inodes(
        port,proc_root=proc_root)
    if not inodes:
        return []
    found=set()
    try:
        entries=list(proc_root.iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.name.isdigit():
            continue
        fd_dir=entry/"fd"
        try:
            fds=list(fd_dir.iterdir())
        except OSError:
            continue
        for fd in fds:
            try:
                target=os.readlink(fd)
            except OSError:
                continue
            if (
                target.startswith("socket:[")
                and target.endswith("]")
                and target[8:-1] in inodes
            ):
                found.add(int(entry.name))
                break
    return sorted(found)


def resolve_service_pid(
        explicit_pid=None,port=None,
        proc_root="/proc"):
    if explicit_pid is not None:
        pid=int(explicit_pid)
        if pid<=0:
            raise ValueError(
                "explicit service pid must be positive")
        if not (
            Path(proc_root)/str(pid)/"stat"
        ).exists():
            return dict(
                pid=None,source="explicit_pid",
                reason="explicit_pid_not_visible")
        return dict(
            pid=pid,source="explicit_pid",
            reason="resolved")
    if port is None:
        return dict(
            pid=None,source="none",
            reason="no_pid_or_port")
    pids=listener_pids(
        int(port),proc_root=proc_root)
    if len(pids)==1:
        return dict(
            pid=pids[0],source="listen_port",
            reason="resolved")
    return dict(
        pid=None,source="listen_port",
        reason=(
            "listener_not_found"
            if not pids
            else "listener_ambiguous"),
        candidates=len(pids),
    )


def service_sample(
        pid,proc_root="/proc",
        cgroup_root="/sys/fs/cgroup"):
    sample=cgroup_v2_sample(
        pid,proc_root=proc_root,
        cgroup_root=cgroup_root)
    if sample is not None:
        return sample
    tree=process_resource_probe.process_tree_sample(
        int(pid),proc_root=proc_root)
    if tree is None:
        return None
    prefix=str(int(pid))+":"
    root_identity=next(
        (
            str(item["identity"])
            for item in tree.get("processes",())
            if str(item.get("identity","")).startswith(prefix)
        ),
        None,
    )
    if root_identity is None:
        return None
    return dict(
        mode="process_tree",
        identity=_scope_fingerprint(
            "process-tree-root:"+root_identity),
        memory_metric="process_tree_rss",
        memory_bytes=int(
            tree.get("rss_bytes",0)),
        process_count=int(
            tree.get("process_count",0)),
        process_sample=tree,
    )


def new_tracker(selection=None):
    selection=dict(selection or {})
    return dict(
        supported=None,
        scope_stable=True,
        mode=None,
        identity=None,
        samples=0,
        peak_memory_bytes=0,
        peak_processes=0,
        limits_seen=False,
        limits_stable=True,
        cpu_quota_cores=None,
        memory_limit_bytes=None,
        selection_source=str(
            selection.get("source") or ""),
        selection_reason=str(
            selection.get("reason") or ""),
        cgroup_first=None,
        cgroup_last=None,
        process_first={},
        process_last={},
    )


def observe(tracker,sample):
    if sample is None:
        if tracker["supported"] is None:
            tracker["supported"]=False
        return tracker
    tracker["supported"]=True
    mode=str(sample.get("mode") or "")
    identity=str(sample.get("identity") or "")
    if tracker["mode"] is None:
        tracker["mode"]=mode
        tracker["identity"]=identity
    elif (
        tracker["mode"]!=mode
        or tracker["identity"]!=identity
    ):
        tracker["scope_stable"]=False
        return tracker
    tracker["samples"]+=1
    tracker["peak_memory_bytes"]=max(
        int(tracker["peak_memory_bytes"]),
        int(sample.get("memory_bytes",0)))
    tracker["peak_processes"]=max(
        int(tracker["peak_processes"]),
        int(sample.get("process_count",0)))
    if mode=="cgroup_v2":
        cpu_quota=sample.get(
            "cpu_quota_cores")
        memory_limit=sample.get(
            "memory_limit_bytes")
        if not tracker["limits_seen"]:
            tracker["limits_seen"]=True
            tracker["cpu_quota_cores"]=cpu_quota
            tracker["memory_limit_bytes"]=memory_limit
        elif (
            tracker["cpu_quota_cores"]!=cpu_quota
            or tracker["memory_limit_bytes"]!=memory_limit
        ):
            tracker["limits_stable"]=False
        current=dict(
            cpu_seconds=max(
                0.0,float(
                    sample.get("cpu_seconds",0))),
            read_bytes=max(
                0,int(sample.get("read_bytes",0))),
            write_bytes=max(
                0,int(sample.get("write_bytes",0))),
        )
        if tracker["cgroup_first"] is None:
            tracker["cgroup_first"]=current
        tracker["cgroup_last"]=current
    elif mode=="process_tree":
        tree=sample.get("process_sample") or {}
        first=tracker["process_first"]
        last=tracker["process_last"]
        for item in tree.get("processes",()):
            key=str(item["identity"])
            current=dict(
                cpu_seconds=max(
                    0.0,float(
                        item.get("cpu_seconds",0))),
                read_bytes=max(
                    0,int(item.get("read_bytes",0))),
                write_bytes=max(
                    0,int(item.get("write_bytes",0))),
            )
            if key not in first:
                first[key]=dict(current)
            previous=last.get(key)
            if previous is not None and (
                current["cpu_seconds"]
                    <previous["cpu_seconds"]
                or current["read_bytes"]
                    <previous["read_bytes"]
                or current["write_bytes"]
                    <previous["write_bytes"]
            ):
                tracker["scope_stable"]=False
            last[key]=current
    else:
        tracker["scope_stable"]=False
    return tracker


def report(tracker):
    mode=tracker.get("mode")
    cpu=0.0
    read_bytes=0
    write_bytes=0
    identities=0
    peak_processes=int(
        tracker.get("peak_processes",0))
    if mode=="cgroup_v2":
        first=tracker.get("cgroup_first") or {}
        last=tracker.get("cgroup_last") or {}
        cpu=max(
            0.0,float(last.get("cpu_seconds",0))
            -float(first.get("cpu_seconds",0)))
        read_bytes=max(
            0,int(last.get("read_bytes",0))
            -int(first.get("read_bytes",0)))
        write_bytes=max(
            0,int(last.get("write_bytes",0))
            -int(first.get("write_bytes",0)))
        identities=1 if tracker.get("identity") else 0
    elif mode=="process_tree":
        first=tracker.get("process_first",{})
        last=tracker.get("process_last",{})
        identities=len(last)
        for key,current in last.items():
            baseline=first.get(key,{})
            cpu+=max(
                0.0,float(
                    current.get("cpu_seconds",0))
                -float(
                    baseline.get("cpu_seconds",0)))
            read_bytes+=max(
                0,int(
                    current.get("read_bytes",0))
                -int(
                    baseline.get("read_bytes",0)))
            write_bytes+=max(
                0,int(
                    current.get("write_bytes",0))
                -int(
                    baseline.get("write_bytes",0)))
    return dict(
        supported=bool(tracker.get("supported")),
        scope_stable=bool(
            tracker.get("scope_stable",False)),
        limits_stable=bool(
            tracker.get("limits_stable",False)),
        cpu_quota_cores=(
            tracker.get("cpu_quota_cores")
            if tracker.get("limits_seen")
            else None),
        memory_limit_bytes=(
            tracker.get("memory_limit_bytes")
            if tracker.get("limits_seen")
            else None),
        mode=mode,
        scope_fingerprint=(
            str(tracker.get("identity") or "")
            if tracker.get("identity")
            else None),
        selection_source=str(
            tracker.get("selection_source") or ""),
        selection_reason=str(
            tracker.get("selection_reason") or ""),
        samples=int(tracker.get("samples",0)),
        process_identities_seen=identities,
        peak_processes=peak_processes,
        peak_memory_bytes=int(
            tracker.get("peak_memory_bytes",0)),
        cpu_seconds=float(cpu),
        read_bytes=int(read_bytes),
        write_bytes=int(write_bytes),
    )
