#!/usr/bin/env python3
"""Pair persisted workload latency samples with sampled source_durable traces.

The gate percentile inputs are not read or changed here. Workload monotonic
time and the daemon monotonic clock are never subtracted. http_accepted is
not treated as StarRocks queryable.
"""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,str(Path(__file__).resolve().parent))
import event_trace_report as report


SPANS=(
    ('prepare_begin','prepare_end','prepare',None),
    ('http_begin','http_accepted','http_request','part'),
    ('http_accepted','acceptance_saved','acceptance_persist','part'),
    ('visible_wait_begin','remote_visible','remote_visibility_wait','txn'),
    ('remote_visible','visible_saved','visible_persist','txn'),
    ('ack_begin','ack_end','ack_local',None),
)


def load_samples(paths):
    samples=[]
    for path in paths:
        with Path(path).open(encoding='utf-8') as handle:
            for line in handle:
                if line.strip():
                    samples.append(json.loads(line))
    return samples


def load_metrics(paths):
    records=[]
    for path in paths:
        with Path(path).open(encoding='utf-8') as handle:
            for line in handle:
                if line.strip():
                    records.append(json.loads(line))
    return records


def load_trace_sidecar(paths):
    """Turn the append-only trace log into one snapshot per daemon instance."""
    groups={}
    order=[]
    for path in paths:
        with Path(path).open(encoding='utf-8') as handle:
            for line in handle:
                if not line.strip():
                    continue
                row=json.loads(line)
                kind=row.get('kind')
                if kind=='m2s_event_trace_header':
                    key=(row['epoch'],row['instance'])
                    if key not in groups:
                        groups[key]=dict(
                            enabled=True,epoch=row['epoch'],instance=row['instance'],
                            dropped=0,metadata_errors=0,events=[])
                        order.append(key)
                elif kind=='m2s_event_trace_event':
                    key=(row['epoch'],row['instance'])
                    group=groups.get(key)
                    if group is None:
                        group=dict(
                            enabled=True,epoch=row['epoch'],instance=row['instance'],
                            dropped=0,metadata_errors=0,events=[])
                        groups[key]=group
                        order.append(key)
                    group['events'].append(row['event'])
    return [groups[key] for key in order]


def _span_seconds(events):
    events=sorted(events,key=lambda event:event['id'])
    open_spans={}
    spans=defaultdict(list)
    conflicts=0
    for event in events:
        stage=event['stage']
        for start,end,name,field in SPANS:
            span=(name,event.get(field) if field else None)
            if stage==start:
                if name=='remote_visibility_wait':
                    open_spans.setdefault(span,event['mono'])
                else:
                    open_spans[span]=event['mono']
            if stage==end and span in open_spans:
                started=open_spans.pop(span)
                if event['mono']<started:
                    conflicts+=1
                else:
                    spans[name].append(event['mono']-started)
    return spans,conflicts


def _latency_block(rows):
    values=[]
    for sample in rows:
        values.append(float(sample['latency_seconds']))
    return dict(samples=len(rows),latency=report.quantiles(values))


def pair(samples,records,max_events=200000):
    unique,stats=report.unique_events(records,max_events=max_events)
    by_marker=defaultdict(list)
    deliveries=defaultdict(list)
    for key,event in unique.items():
        scope=key[:2]
        if event['stage']=='source_durable' and event.get('marker_key'):
            by_marker[event['marker_key']].append(
                (scope,int(event['seq']),event))
        if 'delivery' in event and 'target' in event:
            deliveries[(scope,event['target'],event['delivery'])].append(event)
    seq_deliveries=defaultdict(set)
    for dkey,events in deliveries.items():
        scope=dkey[0]
        for event in events:
            if event['stage']!='delivery_selected':
                continue
            for seq in event.get('sequences') or []:
                seq_deliveries[(scope,int(seq))].add(dkey)
    paired=[]
    unpaired_no_trace=0
    ambiguous=0
    missing_key=0
    mono_disagreements=0
    for sample in samples:
        latency=float(sample['latency_seconds'])
        if ('commit_mono' in sample and sample['commit_mono'] is not None
                and 'observed_mono' in sample and sample['observed_mono'] is not None):
            delta=float(sample['observed_mono'])-float(sample['commit_mono'])
            if abs(delta-latency)>1e-6:
                mono_disagreements+=1
        marker=sample.get('marker_key')
        if not marker:
            missing_key+=1
            continue
        seqs={(scope,seq) for scope,seq,_event in by_marker.get(marker,())}
        if len(seqs)!=1:
            if len(seqs)>1:
                ambiguous+=1
            else:
                unpaired_no_trace+=1
            continue
        scope,seq=next(iter(seqs))
        paired.append((sample,scope,seq))
    segments=defaultdict(list)
    clock_conflicts=0
    attributed=set()
    deliveries_with_paired_seq=0
    for _sample,scope,seq in paired:
        for dkey in seq_deliveries.get((scope,seq),()):
            if dkey in attributed:
                continue
            attributed.add(dkey)
            deliveries_with_paired_seq+=1
            spans,conflicts=_span_seconds(deliveries[dkey])
            clock_conflicts+=conflicts
            for name,values in spans.items():
                segments[name].extend(values)
    paired_ids={id(sample) for sample,_scope,_seq in paired}
    phases={}
    for phase in ('healthy','recovery'):
        rows=[sample for sample in samples if sample.get('phase')==phase]
        hit=[sample for sample in rows if id(sample) in paired_ids]
        missed=[sample for sample in rows if id(sample) not in paired_ids]
        phases[phase]=dict(
            all=_latency_block(rows),
            paired=_latency_block(hit),
            unpaired=_latency_block(missed),
        )
    return dict(
        kind='m2s_event_latency_trace_pairing',
        certification=False,
        clocks='workload monotonic and daemon monotonic are separate and are not subtracted',
        queryable='gate latency ends when the workload SELECT sees the marker id; http_accepted is not queryable; trace ack follows remote VISIBLE',
        coverage='pairing is limited to sampled source_durable events that retained marker_key; unpaired gate samples stay in the latency distribution',
        delivery_stage_attribution='one daemon-clock sample per delivery that contains a paired source seq; the delivery may also contain other seqs',
        latency_samples=len(samples),
        paired_samples=len(paired),
        unpaired_samples=len(samples)-len(paired),
        unpaired_no_trace=unpaired_no_trace,
        ambiguous_marker_keys=ambiguous,
        missing_marker_keys=missing_key,
        latency_mono_disagreements=mono_disagreements,
        deliveries_attributed=deliveries_with_paired_seq,
        trace_clock_conflicts=clock_conflicts,
        trace=stats,
        phases=phases,
        trace_stages={
            name:report.quantiles(values)
            for name,values in sorted(segments.items())},
    )


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--latency',type=Path,nargs='+',required=True)
    parser.add_argument('--metrics',type=Path,nargs='*')
    parser.add_argument('--trace',type=Path,nargs='*')
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    if not args.metrics and not args.trace:
        parser.error('provide --metrics or --trace')
    records=[]
    if args.metrics:
        records.extend(load_metrics(args.metrics))
    if args.trace:
        records.extend(load_trace_sidecar(args.trace))
    result=pair(load_samples(args.latency),records)
    text=json.dumps(result,sort_keys=True,indent=2)+'\n'
    if args.output:
        args.output.write_text(text,encoding='utf-8')
    else:
        print(text,end='',flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
