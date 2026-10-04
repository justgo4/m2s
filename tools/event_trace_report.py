#!/usr/bin/env python3
"""Summarize correlated observations; this is not a performance certification gate."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import cdc_event_trace as trace


def quantiles(values):
    values=sorted(values)
    def at(fraction):
        index=(len(values)-1)*fraction
        low=int(index);high=min(low+1,len(values)-1)
        return values[low]+(values[high]-values[low])*(index-low)
    return dict(n=len(values),p50=at(.5),p95=at(.95),p99=at(.99),max=max(values)) if values else dict(n=0)


def analyze(records,max_events=200000):
    unique={};loss={};metadata={};disabled=0;analysis_dropped=0
    for record in records:
        snapshot=record.get('event_trace',record)
        if not snapshot.get('enabled'):
            disabled+=1
            continue
        epoch=snapshot['epoch'];instance=snapshot['instance']
        scope=(epoch,instance)
        loss[scope]=max(loss.get(scope,0),snapshot.get('dropped',0))
        metadata[scope]=max(metadata.get(scope,0),snapshot.get('metadata_errors',0))
        for event in snapshot['events']:
            key=(*scope,event['id'])
            if key in unique:
                if unique[key]!=event:
                    raise ValueError('conflicting event identity in repeated snapshots')
                continue
            if len(unique)>=max_events:
                analysis_dropped+=1
                continue
            if event['stage'] not in trace.STAGES:
                raise ValueError('unknown event stage')
            unique[key]=dict(event)
    source={};deliveries=defaultdict(list)
    for key,event in unique.items():
        scope=key[:2]
        if event['stage'] in {'source_durable','base_applied'}:
            source[(*scope,event['seq'],event['stage'])]=event['mono']
        if 'delivery' in event:
            deliveries[(*scope,event['target'],event['delivery'])].append(event)
    segments=defaultdict(list);incomplete=0;clock_conflicts=0;complete=0
    def add(name,start,end):
        nonlocal clock_conflicts
        if end<start:
            clock_conflicts+=1
        else:
            segments[name].append(end-start)
    for key,durable in source.items():
        if key[-1]=='source_durable':
            applied=source.get((*key[:-1],'base_applied'))
            if applied is not None:add('source_to_base',durable,applied)
    for key,events in deliveries.items():
        events.sort(key=lambda event:event['id'])
        first={};last={};open_spans={}
        for event in events:
            stage=event['stage'];first.setdefault(stage,event);last[stage]=event
            for start,end,name,field in [
                ('prepare_begin','prepare_end','prepare',None),
                ('http_begin','http_accepted','http_request','part'),
                ('http_accepted','acceptance_saved','acceptance_persist','part'),
                ('visible_wait_begin','remote_visible','remote_visibility_wait','txn'),
                ('remote_visible','visible_saved','visible_persist','txn'),
                ('ack_begin','ack_end','ack_local',None)]:
                span=(name,event.get(field) if field else None)
                if stage==start:
                    if name=='remote_visibility_wait':open_spans.setdefault(span,event['mono'])
                    else:open_spans[span]=event['mono']
                if stage==end and span in open_spans:
                    add(name,open_spans.pop(span),event['mono'])
        if 'delivery_selected' not in first or 'ack_end' not in last:
            incomplete+=1
            continue
        complete+=1
        selected=first['delivery_selected'];ack=last['ack_end']
        add('selected_to_ack',selected['mono'],ack['mono'])
        for seq in selected['sequences']:
            durable=source.get((*key[:2],seq,'source_durable'))
            if durable is not None:
                add('source_to_selected',durable,selected['mono'])
                add('source_to_ack',durable,ack['mono'])
    return dict(kind='m2s_correlated_trace_diagnostic',certification=False,
        clock_scope='within same process instance only',
        coverage='sampled after-durable observations; no MySQL commit/queryable guarantee; task_frontier is an upper-bound observation',
        instances=len(loss),disabled_records=disabled,unique_events=len(unique),
        ring_evictions=sum(loss.values()),metadata_errors=sum(metadata.values()),
        analysis_dropped=analysis_dropped,clock_order_conflicts=clock_conflicts,
        complete_delivery_observations=complete,incomplete_delivery_observations=incomplete,
        stages={name:quantiles(values) for name,values in sorted(segments.items())})


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('metrics',type=Path,nargs='+')
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    def records():
        for path in args.metrics:
            with path.open() as handle:
                for line in handle:
                    if line.strip():yield json.loads(line)
    result=analyze(records())
    text=json.dumps(result,sort_keys=True,indent=2)+'\n'
    if args.output:args.output.write_text(text)
    else:print(text,end='',flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
