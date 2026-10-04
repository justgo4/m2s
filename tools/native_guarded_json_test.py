#!/usr/bin/env python3
"""Bounded native guarded UTF-8 encoding matches existing DuckDB semantics."""
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import pyarrow as pa

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import j4


class NoDuck:
    def unregister(self,*args):
        raise j4.duckdb.InvalidInputException('not registered')

    def register(self,*args):
        raise AssertionError('native-safe projection used DuckDB')

    def execute(self,*args):
        raise AssertionError('native-safe projection used DuckDB')


class GuardedJsonTest(unittest.TestCase):
    def setUp(self):
        self.cfg=dict(duckdb_memory='64MB',key_partitions=16)
        self.engine=j4.transform_engine(self.cfg)
        self.addCleanup(self.engine.close)
        self.mapping=dict(src_table='guarded',sr_table='guarded',primary_key='id',
            sql='SELECT id,payload,required FROM arrow_batch',full_filter=None,
            _schema=[('id',pa.int64()),('payload',pa.large_string()),('required',pa.large_string())],
            _output_columns=['id','payload','required'],_target_sequence=True,
            _target_constraints={
                'payload':dict(target_type='varchar(64)',limit_bytes=64,nullable=True,
                               primary_key=False,value_encoding='utf8'),
                'required':dict(target_type='varchar(64)',limit_bytes=64,nullable=False,
                                primary_key=False,value_encoding='utf8')})
        j4.validate_mapping(self.mapping)

    def raw(self,mutations):
        return j4.route_arrow(self.engine,self.mapping,j4.raw_arrow(self.mapping,mutations),16)

    def wire(self,raw,mode,engine=None):
        with patch.dict(os.environ,CDC_NATIVE_JSON=mode):
            batches=list(j4.transformed_line_batches(
                self.engine if engine is None else engine,self.mapping,raw,
                sequence=77,collect_overflow=True,delivery_dense_order=True))
        wire=b''.join(line.encode() for lines,_ in batches for line in lines.to_pylist())
        return sorted([json.loads(line) for line in wire.splitlines()],key=lambda row:row['id']),batches

    def test_utf8_escapes_nulls_many_chunks_match_duckdb_without_queries(self):
        mutations=[(0,dict(id=i,payload=None if i%3==0 else '中文🙂\n\x00"\\',required='ok'))
                   for i in range(16)]
        raw=self.raw(mutations)
        raw=pa.concat_tables([raw.slice(i,1) for i in range(16)])
        expected,_=self.wire(raw,'off')
        actual,batches=self.wire(raw,'required',NoDuck())
        self.assertEqual(actual,expected)
        self.assertEqual(len(batches),1)
        self.assertEqual(actual,[dict(row[1],__op=0,_cdc_seq=77) for row in mutations])
        self.assertTrue(all(not overflows for _,overflows in batches))

    def test_dense_latest_pk_update_delete_winners_match_duckdb(self):
        old=dict(id=1,payload='old',required='ok')
        new=dict(id=1,payload='new',required='ok')
        deleted=dict(id=2,payload='gone',required='ok')
        raw=self.raw([(0,old),(1,old),(0,new),(0,deleted),(1,deleted)])
        expected,_=self.wire(raw,'off')
        actual,_=self.wire(raw,'required',NoDuck())
        self.assertEqual(actual,expected)
        self.assertEqual(actual,[dict(new,__op=0,_cdc_seq=77),dict(deleted,__op=1,_cdc_seq=77)])

    def test_nullable_and_fatal_overflow_keep_duckdb_diagnostics(self):
        for field,fatal in [('payload',False),('required',True)]:
            row=dict(id=1,payload='ok',required='ok')
            row[field]='汉'*22
            raw=self.raw([(0,row)])
            expected,old_batches=self.wire(raw,'off')
            actual,new_batches=self.wire(raw,'auto')
            self.assertEqual(actual,expected)
            self.assertEqual(new_batches[0][1],old_batches[0][1])
            overflow=new_batches[0][1][0]
            self.assertEqual((overflow['actual_bytes'],overflow['limit_bytes'],overflow['fatal']),(66,64,fatal))
            with self.assertRaisesRegex(RuntimeError,'target overflow guards'):
                self.wire(raw,'required')

    def test_required_null_unusual_encoding_and_size_outputs_stay_guarded(self):
        raw=self.raw([(0,dict(id=1,payload='ok',required=None))])
        self.assertFalse(j4.native_json_constraints_safe(self.mapping,raw))
        expected,_=self.wire(raw,'off')
        actual,_=self.wire(raw,'auto')
        self.assertEqual(actual,expected)
        raw=self.raw([(0,dict(id=1,payload='ok',required='ok'))])
        self.mapping['_target_constraints']['payload']['value_encoding']='base64'
        self.assertFalse(j4.native_json_constraints_safe(self.mapping,raw))
        self.mapping['_target_constraints']['payload']['value_encoding']='utf8'
        self.mapping['_size_columns']={'payload':'payload_bytes'}
        with self.assertRaisesRegex(RuntimeError,'synthetic size columns'):
            self.wire(raw,'required')

    def test_native_windows_bound_rows_bytes_and_preserve_singleton(self):
        table=pa.table(dict(id=range(50),value=['x'*70]*49+['large'*300]))
        table=pa.concat_tables([table.slice(i,1) for i in range(50)])
        batches=list(j4.native_json_batches(table,batch_rows=7,byte_limit=512))
        self.assertEqual(pa.Table.from_batches(batches).to_pylist(),table.to_pylist())
        self.assertTrue(all(batch.num_rows<=7 for batch in batches))
        self.assertTrue(all(batch.nbytes<=512 or batch.num_rows==1 for batch in batches))


if __name__=='__main__':
    unittest.main()
