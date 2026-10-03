#!/usr/bin/env python3
"""Real runner/SQLite idle pacing with unacknowledged output and shared lag."""
from decimal import Decimal
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
sys.path.insert(0,str(ROOT/"tools"))
import stateful_worker_contention_test as contention
import aggregate_runtime_test as fixture
import aggregate_task_catalog
import aggregate_task_runner
import j4
import source_state


class StatefulIdleTest(unittest.TestCase):
    setUp=contention.StatefulContentionTest.setUp

    def activate(self):
        while True:
            result=aggregate_task_runner.step(self.con,self.task["task_id"],self.cfg,
                                             mapping=self.item["mapping"],bootstrap_limit=1)
            fixture.ack_all(self.con)
            if result["phase"]=="ready":
                j4.stateful_physical_registry.sync_runtime_result(self.con,self.item,result)
                return

    def run_until_wait(self,on_wait=None):
        results=[]
        waits=[]
        stop=self.runtime["stop"]
        def step(*args,**kwargs):
            result=self.step(*args,**kwargs)
            results.append(result)
            if len(results)>=10:
                stop.set()
            return result
        def wait(timeout):
            waits.append((timeout,results[-1] if results else None))
            if on_wait is None or not on_wait(results,waits):
                stop.set()
            return stop.is_set()
        with patch.object(j4,"stateful_rebuild_guarded_step",side_effect=step), \
             patch.object(j4,"wake_loaders") as wake, \
             patch.object(j4,"log") as logs, \
             patch.object(stop,"wait",side_effect=wait):
            j4.stateful_task_worker(self.item,self.cfg,self.runtime)
        self.assertTrue(waits,"worker spun without waiting on its unchanged source prefix")
        self.assertEqual(waits[0][0],0.05)
        self.assertEqual(wake.call_count,len(results))
        self.assertTrue(self.con.execute("SELECT 1 FROM active_jobs LIMIT 1").fetchone())
        return results,waits,logs

    def test_catchup_waits_on_pending_bootstrap_output_without_pacing_chunks(self):
        results,waits,_=self.run_until_wait()
        self.assertEqual(len(results),3)
        self.assertTrue(all(x["consumer"] is None for x in results[:-1]))
        self.assertEqual(results[-1]["phase"],"catchup")
        self.assertEqual(results[-1]["consumer"]["watermark"],0)
        self.assertEqual(len(waits),1)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM aggregate_output_commits").fetchone()[0],1)

    def test_ready_waits_with_pending_cdc_jobs(self):
        self.activate()
        fixture.add_commit(self.con,[(3,"a",Decimal("7.00"),1,0)],100)
        results,_,_=self.run_until_wait()
        self.assertEqual(len(results),1)
        self.assertEqual(results[0]["phase"],"ready")
        self.assertEqual(results[0]["consumer"]["watermark"],1)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM aggregate_output_commits").fetchone()[0],2)

    def test_shared_leader_wait_then_copies_new_prefix_without_per_commit_wait(self):
        self.activate()
        leader=self.task
        follower=aggregate_task_catalog.register_task(
            self.con,"follower","follower_sink",32,fixture.ir(),"follower_sink",
            "follower_state","follower_consumer",fixture.target_schema())
        self.task=follower
        self.item=dict(kind="aggregate",task=follower,mapping=fixture.mapping(follower))
        self.runtime["stateful_active_task_ids"]={follower["task_id"]}
        # Bind and acknowledge the follower's bootstrap at W=0.
        self.activate()
        for seq in range(1,4):
            fixture.add_commit(self.con,[(seq+2,"a",Decimal("1.00"),1,0)],100+seq)
        def advance_leader(results,waits):
            if len(waits)>1:
                return False
            self.assertEqual([x["consumer"]["watermark"] for x in results],[0,0])
            for _ in range(3):
                aggregate_task_runner.step(self.con,leader["task_id"],self.cfg,
                                           mapping=fixture.mapping(leader))
            return True
        results,waits,logs=self.run_until_wait(advance_leader)
        self.assertEqual([x["consumer"]["watermark"] for x in results],[0,0,1,2,3])
        self.assertEqual(len(waits),2)
        reuse=[x for x in logs.call_args_list if "STATEFUL PHYSICAL REUSE" in x.args[0]]
        self.assertEqual(len(reuse),1)
        self.assertEqual(source_state.consumer_info(self.con,follower["consumer_id"])["watermark"],3)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM aggregate_output_commits WHERE consumer_id=?",
                                         (follower["consumer_id"],)).fetchone()[0],4)


if __name__=="__main__":
    unittest.main()
