#!/usr/bin/env python3
"""Real child-process topology forwarding and complete independent bag oracle."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT=Path(__file__).with_name("join_bridge_stream_benchmark.py")


class BenchmarkTest(unittest.TestCase):
    def test_topology_and_candidate_are_isolated(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/"report.json"
            subprocess.run([sys.executable,str(SCRIPT),"--rows","101","--repeats","1",
                            "--configured-candidate","--partitions","16","--batch-rows","50",
                            "--batch-bytes","4096","--max-row-bytes","65536",
                            "--output",str(output)],check=True,capture_output=True,text=True)
            report=json.loads(output.read_text())
            self.assertTrue(report["exact_match"])
            self.assertEqual({x["mode"] for x in report["runs"]},
                             {"cache","stream","configured-stream"})
            self.assertEqual(len({x["bag_digest"] for x in report["runs"]}),1)
            for run in report["runs"]:
                self.assertEqual(run["routed_rows"],101)
                self.assertEqual(run["topology"]["key_partitions"],16)
                self.assertEqual(run["topology"]["configured_batch_rows"],50)
                self.assertEqual(run["topology"]["batch_bytes"],4096)
                self.assertEqual(run["topology"]["max_row_bytes"],65536)

    def test_failure_retains_worker_diagnostic(self):
        completed=subprocess.run([sys.executable,str(SCRIPT),"--rows","1","--repeats","1",
                                  "--max-row-bytes","1"],capture_output=True,text=True)
        self.assertNotEqual(completed.returncode,0)
        self.assertIn("isolated cache worker failed",completed.stderr)
        self.assertIn("CDC_MAX_ROW_BYTES",completed.stderr)


if __name__=="__main__":
    unittest.main()
