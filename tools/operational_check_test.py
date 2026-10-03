#!/usr/bin/env python3
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import operational_check


class OperatingCheckTest(unittest.TestCase):
    def setUp(self):
        self.summary = dict(event="metrics", run_id="synthetic-run", timestamp=1000,
                            state=dict(pending_bytes=0, oldest_queue_seconds=0,
                                       source_apply_pending_bytes=0, merge_uncertain_rows=0,
                                       health="normal", quarantined_tables={}))
        self.status = dict(format_version=1, state_exists=True, jobs=dict(merge_uncertain=0))

    def check(self, summary=None, status=None, free=100):
        return operational_check.evaluate(summary or self.summary, status or self.status,
                                          free, 1000, 60, 20, 50, 30)

    def test_fresh_evidence_and_configured_limits(self):
        self.assertEqual(self.check()["exit_code"], 0)
        self.summary["state"]["pending_bytes"] = 51
        self.summary["state"]["oldest_queue_seconds"] = 31
        self.assertEqual(self.check(free=19)["alerts"], ["low_disk", "backlog_bytes", "backlog_age"])
        self.summary["state"]["pending_bytes"] = 0
        self.summary["state"]["source_apply_pending_bytes"] = 51
        self.assertIn("backlog_bytes", self.check()["alerts"])

    def test_durable_unknown_output_is_never_masked_by_runtime_health(self):
        self.status["jobs"]["merge_uncertain"] = 1
        value = self.check()
        self.assertEqual(value["exit_code"], 1)
        self.assertIn("unknown_output_isolated", value["alerts"])
        self.assertEqual(value["merge_uncertain_rows"], 1)
        self.summary["state"]["quarantined_tables"] = {"synthetic_target": dict(reason="unknown")}
        self.assertEqual(self.check()["quarantined_targets"], 1)

    def test_recent_terminal_summary_still_means_stopped(self):
        self.summary["event"] = "run_summary"
        self.summary["state"]["errors"] = ["synthetic failure"]
        value = self.check()
        self.assertIn("daemon_stopped", value["alerts"])
        self.assertIn("worker_errors", value["alerts"])

    def test_missing_stale_future_or_malformed_evidence_is_unknown(self):
        for change in (dict(timestamp=900), dict(timestamp=1010), dict(timestamp=float("nan")),
                       dict(event="unexpected"), dict(state={}), dict(run_id=None)):
            summary = dict(self.summary, **change)
            with self.assertRaises(ValueError):
                self.check(summary=summary)
        for value in (-1, None, True, "0", float("inf")):
            summary = copy.deepcopy(self.summary)
            summary["state"]["merge_uncertain_rows"] = value
            with self.assertRaises(ValueError):
                self.check(summary=summary)
        with self.assertRaises(ValueError):
            self.check(status=dict(self.status, state_exists=False))

    def test_cli_missing_sample_and_recent_stopped_report_exit_distinctly(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            args = [sys.executable, str(Path(operational_check.__file__)),
                    "--summary", str(root/"summary.json"), "--status", str(root/"status.json"),
                    "--state-directory", td, "--min-free-bytes", "0",
                    "--max-pending-bytes", "50", "--max-queue-seconds", "30"]
            result = subprocess.run(args, capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertFalse(json.loads(result.stdout)["availability_known"])
            self.summary.update(timestamp=time.time(), event="run_summary")
            (root/"summary.json").write_text(json.dumps(self.summary))
            (root/"status.json").write_text(json.dumps(self.status))
            result = subprocess.run(args, capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(json.loads(result.stdout)["alerts"], ["daemon_stopped"])


if __name__ == "__main__":
    unittest.main()
