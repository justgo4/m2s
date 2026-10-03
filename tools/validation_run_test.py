#!/usr/bin/env python3
"""Durability/interruption protocol tests; no database or Arrow is needed."""
from pathlib import Path
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT/"tools"))
import p11_profile
import validation_profiles
import validation_run


REVISION = "a" * 40
ORIGINAL_REVISION = validation_run.revision


def finished_report(expected):
    fields = dict(load_mode="protocol", rows="initial_rows", duration_seconds="source_schedule_seconds")
    result = {fields.get(key, key): value for key, value in expected["parameters"].items()}
    result.update(kind="m2s_longhaul_workload", workload_profile="custom",
                  software_fingerprint=dict(code_revision=REVISION), work_directory_persistent=True)
    return result


class ValidationRunTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)/"run"
        override = patch.object(validation_run, "revision", return_value=REVISION)
        override.start()
        self.addCleanup(override.stop)

    def expected(self):
        return validation_profiles.plan("smoke", ROOT, self.directory)

    def fake_stages(self, fail_gate=False):
        execute = validation_run.execute_stage
        workload_calls = []

        def stage(directory, state, name, command):
            if name == "workload":
                workload_calls.append(name)
                payload = finished_report(self.expected())
                target = self.directory/"workload.json"
            else:
                payload = dict(ok=not fail_gate)
                target = self.directory/"gate.json"
            code = "import pathlib,sys; pathlib.Path(sys.argv[1]).write_text(sys.argv[2]); sys.exit(int(sys.argv[3]))"
            return execute(directory, state, name, [sys.executable, "-c", code,
                           str(target), json.dumps(payload), str(int(name == "gate" and fail_gate))])
        return stage, workload_calls

    def test_profiles_keep_formal_contract_and_copy_parameters(self):
        self.assertEqual(validation_profiles.parameters("p11"), p11_profile.PARAMETERS)
        value = validation_profiles.parameters("smoke")
        value["rows"] = 1
        self.assertEqual(validation_profiles.parameters("smoke")["rows"], 5000)
        with self.assertRaises(ValueError):
            validation_profiles.parameters("p11", "transaction")
        for name in ("soak", "scale-short", "p11"):
            self.assertFalse(validation_profiles.hosted_compatible(name))
        self.assertTrue(validation_profiles.hosted_compatible("medium"))
        formal = validation_profiles.plan("p11", ROOT, self.directory)
        self.assertIn("--certification-profile", formal["workload_command"])
        self.assertIn("--require-profile", formal["gate_command"])
        self.assertNotIn("--require-profile", self.expected()["gate_command"])
        self.assertEqual(self.expected()["gate_thresholds"]["max_p95_seconds"], 30)
        self.assertEqual(validation_profiles.plan("million", ROOT, self.directory)["gate_thresholds"]["max_p95_seconds"], 5)
        self.assertEqual(formal["gate_thresholds"]["max_p99_seconds"], 10)

    def test_existing_run_is_never_initialized_again(self):
        self.directory.mkdir()
        marker = self.directory/"precious"
        marker.write_text("old data")
        with self.assertRaises(FileExistsError):
            validation_run.prepare(self.directory, "smoke")
        self.assertEqual(marker.read_text(), "old data")

    def test_revision_rejects_uncommitted_code_and_untracked_files(self):
        repo = Path(self.temporary.name)/"checkout"
        repo.mkdir()
        def git(*args):
            return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True)
        git("init")
        (repo/"code.py").write_text("print('initial')\n")
        git("add", "code.py")
        git("-c", "user.name=Validator", "-c", "user.email=validator@example.invalid",
            "commit", "-m", "synthetic fixture")
        self.assertEqual(len(ORIGINAL_REVISION(repo)), 40)
        (repo/"code.py").write_text("print('changed')\n")
        with self.assertRaises(RuntimeError):
            ORIGINAL_REVISION(repo)
        git("restore", "code.py")
        (repo/"untracked.py").write_text("print('new')\n")
        with self.assertRaises(RuntimeError):
            ORIGINAL_REVISION(repo)

    def test_atomic_checkpoint_failure_preserves_previous_record(self):
        self.directory.mkdir()
        target = self.directory/"status.json"
        validation_run.atomic_json(target, dict(state="old"))
        with patch.object(validation_run.os, "replace", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                validation_run.atomic_json(target, dict(state="new"))
        self.assertEqual(validation_run.read_json(target), dict(state="old"))
        self.assertEqual(list(self.directory.glob("*.tmp-*")), [])

    def test_complete_run_and_gate_retry_without_reinitialization(self):
        validation_run.prepare(self.directory, "smoke")
        stage, calls = self.fake_stages(fail_gate=True)
        with patch.object(validation_run, "execute_stage", stage):
            self.assertEqual(validation_run.supervise(self.directory), 1)
        self.assertEqual(calls, ["workload"])
        self.assertTrue(validation_run.status(self.directory)["can_resume_gate"])
        original = (self.directory/"workload.json").read_bytes()
        stage, calls = self.fake_stages()
        with patch.object(validation_run, "execute_stage", stage):
            self.assertEqual(validation_run.supervise(self.directory, resume_gate=True), 0)
        self.assertEqual(calls, [])
        self.assertEqual((self.directory/"workload.json").read_bytes(), original)
        self.assertEqual(validation_run.status(self.directory)["state"], "passed")
        self.assertFalse(validation_run.status(self.directory)["can_resume_gate"])
        with self.assertRaises(ValueError):
            validation_run.supervise(self.directory, resume_gate=True)

    def test_interrupted_workload_and_tampered_report_are_not_resumable(self):
        state = validation_run.prepare(self.directory, "smoke")
        validation_run.write_state(self.directory, state, state="interrupted", stage="workload")
        with self.assertRaises(ValueError):
            validation_run.supervise(self.directory, resume_gate=True)
        report = finished_report(self.expected())
        validation_run.atomic_json(self.directory/"workload.json", report)
        validation_run.write_state(self.directory, state, workload_passed=True,
                                  workload_sha256=validation_run.checksum(self.directory/"workload.json"))
        report["initial_rows"] += 1
        validation_run.atomic_json(self.directory/"workload.json", report)
        with self.assertRaises(ValueError):
            validation_run.supervise(self.directory, resume_gate=True)
        self.assertFalse(validation_run.status(self.directory)["can_resume_gate"])

    def test_wrong_report_revision_or_parameter_is_rejected(self):
        for key, value in (("initial_rows", 1), ("workload_profile", p11_profile.NAME),
                           ("software_fingerprint", dict(code_revision="b"*40))):
            report = finished_report(self.expected())
            report[key] = value
            with self.assertRaises(ValueError):
                validation_profiles.validate_report(report, self.expected(), REVISION)

    def test_pid_reuse_does_not_signal_another_process(self):
        state = validation_run.prepare(self.directory, "smoke")
        validation_run.write_state(self.directory, state, supervisor=validation_run.process_identity(os.getpid()))
        with patch.object(validation_run, "alive", side_effect=[True, False]), \
             patch.object(validation_run.signal, "pidfd_send_signal") as send:
            with self.assertRaises(RuntimeError):
                validation_run.cancel(self.directory)
            send.assert_not_called()

    def test_real_cancel_waits_for_child_cleanup_and_records_interruption(self):
        validation_run.prepare(self.directory, "smoke")
        program = '''
import pathlib,sys,time,signal
sys.path.insert(0,sys.argv[1])
import validation_run as run
run.revision=lambda root=run.ROOT: "a"*40
original=run.execute_stage
def stage(directory,state,name,command):
    worker="import pathlib,signal,sys,time; signal.signal(signal.SIGTERM,lambda *a: (_ for _ in ()).throw(KeyboardInterrupt()));\\ntry: time.sleep(30)\\nfinally: pathlib.Path(sys.argv[1]).write_text('cleaned')"
    return original(directory,state,name,[sys.executable,"-c",worker,str(pathlib.Path(directory)/"cleanup")])
run.execute_stage=stage
sys.exit(run.supervise(sys.argv[2]))
'''
        proc = subprocess.Popen([sys.executable, "-c", program, str(ROOT/"tools"), str(self.directory)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                state = validation_run.read_json(self.directory/"status.json")
                if state.get("child") and validation_run.alive(state["child"]):
                    # The child records readiness after its handler is installed.
                    time.sleep(0.2)
                    break
                if proc.poll() is not None:
                    self.fail(proc.communicate()[1].decode())
                time.sleep(0.02)
            else:
                self.fail("supervisor did not start")
            validation_run.cancel(self.directory)
            stdout, stderr = proc.communicate(timeout=10)
            self.assertEqual(proc.returncode, 130, stderr.decode())
            self.assertEqual((self.directory/"cleanup").read_text(), "cleaned")
            state = validation_run.status(self.directory)
            self.assertEqual(state["state"], "interrupted")
            self.assertFalse(state["child_alive"])
            self.assertFalse(state["can_resume_gate"])
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()


if __name__ == "__main__":
    unittest.main()
