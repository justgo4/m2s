#!/usr/bin/env python3
from pathlib import Path
import io
import json
import sys
import unittest
from unittest.mock import patch
import urllib.error

sys.path.insert(0, str(Path(__file__).resolve().parent))
import runner_inventory


class RunnerInventoryTest(unittest.TestCase):
    def fetch(self, payload):
        return patch.object(runner_inventory.urllib.request, "urlopen",
                            return_value=io.BytesIO(json.dumps(payload).encode()))

    def test_counts_without_private_host_metadata(self):
        with self.fetch(dict(total_count=2, runners=[
                dict(name="private-name", labels=["private-label"], status="online", busy=False),
                dict(name="another-host", status="offline", busy=False)])):
            value = runner_inventory.inventory("owner/repo", "CHANGE_ME_TEST_TOKEN")
        self.assertEqual(value["total_registered"], 2)
        self.assertEqual(value["listed_online_idle"], 1)
        self.assertTrue(value["complete"])
        self.assertNotIn("private-name", json.dumps(value))
        self.assertNotIn("private-label", json.dumps(value))

    def test_permission_denial_is_unknown_not_zero_runners(self):
        error = urllib.error.HTTPError("url", 403, "forbidden", {}, None)
        with patch.object(runner_inventory.urllib.request, "urlopen", side_effect=error):
            value = runner_inventory.inventory("owner/repo", "CHANGE_ME_TEST_TOKEN")
        self.assertFalse(value["availability_known"])
        self.assertIsNone(value["total_registered"])
        self.assertEqual(value["http_status"], 403)

    def test_zero_and_partial_inventory_are_distinct(self):
        with self.fetch(dict(total_count=0, runners=[])):
            self.assertEqual(runner_inventory.inventory("owner/repo", "CHANGE_ME_TEST_TOKEN")["total_registered"], 0)
        with self.fetch(dict(total_count=101, runners=[dict(status="online", busy=True)])):
            self.assertFalse(runner_inventory.inventory("owner/repo", "CHANGE_ME_TEST_TOKEN")["complete"])

    def test_missing_token_and_invalid_response(self):
        with patch.object(runner_inventory.urllib.request, "urlopen") as fetch:
            value = runner_inventory.inventory("owner/repo", None)
            fetch.assert_not_called()
        self.assertFalse(value["availability_known"])
        with self.fetch(dict(total_count="zero", runners=[])):
            self.assertFalse(runner_inventory.inventory("owner/repo", "CHANGE_ME_TEST_TOKEN")["accessible"])
        with self.assertRaises(ValueError):
            runner_inventory.inventory("owner/repo?redirect=elsewhere", "CHANGE_ME_TEST_TOKEN")


if __name__ == "__main__":
    unittest.main()
