import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("mqtt_probe", Path(__file__).with_name("mqtt-probe.py"))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class RetryConnectTest(unittest.TestCase):
    @patch.object(probe.time, "sleep")
    @patch.object(probe.random, "uniform", return_value=0.01)
    @patch.object(probe, "connect_attempt", side_effect=["Server unavailable", "Success"])
    def test_retries_temporary_refusal(self, connect, jitter, sleep):
        self.assertEqual(
            probe.retry_connect("device", "token", 1883, "Server unavailable"),
            ["Server unavailable", "Success"],
        )
        self.assertEqual(connect.call_count, 2)
        self.assertEqual(jitter.call_count, 2)
        self.assertEqual(sleep.call_count, 2)

    @patch.object(probe.time, "sleep")
    @patch.object(probe, "connect_attempt", return_value="Bad user name or password")
    def test_does_not_retry_bad_credentials(self, connect, _sleep):
        with self.assertRaisesRegex(RuntimeError, "unexpected CONNACK"):
            probe.retry_connect("device", "token", 1883, "Server unavailable")
        self.assertEqual(connect.call_count, 1)

    @patch.object(probe.time, "sleep")
    @patch.object(probe, "connect_attempt", return_value="Server unavailable")
    def test_retry_is_bounded(self, connect, _sleep):
        with self.assertRaisesRegex(RuntimeError, "bounded reconnect exhausted"):
            probe.retry_connect("device", "token", 1883, "Server unavailable")
        self.assertEqual(connect.call_count, 8)


if __name__ == "__main__":
    unittest.main()
