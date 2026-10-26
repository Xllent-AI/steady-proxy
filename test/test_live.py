"""Run the actual live harness with disposable clients; no Docker or billed calls."""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
BASH = shutil.which("bash")

# Every child reads its fixture from disk because live.sh deliberately clears
# the environment passed to Claude. These executables provide observations,
# not copies of the harness's pass/fail conditions.
CLIENT = r'''
import json
import os
from pathlib import Path
import signal
import sys

root = Path(__ROOT__)
fixture = json.loads((root / "fixture.json").read_text())
name = Path(sys.argv[0]).name
args = sys.argv[1:]
with (root / "calls.jsonl").open("a") as calls:
    calls.write(json.dumps({"name": name, "args": args,
                           "keepalive": os.environ.get("PROXY_KEEPALIVE_MS")}) + "\n")
state = root / "fault"
container_log = root / "container.log"

def fault():
    return state.read_text() if state.exists() else ""

if name == "mktemp":
    directory = root / "logs"
    directory.mkdir()
    print(directory)
elif name == "docker":
    if "port" in args:
        print("127.0.0.1:18789" if "proxy" in args else "127.0.0.1:19099")
    elif "up" in args and "--force-recreate" in args:
        container_log.write_text("")
    elif "logs" in args:
        print(container_log.read_text(), end="")
        sys.exit(fixture.get("logs_exit", 0))
elif name == "timeout":
    if any("[[FAULT=permanent]]" in arg for arg in args):
        state.write_text("permanent")
        if fixture.get("signal"):
            os.kill(os.getpid(), signal.SIGTERM)
        if "launcher_exit" in fixture:
            sys.exit(fixture["launcher_exit"])
    os.execvp(args[1], args[1:])
elif name == "claude":
    prompt = args[args.index("-p") + 1]
    kind = prompt.split("[[FAULT=", 1)[1].split("]]", 1)[0]
    state.write_text(kind)
    if kind == "permanent":
        print("API Error: 401 authentication_error", file=sys.stderr)
        sys.exit(fixture.get("client_exit", 1))
    with container_log.open("a") as logs:
        if kind.startswith("slow:"):
            logs.write(fixture.get("long_logs", "2026/10/26 12:00:00 OK    sonnet->mock  in=1 out=1 tok  end_turn  buffered  5m10s\n"))
        else:
            logs.write("2026/10/26 12:00:00 OK    sonnet->mock  in=1 out=1 tok  end_turn  buffered  1s\n")
    if kind.startswith("slow:"):
        print(fixture.get("long_output", "MOCK_OK"))
        sys.exit(fixture.get("long_exit", 0))
    print("MOCK_OK")
elif name == "curl":
    url = args[-1]
    if "/__reset" in url:
        state.write_text("")
    elif "/__count" in url:
        if fault() == "permanent":
            print(fixture.get("count", "1"), end="")
            sys.exit(fixture.get("count_exit", 0))
        if fault().startswith("slow:"):
            print(fixture.get("long_count", "1"), end="")
        else:
            print("3" if "then-ok" in fault() else "1", end="")
else:
    raise RuntimeError("Unexpected fake executable: " + name)
'''


class LiveHarnessTests(unittest.TestCase):
    def run_harness(self, fixture=None, *, long=False, missing_timeout=False):
        with tempfile.TemporaryDirectory(prefix="steady-live-regression-") as temporary:
            root = Path(temporary)
            (root / "fixture.json").write_text(json.dumps(fixture or {}))
            binary = root / "bin"
            binary.mkdir()
            for name in ("docker", "curl", "claude", "timeout", "mktemp"):
                if name == "timeout" and missing_timeout:
                    continue
                executable = binary / name
                executable.write_text(
                    "#!" + sys.executable + "\n" + CLIENT.replace("__ROOT__", repr(str(root)))
                )
                executable.chmod(0o755)
            environment = {
                "PATH": str(binary) + ("" if missing_timeout else ":/usr/bin:/bin"),
                "HOME": str(root),
                "DO_REAL": "0",
                "DO_LONG": "1" if long else "0",
                # The long probe must deliberately override a shorter hold.
                "PROXY_KEEPALIVE_MS": "1000",
            }
            result = subprocess.run(
                [BASH, str(ROOT / "test/live.sh")],
                cwd=ROOT, env=environment, text=True, capture_output=True, timeout=20,
            )
            calls_path = root / "calls.jsonl"
            calls = [json.loads(line) for line in calls_path.read_text().splitlines()] if calls_path.exists() else []
            return result, calls

    def assert_passed(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("ALL LIVE TESTS PASSED", result.stdout)

    def assert_failed(self, result, scenario):
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn("ALL LIVE TESTS PASSED", result.stdout)
        self.assertRegex(result.stdout, "FAIL.*" + scenario)

    def test_normal_permanent_client_error_passes_after_one_attempt(self):
        result, _ = self.run_harness()
        self.assert_passed(result)

    def test_successful_client_exit_is_not_a_permanent_error(self):
        result, _ = self.run_harness({"client_exit": 0})
        self.assert_failed(result, "permanent-no-storm")

    def test_launcher_and_watchdog_statuses_fail_even_after_one_attempt(self):
        for status in (124, 125, 126, 127, 128, 137, 143, 255):
            with self.subTest(status=status):
                result, _ = self.run_harness({"launcher_exit": status})
                self.assert_failed(result, "permanent-no-storm")

    def test_signaled_launcher_fails(self):
        result, _ = self.run_harness({"signal": True})
        self.assert_failed(result, "permanent-no-storm")

    def test_missing_or_invalid_attempt_counts_fail(self):
        for count in ("", "0", "2", "-1", "garbage", "1 1", "1\n2"):
            with self.subTest(count=count):
                result, _ = self.run_harness({"count": count})
                self.assert_failed(result, "permanent-no-storm")

    def test_count_transport_and_http_errors_fail_even_with_one_in_body(self):
        for status in (7, 22, 28):
            with self.subTest(status=status):
                result, _ = self.run_harness({"count_exit": status})
                self.assert_failed(result, "permanent-no-storm")

    def test_missing_timeout_fails_before_starting_stack(self):
        result, calls = self.run_harness(missing_timeout=True)
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("missing required tool: timeout", result.stdout)
        self.assertEqual(calls, [])

    def test_long_probe_overrides_window_and_accepts_buffered_claude_success(self):
        result, calls = self.run_harness(long=True)
        self.assert_passed(result)
        overrides = [call for call in calls if call["name"] == "docker" and "--no-deps" in call["args"]]
        self.assertEqual(len(overrides), 1)
        self.assertEqual(overrides[0]["keepalive"], "600000")
        self.assertIn("--force-recreate", overrides[0]["args"])
        requests = [call for call in calls if call["name"] == "claude" and any("[[FAULT=slow:310]]" in arg for arg in call["args"])]
        self.assertEqual(len(requests), 1)

    def test_long_probe_rejects_live_commit_even_with_successful_client(self):
        result, _ = self.run_harness({"long_logs": "2026/10/26 12:00:00 OK    sonnet->mock  in=1 out=1 tok  end_turn  live  5m10s\n"}, long=True)
        self.assert_failed(result, "long-gen-310s")

    def test_long_probe_rejects_failed_or_incomplete_observations(self):
        for fixture in (
            {"long_logs": ""},
            {"long_logs": "2026/10/26 12:00:00 DROP  sonnet  truncated_stream -> committed\n"},
            {"long_logs": "2026/10/26 12:00:00 OK    sonnet buffered 5m10s\n2026/10/26 12:00:01 OK    sonnet live 1s\n"},
            {"long_logs": "2026/10/26 12:00:00 OK    sonnet buffered 5m10s\n2026/10/26 12:00:01 OK    sonnet buffered 1s\n"},
            {"logs_exit": 1},
            {"long_exit": 124},
            {"long_output": ""},
            {"long_count": "0"},
            {"long_count": "2"},
        ):
            with self.subTest(fixture=fixture):
                result, _ = self.run_harness(fixture, long=True)
                self.assert_failed(result, "long-gen-310s")


if __name__ == "__main__":
    unittest.main()
