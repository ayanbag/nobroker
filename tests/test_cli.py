"""The CLI, driven in-process.

``main()`` returns an exit code instead of calling :func:`sys.exit` precisely so
these tests can call it directly. Spawning a subprocess to test our own
command-line parser would be slower and would prove less.
"""

from __future__ import annotations

import io
import json
import logging
import unittest
from contextlib import redirect_stderr, redirect_stdout

from nobroker import Queue
from nobroker.cli import main

from .support import TempQueueTest

CALLS: list[str] = []


def handler(job) -> None:
    """Handler target for ``nobroker work tests.test_cli:handler``."""
    CALLS.append(job.id)


def failing_handler(job) -> None:
    raise RuntimeError("nope")


class CliTest(TempQueueTest):
    def setUp(self) -> None:
        super().setUp()
        CALLS.clear()
        logging.disable(logging.CRITICAL)
        self.addCleanup(logging.disable, logging.NOTSET)

    def run_cli(self, *args: str) -> tuple[int, str]:
        """Invoke the CLI against this test's directory and capture stdout."""
        out = io.StringIO()
        with redirect_stdout(out), redirect_stderr(io.StringIO()):
            code = main(["--dir", str(self.dir), *args])
        return code, out.getvalue()

    def run_json(self, *args: str):
        code, out = self.run_cli("--json", *args)
        self.assertEqual(code, 0, out)
        return json.loads(out)

    # -- basics ----------------------------------------------------------

    def test_no_command_prints_help(self) -> None:
        code, out = self.run_cli()
        self.assertEqual(code, 2)
        self.assertIn("usage", out)

    def test_enqueue_and_stats(self) -> None:
        code, out = self.run_cli("enqueue", '{"task": "resize"}')
        self.assertEqual(code, 0)
        self.assertTrue(out.strip(), "enqueue should print the new job id")

        stats = self.run_json("stats")
        self.assertEqual(stats["ready"], 1)
        self.assertEqual(stats["total"], 1)

    def test_plain_text_payloads_do_not_need_quoting(self) -> None:
        self.run_cli("enqueue", "just some text")
        jobs = self.run_json("list")
        self.assertEqual(jobs[0]["payload"], "just some text")

    def test_json_payloads_are_parsed(self) -> None:
        self.run_cli("enqueue", '{"a": [1, 2]}')
        jobs = self.run_json("list")
        self.assertEqual(jobs[0]["payload"], {"a": [1, 2]})

    def test_enqueue_count_and_priority(self) -> None:
        self.run_cli("enqueue", "bulk", "--count", "5", "--priority", "3")
        jobs = self.run_json("list")
        self.assertEqual(len(jobs), 5)
        self.assertTrue(all(job["priority"] == 3 for job in jobs))

    def test_lease_ack_cycle(self) -> None:
        self.run_cli("enqueue", "work")
        leased = self.run_json("lease")
        self.assertEqual(len(leased), 1)
        job_id = leased[0]["id"]

        code, _ = self.run_cli("ack", job_id)
        self.assertEqual(code, 0)
        self.assertEqual(self.run_json("stats")["done"], 1)

    def test_lease_on_an_empty_queue_is_not_an_error(self) -> None:
        code, out = self.run_cli("lease")
        self.assertEqual(code, 0)
        self.assertIn("no eligible jobs", out)

    def test_nack_reports_the_new_state(self) -> None:
        self.run_cli("enqueue", "flaky", "--max-attempts", "1")
        job_id = self.run_json("lease")[0]["id"]
        result = self.run_json("nack", job_id, "--error", "kaboom")
        self.assertEqual(result["state"], "dead")
        self.assertEqual(result["last_error"], "kaboom")

    def test_unknown_job_id_exits_nonzero(self) -> None:
        code, _ = self.run_cli("ack", "does-not-exist")
        self.assertEqual(code, 1)

    # -- operator commands ------------------------------------------------

    def test_dlq_listing_and_requeue(self) -> None:
        self.run_cli("enqueue", "doomed", "--max-attempts", "1")
        job_id = self.run_json("lease")[0]["id"]
        self.run_cli("nack", job_id)

        dead = self.run_json("dlq")
        self.assertEqual([job["id"] for job in dead], [job_id])

        result = self.run_json("dlq", "--requeue")
        self.assertEqual(result["requeued"], 1)
        self.assertEqual(self.run_json("stats")["ready"], 1)

    def test_empty_dlq_says_so(self) -> None:
        code, out = self.run_cli("dlq")
        self.assertEqual(code, 0)
        self.assertIn("empty", out)

    def test_purge_requires_confirmation(self) -> None:
        self.run_cli("enqueue", "x")
        result = self.run_json("purge", "--yes")
        self.assertEqual(result["purged"], 1)
        self.assertEqual(self.run_json("stats")["total"], 0)

    def test_compact(self) -> None:
        self.run_cli("enqueue", "a", "--count", "10")
        job_ids = [job["id"] for job in self.run_json("lease", "-n", "10")]
        for job_id in job_ids:
            self.run_cli("ack", job_id)

        result = self.run_json("compact")
        self.assertEqual(result["jobs_dropped"], 10)
        self.assertLess(result["bytes_after"], result["bytes_before"])

    def test_inspect_shows_the_raw_log(self) -> None:
        """The durability story has to be checkable from a terminal."""
        self.run_cli("enqueue", "a")
        job_id = self.run_json("lease")[0]["id"]
        self.run_cli("ack", job_id)

        records = self.run_json("inspect")
        self.assertEqual(
            [record["type"] for record in records], ["ENQUEUE", "LEASE", "ACK"]
        )
        self.assertEqual(records[0]["offset"], 16, "records start after the header")

    def test_recover_reports_a_clean_log(self) -> None:
        self.run_cli("enqueue", "a", "--count", "3")
        report = self.run_json("recover")
        self.assertFalse(report["repaired"])
        self.assertEqual(report["damage"], "none")
        self.assertEqual(report["total"], 3)

    def test_recover_reports_damage(self) -> None:
        self.run_cli("enqueue", "a", "--count", "3")
        path = sorted(self.dir.glob("default.*.log"))[-1]
        path.write_bytes(path.read_bytes() + b"garbage that is not a record")

        report = self.run_json("recover")
        self.assertTrue(report["repaired"])
        self.assertGreater(report["bytes_discarded"], 0)

    # -- worker and bench -------------------------------------------------

    def test_work_runs_a_handler(self) -> None:
        self.run_cli("enqueue", "a", "--count", "4")
        stats = self.run_json(
            "work", "tests.test_cli:handler", "--idle-timeout", "0"
        )
        self.assertEqual(stats["succeeded"], 4)
        self.assertEqual(len(CALLS), 4)

    def test_work_reports_failures(self) -> None:
        self.run_cli("enqueue", "a", "--max-attempts", "1")
        stats = self.run_json(
            "work", "tests.test_cli:failing_handler", "--idle-timeout", "0"
        )
        self.assertEqual(stats["dead"], 1)

    def test_a_bad_handler_path_is_a_clear_error(self) -> None:
        code, _ = self.run_cli("work", "not-a-path")
        self.assertEqual(code, 1)
        code, _ = self.run_cli("work", "tests.test_cli:no_such_function")
        self.assertEqual(code, 1)

    def test_bench_runs(self) -> None:
        results = self.run_json("bench", "--jobs", "20")
        self.assertTrue(results)
        self.assertTrue(all("ops_per_sec" in row for row in results))

    def test_named_queues_are_addressable(self) -> None:
        code, _ = self.run_cli("--queue", "emails", "enqueue", "send")
        self.assertEqual(code, 0)
        self.assertEqual(self.run_json("stats")["total"], 0)
        code, out = self.run_cli("--json", "--queue", "emails", "stats")
        self.assertEqual(json.loads(out)["total"], 1)


if __name__ == "__main__":
    unittest.main()
