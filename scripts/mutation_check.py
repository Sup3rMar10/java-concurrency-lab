#!/usr/bin/env python
"""Hand-rolled mutation check: apply one behaviour-breaking edit at a time, run the tests, restore the file.

A mutation is "killed" if the test run fails because of it. Usage (from the repo root):
    python scripts/mutation_check.py            # all mutations
    python scripts/mutation_check.py 3 7        # only mutations #3 and #7
Needs Maven on PATH (or set MVN). Prints a markdown table. Source files are restored byte-for-byte after
every mutation and the script verifies that at the end.
"""
import os
import re
import shutil
import subprocess
import sys

BASE = "src/main/java/com/github/sup3rmar10/concurrencylab/"
POOL = BASE + "pool/BoundedWorkerPool.java"

# (description, file, old text, new text) -- old text must occur exactly once.
MUTATIONS = [
    ("pool: queue-full check `<` -> `<=` (capacity off by one)", POOL,
     "if (queue.size() < capacity) {", "if (queue.size() <= capacity) {"),
    ("pool: CALLER_RUNS behaves like ABORT (wrong reject policy)", POOL,
     "case CALLER_RUNS -> { /* run below, outside the lock */ }",
     'case CALLER_RUNS -> { rejected.incrementAndGet(); throw new RejectedExecutionException("x"); }'),
    ("pool: DISCARD_OLDEST drops the newest queued task instead", POOL,
     "                    queue.pollFirst();\n", "                    queue.pollLast();\n"),
    ("pool: drop the notEmpty signal on enqueue (lost wakeup)", POOL,
     "                queue.addLast(task);\n                notEmpty.signal();\n                return;\n            }\n            switch",
     "                queue.addLast(task);\n                return;\n            }\n            switch"),
    ("pool: shutdown() no longer wakes idle workers", POOL,
     "            shutdown = true;\n            notEmpty.signalAll();\n        } finally {\n            lock.unlock();\n        }\n    }\n\n    /** Stop accepting tasks, drop",
     "            shutdown = true;\n        } finally {\n            lock.unlock();\n        }\n    }\n\n    /** Stop accepting tasks, drop"),
    ("pool: worker dies when a task throws", POOL,
     "            failed.incrementAndGet();\n", "            failed.incrementAndGet();\n            if (true) throw new RuntimeException(t);\n"),
    ("pool: shutdownNow() does not interrupt workers", POOL,
     "            workers.forEach(Thread::interrupt);\n", ""),
    ("counter: SynchronizedCounter.increment loses `synchronized`", BASE + "counter/SynchronizedCounter.java",
     "public synchronized void increment() {", "public void increment() {"),
    ("counter: AtomicCounter does get()+set() instead of incrementAndGet()", BASE + "counter/AtomicCounter.java",
     "value.incrementAndGet();", "value.set(value.get() + 1);"),
    ("counter: LockCounter increments without the lock", BASE + "counter/LockCounter.java",
     "    public void increment() {\n        lock.lock();\n        try {\n            value++;\n        } finally {\n            lock.unlock();\n        }\n    }",
     "    public void increment() {\n        value++;\n    }"),
    ("counter: RacyCounter re-reads the field when writing (race hidden in the forced interleaving)", BASE + "counter/RacyCounter.java",
     "value = v + 1;", "value = value + 1;"),
    ("deadlock: OrderedTransfers locks in argument order (no global order)", BASE + "deadlock/OrderedTransfers.java",
     "Account first = from.id() < to.id() ? from : to;", "Account first = from;"),
    ("deadlock: TimedTransfers second lock waits forever", BASE + "deadlock/TimedTransfers.java",
     "if (!to.lock().tryLock(timeoutMillis, TimeUnit.MILLISECONDS)) return false;", "to.lock().lockInterruptibly();"),
    ("pipeline: retry backoff stops doubling", BASE + "pipeline/Async.java",
     "long delay = base * (1L << Math.min(n - 1, 20));", "long delay = base;"),
    ("pipeline: timeout no longer cancels the slow source", BASE + "pipeline/Async.java",
     "                source.cancel(true);\n            }\n        }, timeoutMs);", "            }\n        }, timeoutMs);"),
    ("pipeline: fanOut fail-fast no longer cancels siblings", BASE + "pipeline/Async.java",
     "if (result.completeExceptionally(unwrap(e))) futures.forEach(o -> o.cancel(true));",
     "result.completeExceptionally(unwrap(e));"),
    ("pipeline: cancelling retry() result no longer cancels in-flight work", BASE + "pipeline/Async.java",
     "if (result.isCancelled()) cancelCurrent.get().run();", "if (false) cancelCurrent.get().run();"),
    ("pipeline: retry gives up one attempt early (off by one)", BASE + "pipeline/Async.java",
     "|| n >= max) {", "|| n >= max - 1) {"),
    ("buffer: enqueue drops the notEmpty signal (lost wakeup)", BASE + "buffer/BoundedBuffer.java",
     "        count++;\n        notEmpty.signal();", "        count++;"),
    ("buffer: dequeue drops the notFull signal (lost wakeup)", BASE + "buffer/BoundedBuffer.java",
     "        count--;\n        notFull.signal();", "        count--;"),
    ("buffer: put() waits with `if` instead of `while`", BASE + "buffer/BoundedBuffer.java",
     "while (count == items.length) notFull.await();", "if (count == items.length) notFull.await();"),
]


def mvn_cmd():
    exe = os.environ.get("MVN") or shutil.which("mvn") or shutil.which("mvn.cmd")
    if not exe:
        home = os.path.expanduser("~/tools/apache-maven-3.9.9/bin/mvn.cmd")
        exe = home if os.path.exists(home) else "mvn"
    return exe


def run_tests():
    # short timeout so a hung (killed) test fails fast instead of waiting the default 30 s
    p = subprocess.run([mvn_cmd(), "-q", "-B", "test", "-Djunit.jupiter.execution.timeout.default=10s"],
                       capture_output=True, text=True, timeout=900)
    out = p.stdout + p.stderr
    if p.returncode == 0:
        return "passed", []
    if "COMPILATION ERROR" in out:
        return "compile-error", []
    tests = sorted(set(re.findall(r"\[ERROR\]\s+(\w+Test\.\w+)", out)))
    return "failed", tests


def main():
    wanted = {int(a) for a in sys.argv[1:]}
    status, _ = run_tests()
    if status != "passed":
        sys.exit("baseline is not green; fix that first")
    originals = {}
    rows = []
    for i, (desc, path, old, new) in enumerate(MUTATIONS, 1):
        if wanted and i not in wanted:
            continue
        with open(path, "rb") as fh:
            raw = fh.read()
        originals.setdefault(path, raw)
        text = raw.decode("utf-8").replace("\r\n", "\n")
        if text.count(old) != 1:
            rows.append((i, desc, "INVALID (pattern count %d)" % text.count(old), ""))
            continue
        try:
            with open(path, "w", encoding="utf-8", newline="\n") as fh:
                fh.write(text.replace(old, new))
            status, tests = run_tests()
        finally:
            with open(path, "wb") as fh:
                fh.write(raw)
        verdict = {"failed": "killed", "passed": "SURVIVED", "compile-error": "INVALID (no compile)"}[status]
        names = ", ".join(t.split(".")[0] + "." + t.split(".")[1] for t in tests[:3]) + (" ..." if len(tests) > 3 else "")
        rows.append((i, desc, verdict, names))
        print("#%d %s -> %s %s" % (i, desc, verdict, names), flush=True)
    for path, raw in originals.items():
        with open(path, "rb") as fh:
            assert fh.read() == raw, "not restored: " + path
    print("\nAll mutated files restored byte-for-byte.\n")
    print("| # | Mutation | Result | Failing test(s) |")
    print("|---|---|---|---|")
    for i, desc, verdict, names in rows:
        print("| %d | %s | %s | %s |" % (i, desc.replace("|", "/"), verdict, names))
    killed = sum(1 for r in rows if r[2] == "killed")
    print("\nkilled %d / %d" % (killed, len(rows)))


if __name__ == "__main__":
    main()
