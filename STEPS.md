# STEPS

## Part A: Steps taken

1. **Scaffold.** Maven project, Java 17 (`maven.compiler.release=17`), JUnit 5.11.4, surefire 3.5.2, no other libraries.
   A `junit-platform.properties` sets a 30 s default timeout per test so a lost wakeup or deadlock fails the test
   instead of hanging the build.
2. **BoundedWorkerPool.** One `ReentrantLock`, one `notEmpty` condition, an `ArrayDeque` capped at the queue capacity.
   Three reject policies, `shutdown` (drain) versus `shutdownNow` (drop and interrupt), an injected handler for task failures.
   Design decision: CALLER_RUNS executes the task after the pool lock is released.
3. **Counters.** Racy, synchronized, ReentrantLock, AtomicLong, LongAdder behind one interface. The racy one has a hook
   between its read and write so a test can force the lost update with two latches instead of hoping for one.
4. **Deadlock demo.** `Account` with a `ReentrantLock`, three transfer strategies (naive, global order, tryLock with timeout)
   and a `ThreadMXBean` detector. A hook after the first lock plus a `CyclicBarrier` makes the deadlock certain. All lock
   calls are interruptible so tests can break the deadlock and leave the JVM clean.
5. **Async pipeline.** `CompletableFuture.orTimeout` cannot be driven by a fake clock, so the lab has a `Timers` seam:
   `RealTimers` in production, `FakeTimers` (manual `advance(ms)`) in tests. On top: `withTimeout`, `retry` with exponential
   backoff, `fanOut`, and a small `AsyncPipeline` that composes them. Every combinator propagates cancellation upstream.
6. **BoundedBuffer.** Ring buffer, one lock, `notFull` and `notEmpty`. Tests: a sequential differential test against
   `LinkedBlockingQueue` (300 seeds of random non-blocking operations) and a concurrent scenario run 150 times on each
   implementation (random producers, consumers, capacity), checking exactly-once delivery, per-producer order and capacity.
7. **Demo.** `Demo` runs the counter table, the deadlock demonstration and the reject-policy demonstration.
8. **Mutation check.** `scripts/mutation_check.py` applies one edit at a time, runs the tests, restores the file, and
   verifies byte-for-byte restoration. Results are in the README.
9. **Three consecutive green `mvn -q -B verify` runs** after the last source change.

### What I learned, including the bugs I hit

- **A test bug, not a product bug:** my first "ordered locks survive hammering" test used balances of 1,000 and 20,000
  one-unit transfers per thread. It failed with 17,184 of 20,000 transfers done. Cause: when one thread runs ahead it drains
  its own account and the transfers are legitimately refused for insufficient funds. The same thing made the demo print
  "13,090 transfers done". Fix: balances of 1,000,000. Lesson: assert on what the code is responsible for, and size the
  fixture so no unrelated rule fires.
- **Process bug while mutation testing:** I started the mutation script in a background shell and, thinking it had died,
  started it a second time. Two instances mutated the same files at once. I noticed because the source hashes I had
  snapshotted before the run no longer matched. I killed both runs, reverted the one stray mutation by hand, confirmed the
  hashes were identical to the snapshot, and ran the script once more. Lesson: snapshot hashes before mutation work, and run
  exactly one mutation process.
- **Lost updates are large.** With 8 threads the plain `long` counter lost about 7 to 10 million of 16 million increments
  per run (see README for numbers).
- **JIT and the racy counter.** The racy row reports absurd throughput because the JIT can keep the field in a register
  across the loop. That row measures a broken loop; the interesting number is the lost-update count.
- **Cancelling a dependent `CompletableFuture` does not cancel its source.** Every combinator in `Async` wires that
  explicitly, and each has a test and a mutation.
- **`findDeadlockedThreads` versus `findMonitorDeadlockedThreads`.** Only the first sees `java.util.concurrent` locks.
- **Benchmark numbers move.** Three runs of the same demo gave `LongAdder` between 115 and 252 ms and `AtomicLong` between
  296 and 438 ms. I report one run and say so.

## Part B: How to run it yourself

Requirements: JDK 17 or newer, Maven 3.9, Python 3 (only for the mutation script).

```bash
# build and run all tests
mvn -q -B verify

# run the demo (counters table, deadlock detection and fixes, reject policies)
java -cp target/classes com.github.sup3rmar10.concurrencylab.Demo
# or
mvn -q exec:java

# run only one area
mvn -q -B test -Dtest=AsyncTest
mvn -q -B test -Dtest=BoundedBufferTest

# repeat the whole suite a few times to look for flakiness
for i in 1 2 3; do mvn -q -B verify || break; done

# mutation check (about 10 to 15 minutes; run one instance only)
python scripts/mutation_check.py
python scripts/mutation_check.py 3 7   # just mutations 3 and 7
```

To try your own mutation: break something in `src/main`, run `mvn -q -B test`, and see which test names fail. If none
fails, you have found a gap in the tests.
