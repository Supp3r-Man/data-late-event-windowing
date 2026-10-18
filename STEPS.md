# STEPS

**Part A** is what I did, in order. **Part B** is how you run it.

---

## Part A: steps taken

1. **Chose the model before coding:** a window id is `(key, start, end)` with end exclusive, a result is `(count, sum)`, and times are integer ticks so equality checks are exact (no float comparison).
2. **Wrote the generator** with two separate seeded random streams, one for event times and one for lateness, so every lateness distribution sees the same underlying events and only arrival differs. Five distributions: none, uniform, exponential, heavy tail (Pareto, capped at 300), straggler (5% land 40-80 ticks late). Fictional sensors `k0..k2` only.
3. **Wrote the batch ground truth** first (tumbling, sliding, session). Session rule fixed up front: a gap of exactly `gap` splits, less than `gap` merges.
4. **Wrote the streaming processor:** watermark = max event time seen minus delay; a min-heap of window ends fires windows when the watermark reaches the end; an event is accepted only if `end + allowed_lateness > watermark`, otherwise it goes to a side output; an update to an already-fired window emits `retract(v)` then `emit(v+1)`; sessions merge on arrival and retract the ids they replace.
5. **Wrote the sink** so delivery is idempotent and order-tolerant: dedup by (kind, window, version), highest version wins, a retract of version v dominates an emit of v.
6. **Wrote the metrics:** first-emission correctness, final correctness after updates, dropped rate, first-emission latency, and the processing-time baseline (same windows, events assigned by arrival time).
7. **Wrote 42 tests** in four files, including seeded property tests: with unbounded lateness the streaming view equals the batch result for every window kind and distribution, and also when arrival order is shuffled.
8. **Mutation-checked the tests:** 10 deliberate one-line breaks on a fresh copy of the repo, one at a time. All 10 caught, and I re-ran without `-x` to confirm each is caught by a targeted test, not only by a statistical one.
9. **Ran the demo, copied the real numbers into the README,** wrote this file, committed locally.

Problems and choices along the way:
- Three of my first tests were wrong, not the code: a window I asserted on never existed (the events never landed in it), and a session test accidentally overlapped the next session. I fixed the tests after working through the arithmetic by hand.
- The first draft had about 68 collected tests because of parametrisation; I folded them into fewer tests (42) rather than pad the count.
- A session can extend an already-fired session even with lateness 0, because the lateness check uses the merged window end. I kept it and documented it instead of hiding it.
- I did not add CI dependencies: the code is stdlib only, so `ci.yml` only installs pytest.

---

## Part B: steps for you

1. Needs Python 3.11 or newer. No packages except pytest.
2. From the repo folder: `python -m pytest` (expect 42 passed).
3. `python -m latewin demo` prints the tables (about 3 seconds); `--seeds 2 --n 500` is faster.
4. Read the failure case at the bottom of the demo output first: it is the whole idea in five lines.
5. To experiment: change the delay or lateness in `latewin/report.py`, or call `run_stream(events, tumbling(20), delay, lateness)` yourself and inspect `.out` (messages), `.side` (dropped events) and `.first` (first emission and latency).
6. To push later: create the GitHub repo, then `git remote add origin ...` and `git push -u origin main`. CI has not run yet.
