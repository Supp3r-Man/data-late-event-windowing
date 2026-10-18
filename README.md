# data-late-event-windowing

![ci](https://github.com/Supp3r-Man/data-late-event-windowing/actions/workflows/ci.yml/badge.svg)

Event-time **tumbling, sliding and session windows** over a stream with late and out-of-order events, in pure stdlib Python: a **watermark** (max event time seen minus an allowed delay), **allowed lateness** with window **updates and retractions**, a **side output** for events past the allowed lateness, and **idempotent emission** (deduplicated by window id plus version). Every streaming result is compared with a **batch recompute over the full data**, and the watermark-delay versus completeness versus latency trade-off is measured.

> **What is verified, and what is not.** 42 tests pass locally on Python 3.13 (stdlib only; pytest for the tests). The GitHub Actions workflow (Linux, Python 3.11 and 3.13) ran green after the first push. I broke 10 key behaviours on purpose (watermark sign, lateness boundary, fire condition, sink dedup, retraction, session overlap, side output, batch session split, sliding assignment, retract versioning) and the tests failed for each. **All data is synthetic and fictional** (3 made-up sensors, integer ticks), generated from seeds. The numbers below show how this implementation behaves on that generator, **not** how a real pipeline or engine behaves: there is no real traffic, no multiple partitions, no clock skew, no throughput or state-size measurement, and no comparison with Flink or Beam.

## Measured results

Real output of `python -m latewin demo` (n=1500 events, mean of 5 seeds, delays and latency in ticks). `first_ok` = share of windows whose first emitted value equals the batch ground truth; `dropped` = events sent to the side output; `latency` = mean ticks from window end to first emission. `proc-time_ok` = the naive baseline: same windows, but events assigned by arrival time instead of event time.

Tumbling(20), allowed lateness 0 (strict), watermark delay sweep:

| lateness dist | delay 0 | delay 5 | delay 10 | delay 20 | delay 50 |
|---|---|---|---|---|---|
| uniform 0..20: first_ok / dropped | 68.8% / 16.1% | 84.3% / 6.5% | 95.1% / 1.7% | 100% / 0% | 100% / 0% |
| exponential (mean 6) | 73.7% / 10.4% | 89.2% / 3.6% | 94.6% / 1.7% | 99.0% / 0.3% | 100% / 0% |
| heavy tail (Pareto) | 75.2% / 8.9% | 84.3% / 5.2% | 87.6% / 3.9% | 92.0% / 2.5% | 96.5% / 1.1% |
| straggler (5% land 40-80 late) | 82.6% / 5.6% | 83.4% / 5.3% | 83.5% / 5.3% | 84.4% / 4.9% | 93.9% / 1.8% |
| mean latency (exponential) | 8.8 | 13.3 | 17.6 | 28.1 | 57.1 |
| proc-time baseline_ok (uniform / exp / heavy / straggler) | 6.2% / 20.6% / 33.9% / 51.4% (independent of delay) | | | | |

Window kinds, exponential lateness, delay 5, allowed lateness 0 vs 30:

| window | lateness | first_ok | final_ok | dropped | updates | proc-time_ok |
|---|---|---|---|---|---|---|
| tumbling(20) | 0 / 30 | 89.2% / 90.2% | 89.2% / 99.9% | 3.6% / 0.0% | 0 / 49 | 20.6% |
| sliding(20,10) | 0 / 30 | 88.7% / 89.7% | 88.7% / 99.9% | 6.6% / 0.0% | 0 / 103 | 21.8% |
| session(8) | 0 / 30 | 82.1% / 85.4% | 89.8% / 99.9% | 1.8% / 0.0% | 15 / 26 | 1.5% |

Allowed-lateness sweep (tumbling(20), heavy tail, delay 5): final_ok 84.3% (0), 90.0% (10), 94.9% (30), 98.2% (100), 100% (300 or unbounded); dropped 5.2%, 3.1%, 1.6%, 0.5%, 0%; updates 0, 29, 49, 64, 72.

What this shows: a bigger delay buys completeness at a latency cost of about the delay itself; allowed lateness repairs results after the fact (at the price of retractions) without delaying the first answer; and with the straggler mix the curve is flat until the delay passes the straggler lateness (about 80), so a "typical" delay does not help a bimodal tail. Processing-time windows are only right when nothing is late. What it does **not** show: where these curves sit on real data (they depend entirely on the generator parameters).

Failure case (hand-built, printed by the demo): event 3 has event time 12 but arrives at 27, after the watermark passed 20. With allowed lateness 0 it goes to the side output and window [0,20) stays at (2, 30) when the truth is (3, 37). With allowed lateness 30 the processor emits `retract(v1)` then `emit(v2, (3, 37))`.

## Run

```
python -m pytest                 # 42 tests, under 2 seconds
python -m latewin demo           # prints the tables above (about 3 seconds)
python -m latewin demo --seeds 2 --n 500
```

## Layout

- `latewin/events.py`: seeded synthetic stream, 5 lateness distributions (none, uniform, exponential, heavy, straggler).
- `latewin/windows.py`: window specs, assignment, and the batch ground truth.
- `latewin/stream.py`: `Processor` (watermark, lateness, retractions, side output) and `Sink` (idempotent, order-tolerant materialised view).
- `latewin/metrics.py`, `latewin/report.py`: evaluation, baseline, demo tables.
- `tests/`: unit tests, sink tests, seeded property tests (unbounded lateness: streaming equals batch for all window kinds, all distributions, shuffled arrival order), metric trend tests.

## Known gaps

- One global watermark: no per-partition watermarks and no idle-source handling.
- No duplicate source events (the sink dedups emissions, not input events); no clock skew.
- No early or periodic triggers; every late event publishes its own new version (no update coalescing).
- Aggregates are count and sum only; window state is never purged, so memory is unbounded.
- A session can still extend an already-fired session with lateness 0, because the lateness check uses the merged window end; this is a modelling choice.
- Windows still open at end of stream are flushed; their latency is clamped at 0, which flatters latency a little.
- Sliding-window `dropped` counts an event once even if only one of its windows rejected it.
- No real engine comparison.
