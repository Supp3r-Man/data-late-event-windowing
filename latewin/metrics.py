"""Compare streaming output to the batch ground truth; processing-time baseline."""
from .events import generate_stream
from .stream import Sink, run_stream
from .windows import batch_windows


def _frac(got, truth):
    keys = set(got) | set(truth)
    return sum(1 for k in keys if got.get(k) == truth.get(k)) / len(keys) if keys else 1.0


def evaluate(events, spec, delay, lateness=0):
    """Metrics for one run. first_ok: share of windows (truth plus spurious) whose
    FIRST emitted value equals truth. final_ok: same for the sink view after all
    updates/retractions. latency: mean ticks from window end to first emission."""
    truth = batch_windows(events, spec)
    p = run_stream(events, spec, delay, lateness)
    sink = Sink()
    for m in p.out:
        sink.apply(m)
    first = {w: v for w, (v, _) in p.first.items()}
    lat = [l for _, l in p.first.values()]
    pt = batch_windows(events, spec, time_of=lambda e: e.arrival_time)
    return {
        "first_ok": _frac(first, truth),
        "final_ok": _frac(sink.view, truth),
        "dropped": len(p.side) / len(events),
        "latency": sum(lat) / len(lat) if lat else 0.0,
        "updates": float(p.updates),
        "proc_ok": _frac(pt, truth),
    }


def mean_eval(dist, spec, delay, lateness, seeds, n):
    rs = [evaluate(generate_stream(n, s, dist), spec, delay, lateness) for s in range(seeds)]
    return {k: sum(r[k] for r in rs) / len(rs) for k in rs[0]}
