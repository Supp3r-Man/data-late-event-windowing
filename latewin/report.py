"""Text report printed by `python -m latewin demo`."""
from .events import Event
from .metrics import mean_eval
from .stream import INF, Sink, run_stream
from .windows import batch_windows, session, sliding, tumbling

DELAYS = (0, 2, 5, 10, 20, 50)
DISTS = ("uniform", "exponential", "heavy", "straggler")
pct = lambda x: f"{100 * x:5.1f}"  # noqa: E731


def straggler_case():
    """Hand-built failure case: one event lands after the watermark passed its window."""
    evs = [Event(0, "k0", 3, 3, 10), Event(1, "k0", 8, 8, 20), Event(2, "k0", 25, 25, 5),
           Event(3, "k0", 12, 27, 7), Event(4, "k0", 40, 40, 1)]
    wid = ("k0", 0, 20)
    lines = [f"tumbling(20), watermark delay 5; truth for {wid[1:]} = {batch_windows(evs, tumbling(20))[wid]}",
             "event id 3 has event_time 12 but arrives at 27, after the watermark reached 20"]
    for lat in (0, 30):
        p = run_stream(evs, tumbling(20), 5, lat)
        s = Sink()
        for m in p.out:
            s.apply(m)
        msgs = [f"{k}(v{v}{'' if val is None else ',' + str(val)})" for k, w, v, val in p.out if w == wid]
        lines.append(f"  allowed_lateness={lat:<2}: messages {' '.join(msgs)}; side output ids "
                     f"{[e.id for e in p.side]}; final view {s.view[wid]}")
    return lines


def report(seeds=5, n=1500):
    L = [f"Synthetic stream: n={n} events, 3 keys, mean over {seeds} seeds. Ticks are integer time units.",
         "", "Table A: tumbling(20), allowed_lateness=0 (strict). first_ok = windows correct when first emitted",
         "dist         delay  first_ok%  dropped%  latency  proc-time_ok%"]
    for d in DISTS:
        for dl in DELAYS:
            r = mean_eval(d, tumbling(20), dl, 0, seeds, n)
            L.append(f"{d:<12} {dl:>5}  {pct(r['first_ok'])}     {pct(r['dropped'])}    {r['latency']:6.1f}  {pct(r['proc_ok'])}")
    L += ["", "Table B: window kinds, exponential lateness, delay 5. lateness 0 vs 30",
          "window          lat  first_ok%  final_ok%  dropped%  updates  proc-time_ok%"]
    for name, sp in (("tumbling(20)", tumbling(20)), ("sliding(20,10)", sliding(20, 10)), ("session(8)", session(8))):
        for lat in (0, 30):
            r = mean_eval("exponential", sp, 5, lat, seeds, n)
            L.append(f"{name:<15} {lat:>3}  {pct(r['first_ok'])}     {pct(r['final_ok'])}     {pct(r['dropped'])}  "
                     f"{r['updates']:7.0f}  {pct(r['proc_ok'])}")
    L += ["", "Table C: tumbling(20), heavy-tail lateness, delay 5, allowed-lateness sweep",
          "allowed_lateness  first_ok%  final_ok%  dropped%  updates"]
    for lat in (0, 10, 30, 100, 300, INF):
        r = mean_eval("heavy", tumbling(20), 5, lat, seeds, n)
        L.append(f"{lat:>16}  {pct(r['first_ok'])}     {pct(r['final_ok'])}     {pct(r['dropped'])}  {r['updates']:7.0f}")
    return "\n".join(L + ["", "Failure case: straggler after the watermark"] + straggler_case())
