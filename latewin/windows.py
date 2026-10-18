"""Window specs (tumbling / sliding / session) and the batch ground truth.

A window id is (key, start, end), end exclusive. A result is (count, sum).
Session rule: consecutive events of a key whose times differ by < gap share a
session; a difference of exactly gap splits it. Session = [first, last + gap).
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class WindowSpec:
    kind: str
    size: int = 0
    slide: int = 0
    gap: int = 0


def tumbling(size):
    return WindowSpec("tumbling", size, size)


def sliding(size, slide):
    return WindowSpec("sliding", size, slide)


def session(gap):
    return WindowSpec("session", gap=gap)


def assign(spec, t):
    """(start, end) of every tumbling/sliding window containing time t."""
    first, last = (t - spec.size) // spec.slide + 1, t // spec.slide
    return [(k * spec.slide, k * spec.slide + spec.size) for k in range(first, last + 1)]


def batch_windows(events, spec, time_of=lambda e: e.event_time):
    """Ground truth: recompute every window from the full data at once."""
    out = {}
    if spec.kind != "session":
        for e in events:
            for s, en in assign(spec, time_of(e)):
                c, tot = out.get((e.key, s, en), (0, 0))
                out[(e.key, s, en)] = (c + 1, tot + e.value)
        return out
    bykey = {}
    for e in events:
        bykey.setdefault(e.key, []).append(e)
    for key, es in bykey.items():
        es.sort(key=time_of)
        cur = None
        for e in es:
            t = time_of(e)
            if cur and t < cur[1]:
                cur = [cur[0], t + spec.gap, cur[2] + 1, cur[3] + e.value]
            else:
                if cur:
                    out[(key, cur[0], cur[1])] = (cur[2], cur[3])
                cur = [t, t + spec.gap, 1, e.value]
        if cur:
            out[(key, cur[0], cur[1])] = (cur[2], cur[3])
    return out
