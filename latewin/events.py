"""Synthetic, fictional event stream with event time vs arrival time."""
import random
from dataclasses import dataclass

DISTS = ("none", "uniform", "exponential", "heavy", "straggler")
MAX_LATENESS = 300  # hard cap on any generated lateness (ticks)


@dataclass(frozen=True)
class Event:
    id: int
    key: str
    event_time: int
    arrival_time: int
    value: int


def lateness(dist, rng):
    """One lateness sample in integer ticks (arrival_time - event_time)."""
    if dist == "none":
        return 0
    if dist == "uniform":
        return rng.randint(0, 20)
    if dist == "exponential":
        return min(MAX_LATENESS, int(rng.expovariate(1 / 6)))
    if dist == "heavy":  # Pareto tail: most events near-on-time, a few very late
        return min(MAX_LATENESS, int(2 * rng.paretovariate(1.2)) - 2)
    if dist == "straggler":  # 95% nearly on time, 5% land 40-80 ticks late
        return rng.randint(0, 2) if rng.random() < 0.95 else rng.randint(40, 80)
    raise ValueError(f"unknown lateness distribution: {dist}")


def generate_stream(n, seed, dist="exponential", keys=3):
    """n events for `keys` fictional sensors, returned in ARRIVAL order.

    Event times depend only on the seed (not on dist), so every distribution
    sees the same underlying facts and only arrival order/time differs.
    """
    rt = random.Random(f"{seed}:times")
    rl = random.Random(f"{seed}:late")
    t, evs = 0, []
    for i in range(n):
        t += rt.randint(10, 25) if rt.random() < 0.08 else rt.randint(0, 2)
        evs.append(Event(i, f"k{rt.randrange(keys)}", t, t + lateness(dist, rl), rt.randint(1, 100)))
    return sorted(evs, key=lambda e: (e.arrival_time, e.id))
