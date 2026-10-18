import random
import re

import pytest

from latewin.events import DISTS, MAX_LATENESS, Event, generate_stream
from latewin.windows import assign, batch_windows, session, sliding, tumbling


def ev(i, t, v=1, key="k"):
    return Event(i, key, t, t, v)


def test_stream_is_deterministic_per_seed():
    assert generate_stream(200, 3, "heavy") == generate_stream(200, 3, "heavy")
    assert generate_stream(200, 3, "heavy") != generate_stream(200, 4, "heavy")


def test_stream_sorted_by_arrival_and_never_early():
    for d in DISTS:
        s = generate_stream(300, 1, d)
        assert [e.arrival_time for e in s] == sorted(e.arrival_time for e in s)
        assert all(0 <= e.arrival_time - e.event_time <= MAX_LATENESS for e in s)


def test_none_is_in_order_and_other_distributions_reorder():
    s = generate_stream(300, 1, "none")
    assert all(e.arrival_time == e.event_time for e in s)
    assert [e.event_time for e in s] == sorted(e.event_time for e in s)
    for d in ("uniform", "exponential", "heavy", "straggler"):
        s = generate_stream(500, 1, d)
        times = [e.event_time for e in s]
        assert times != sorted(times), d


def test_event_times_do_not_depend_on_distribution():
    a = {e.id: e.event_time for e in generate_stream(200, 2, "uniform")}
    b = {e.id: e.event_time for e in generate_stream(200, 2, "heavy")}
    assert a == b


def test_synthetic_only_shapes_and_unknown_distribution():
    with pytest.raises(ValueError):
        generate_stream(5, 0, "gaussian")
    for e in generate_stream(300, 0, "uniform"):
        assert re.fullmatch(r"k\d", e.key) and 1 <= e.value <= 100


def test_tumbling_assign_boundaries_end_exclusive():
    sp = tumbling(10)
    assert assign(sp, 0) == [(0, 10)]
    assert assign(sp, 9) == [(0, 10)]
    assert assign(sp, 10) == [(10, 20)]


def test_sliding_assign_overlap():
    sp = sliding(10, 5)
    assert assign(sp, 7) == [(0, 10), (5, 15)]
    assert all(len(assign(sp, t)) == 2 for t in range(0, 60))
    assert assign(sp, 5) == [(0, 10), (5, 15)]
    assert assign(sp, 10) == [(5, 15), (10, 20)]
    assert assign(sliding(10, 5), 2) == [(-5, 5), (0, 10)]


def test_batch_tumbling_hand_computed():
    evs = [ev(0, 1, 10), ev(1, 9, 5), ev(2, 10, 7), ev(3, 25, 1, "j")]
    assert batch_windows(evs, tumbling(10)) == {
        ("k", 0, 10): (2, 15), ("k", 10, 20): (1, 7), ("j", 20, 30): (1, 1)}


    evs = [ev(0, 7, 3), ev(1, 12, 4)]
    assert batch_windows(evs, sliding(10, 5)) == {
        ("k", 0, 10): (1, 3), ("k", 5, 15): (2, 7), ("k", 10, 20): (1, 4)}


def test_batch_session_gap_boundary():
    evs = [ev(0, 0), ev(1, 9), ev(2, 19)]  # diffs 9 (<10 merges), 10 (== gap splits)
    assert batch_windows(evs, session(10)) == {("k", 0, 19): (2, 2), ("k", 19, 29): (1, 1)}


def test_batch_session_keys_independent():
    evs = [ev(0, 0, 1, "a"), ev(1, 3, 1, "b"), ev(2, 5, 1, "a")]
    r = batch_windows(evs, session(10))
    assert r == {("a", 0, 15): (2, 2), ("b", 3, 13): (1, 1)}


def test_batch_is_arrival_order_independent():
    evs = generate_stream(300, 5, "heavy")
    for sp in (tumbling(20), sliding(20, 10), session(8)):
        base = batch_windows(evs, sp)
        shuffled = evs[:]
        random.Random(1).shuffle(shuffled)
        assert batch_windows(shuffled, sp) == base
