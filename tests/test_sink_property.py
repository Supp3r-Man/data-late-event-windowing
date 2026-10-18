import random

import pytest

from latewin.events import DISTS, generate_stream
from latewin.metrics import evaluate
from latewin.stream import INF, Sink, run_stream
from latewin.windows import batch_windows, session, sliding, tumbling

W = ("k", 0, 10)
SPECS = [tumbling(20), sliding(20, 10), session(8)]


def view_of(msgs):
    s = Sink()
    for m in msgs:
        s.apply(m)
    return s


def test_duplicate_message_is_ignored_and_counted():
    s = Sink()
    assert s.apply(("emit", W, 1, (1, 1))) and not s.apply(("emit", W, 1, (1, 1)))
    assert (s.applied, s.dupes) == (1, 1)


def test_replaying_whole_outbox_twice_equals_once():
    evs = generate_stream(400, 2, "heavy")
    p = run_stream(evs, session(8), 5, 50)
    once, twice = view_of(p.out), view_of(p.out + p.out)
    assert once.view == twice.view and twice.dupes == len(p.out) and twice.applied == once.applied


def test_stale_emit_and_stale_retract_do_not_override_newer_version():
    assert view_of([("emit", W, 2, (5, 5)), ("emit", W, 1, (1, 1))]).view[W] == (5, 5)
    assert view_of([("emit", W, 2, (5, 5)), ("retract", W, 1, None)]).view[W] == (5, 5)


def test_retract_removes_window_and_blocks_late_same_version_emit():
    assert view_of([("emit", W, 1, (1, 1)), ("retract", W, 1, None)]).view == {}
    assert view_of([("retract", W, 1, None), ("emit", W, 1, (1, 1))]).view == {}


def test_any_delivery_order_gives_same_view():
    for spec in SPECS:
        p = run_stream(generate_stream(300, 4, "exponential"), spec, 3, 40)
        want = view_of(p.out).view
        for seed in range(5):
            msgs = p.out[:]
            random.Random(seed).shuffle(msgs)
            assert view_of(msgs).view == want


@pytest.mark.parametrize("spec", SPECS, ids=lambda s: s.kind)
def test_unbounded_lateness_streaming_equals_batch(spec):
    for dist in DISTS:
        for seed in range(6):
            evs = generate_stream(300, seed, dist)
            for delay in (0, 7):
                p = run_stream(evs, spec, delay, INF)
                assert p.side == []
                assert view_of(p.out).view == batch_windows(evs, spec), (dist, seed, delay)


def test_unbounded_lateness_is_arrival_order_independent():
    for spec in SPECS:
        for seed in range(4):
            evs = generate_stream(300, seed, "heavy")
            random.Random(seed).shuffle(evs)
            assert view_of(run_stream(evs, spec, 0, INF).out).view == batch_windows(evs, spec)


def test_lateness_above_generator_cap_is_complete_and_huge_delay_is_first_correct():
    for spec in SPECS:
        evs = generate_stream(400, 1, "heavy")
        p = run_stream(evs, spec, 0, 400)
        assert p.side == [] and view_of(p.out).view == batch_windows(evs, spec)
        r = evaluate(generate_stream(300, 1, "heavy"), spec, 10**6, 0)
        assert r["first_ok"] == 1.0 and r["dropped"] == 0.0
