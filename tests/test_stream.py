from latewin.events import Event
from latewin.stream import Processor, Sink, run_stream
from latewin.windows import batch_windows, session, sliding, tumbling


def ev(i, t, arr=None, v=1, key="k"):
    return Event(i, key, t, t if arr is None else arr, v)


def run(evs, spec, delay=0, lat=0):
    p = run_stream(evs, spec, delay, lat)
    s = Sink()
    for m in p.out:
        s.apply(m)
    return p, s


def test_watermark_is_max_event_time_minus_delay_and_never_regresses():
    p = Processor(tumbling(10), 3)
    seen = []
    for i, t in enumerate([10, 5, 20, 2, 19]):
        p.process(ev(i, t, 100 + i))
        seen.append(p.wm)
    assert seen == [7, 7, 17, 17, 17]


def test_window_not_fired_until_watermark_reaches_end():
    p = Processor(tumbling(10), 0)
    p.process(ev(0, 9))
    assert p.out == []
    p.process(ev(1, 10))
    assert [m[:3] for m in p.out] == [("emit", ("k", 0, 10), 1)]
    q = Processor(tumbling(10), 5)  # delay postpones firing by exactly 5 ticks
    q.process(ev(0, 3))
    q.process(ev(1, 14))
    assert q.out == []
    q.process(ev(2, 15))
    assert len(q.out) == 1


def test_first_emission_latency_is_clock_minus_window_end():
    p = Processor(tumbling(10), 5)
    p.process(ev(0, 3))
    p.process(ev(1, 15))
    assert p.first[("k", 0, 10)] == ((1, 1), 5)


def test_straggler_after_watermark_goes_to_side_output():
    evs = [ev(0, 3, v=10), ev(1, 25), ev(2, 4, 30, v=7)]
    p, s = run(evs, tumbling(10), 0, 0)
    assert [e.id for e in p.side] == [2]
    assert s.view[("k", 0, 10)] == (1, 10)  # wrong vs truth (2, 17): the failure case
    assert batch_windows(evs, tumbling(10))[("k", 0, 10)] == (2, 17)


def test_allowed_lateness_updates_with_retraction_and_version_bump():
    evs = [ev(0, 3, v=10), ev(1, 25), ev(2, 4, 30, v=7)]
    p, s = run(evs, tumbling(10), 0, 30)
    w = ("k", 0, 10)
    assert [(m[0], m[2]) for m in p.out if m[1] == w] == [("emit", 1), ("retract", 1), ("emit", 2)]
    assert s.view[w] == (2, 17) and p.side == [] and p.updates == 1


def test_lateness_boundary_is_exclusive_of_end_plus_lateness():
    base = [ev(0, 3), ev(1, 20)]  # watermark 20; window [0,10) end 10
    late = ev(2, 4, 99)
    p10, _ = run(base + [late], tumbling(10), 0, 10)  # 10 + 10 == 20 -> closed
    p11, _ = run(base + [late], tumbling(10), 0, 11)
    assert p10.side == [late] and p11.side == []


def test_late_event_does_not_move_watermark():
    p = Processor(tumbling(10), 0)
    p.process(ev(0, 50))
    p.process(ev(1, 1, 60))
    assert p.wm == 50


def test_flush_emits_unfired_windows():
    p = Processor(tumbling(10), 5)
    p.process(ev(0, 3))
    assert p.out == []
    p.flush()
    assert len(p.out) == 1 and p.out[0][3] == (1, 1)


def test_sliding_partial_drop_when_one_window_closed():
    evs = [ev(0, 3), ev(1, 10), ev(2, 7, 11)]  # watermark 10 closes [0,10) but not [5,15)
    p, s = run(evs, sliding(10, 5), 0, 0)
    assert [e.id for e in p.side] == [2]
    assert s.view[("k", 5, 15)] == (2, 2) and ("k", 0, 10) in s.view
    _, s2 = run([ev(0, 7, v=3)], sliding(10, 5))
    assert s2.view == {("k", 0, 10): (1, 3), ("k", 5, 15): (1, 3)}


def test_session_merges_two_open_sessions():
    p, s = run([ev(0, 0), ev(1, 15), ev(2, 8)], session(10), 100)
    assert s.view == {("k", 0, 25): (3, 3)} and p.updates == 0


def test_session_gap_equal_does_not_merge():
    _, s = run([ev(0, 0), ev(1, 10)], session(10), 100)
    assert set(s.view) == {("k", 0, 10), ("k", 10, 20)}


def test_session_late_event_retracts_fired_sessions_and_merges():
    evs = [ev(0, 0), ev(1, 15), ev(2, 40), ev(3, 8, 50)]
    p, s = run(evs, session(10), 0, 100)
    a, b, m = ("k", 0, 10), ("k", 15, 25), ("k", 0, 25)
    assert [x[:3] for x in p.out if x[0] == "retract"] == [("retract", a, 1), ("retract", b, 1)]
    assert s.view[m] == (3, 3) and a not in s.view and b not in s.view
    assert s.view == batch_windows(evs, session(10))


def test_session_event_past_lateness_goes_to_side_output():
    p, s = run([ev(0, 0), ev(1, 100), ev(2, 1, 101)], session(10), 0, 0)
    assert [e.id for e in p.side] == [2] and s.view[("k", 0, 10)] == (1, 1)


def test_session_extension_of_fired_window_republishes_new_id():
    evs = [ev(0, 0), ev(1, 15), ev(2, 5, 16)]  # [0,10) fired at wm 10; t=5 extends it to [0,15)
    p, s = run(evs, session(10), 5, 5)
    assert [x[0] for x in p.out if x[1] == ("k", 0, 10)] == ["emit", "retract"]
    assert s.view[("k", 0, 15)] == (2, 2)
