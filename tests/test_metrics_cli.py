from latewin.__main__ import main
from latewin.events import generate_stream
from latewin.metrics import evaluate, mean_eval
from latewin.report import straggler_case
from latewin.stream import INF
from latewin.windows import tumbling


def test_dropped_rate_never_increases_with_delay_per_run():
    for seed in range(4):
        evs = generate_stream(500, seed, "heavy")
        d = [evaluate(evs, tumbling(20), dl, 0)["dropped"] for dl in (0, 2, 5, 10, 20, 50, 400)]
        assert d == sorted(d, reverse=True) and d[0] > 0 and d[-1] == 0


def test_delay_trades_latency_for_completeness():
    lo = mean_eval("uniform", tumbling(20), 0, 0, 3, 600)
    hi = mean_eval("uniform", tumbling(20), 20, 0, 3, 600)
    assert hi["latency"] > lo["latency"] + 10
    assert hi["first_ok"] > lo["first_ok"] + 0.15 and hi["first_ok"] == 1.0


def test_in_order_stream_is_perfect_even_with_zero_delay():
    r = evaluate(generate_stream(500, 0, "none"), tumbling(20), 0, 0)
    assert r["first_ok"] == r["final_ok"] == r["proc_ok"] == 1.0 and r["dropped"] == 0


def test_processing_time_baseline_is_wrong_under_disorder():
    assert mean_eval("heavy", tumbling(20), 5, 0, 3, 600)["proc_ok"] < 0.6


def test_allowed_lateness_raises_final_correctness_and_cuts_drops():
    a = mean_eval("heavy", tumbling(20), 5, 0, 3, 600)
    b = mean_eval("heavy", tumbling(20), 5, 100, 3, 600)
    assert b["final_ok"] > a["final_ok"] and b["dropped"] < a["dropped"] and b["updates"] > 0
    assert mean_eval("heavy", tumbling(20), 5, INF, 3, 600)["final_ok"] == 1.0


def test_straggler_stays_wrong_until_delay_exceeds_its_lateness():
    f = lambda dl: mean_eval("straggler", tumbling(20), dl, 0, 3, 600)["first_ok"]  # noqa: E731
    assert f(20) < 0.9 < f(100)
    txt = "\n".join(straggler_case())
    assert "side output ids [3]; final view (2, 30)" in txt
    assert "emit(v2,(3, 37))" in txt


def test_demo_cli_prints_all_tables(capsys):
    main(["demo", "--seeds", "1", "--n", "200"])
    out = capsys.readouterr().out
    for h in ("Table A", "Table B", "Table C", "Failure case"):
        assert h in out
