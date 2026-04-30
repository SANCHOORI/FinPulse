"""Throughput benchmark tests."""
from finpulse.bench import run_throughput


def test_throughput_runs_small():
    """Run with a tiny n to keep CI fast; assert basic shape."""
    r = run_throughput(n=200)
    assert r.n_events == 200
    assert r.elapsed_sec > 0
    assert r.events_per_sec > 0
    assert r.bytes_written > 0
    assert r.mb_per_sec >= 0
    assert r.bytes_per_event > 0
