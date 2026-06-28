from tools.build_us_membership import to_spans


def test_to_spans_backward_walk():
    current = ["AAPL", "NVDA"]
    asof = "2026-06-01"
    changes = [
        {"date": "2020-01-01", "added": ["NVDA"], "removed": ["XRX"]},
    ]
    spans = to_spans(changes, current, asof)
    # NVDA added 2020-01-01, still in -> open span
    assert {"ticker": "NVDA", "market": "us", "index_name": "SP500",
            "valid_from": "2020-01-01", "valid_to": None} in spans
    # XRX removed 2020-01-01 -> closed span ending then (valid_from unknown -> dataset epoch)
    xrx = [s for s in spans if s["ticker"] == "XRX"]
    assert xrx and xrx[0]["valid_to"] == "2020-01-01"
    # AAPL never changed -> open span from dataset epoch
    aapl = [s for s in spans if s["ticker"] == "AAPL"]
    assert aapl and aapl[0]["valid_to"] is None
