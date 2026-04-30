from finpulse.features.tickers import extract_tickers


def test_cashtag():
    assert extract_tickers("Bullish on $AAPL today") == ["AAPL"]


def test_bare_known_ticker():
    assert extract_tickers("AAPL hit a new high") == ["AAPL"]


def test_unknown_cashtag_rejected():
    assert extract_tickers("$YOLO to the moon") == []


def test_common_word_not_a_ticker():
    # "FOR" and "ALL" are uppercase and 3-letters but obviously not tickers.
    assert extract_tickers("FOR ALL THE THINGS") == []


def test_dedup_preserves_order():
    assert extract_tickers("$NVDA up. AAPL flat. NVDA again.") == ["NVDA", "AAPL"]


def test_multiple_tickers():
    out = extract_tickers("Long $AAPL, short MSFT")
    assert "AAPL" in out and "MSFT" in out


def test_empty():
    assert extract_tickers("") == []
    assert extract_tickers("just some prose with no symbols") == []
