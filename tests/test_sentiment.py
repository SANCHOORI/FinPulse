from finpulse.features.sentiment import score_text


def test_positive():
    s = score_text("This is fantastic news, I love it")
    assert s.compound > 0.5
    assert s.pos > s.neg


def test_negative():
    s = score_text("This is terrible, awful, the worst")
    assert s.compound < -0.5
    assert s.neg > s.pos


def test_neutral():
    s = score_text("The market opened at 9:30 today")
    assert -0.2 < s.compound < 0.2


def test_empty():
    s = score_text("")
    assert s.compound == 0.0
    assert s.neu == 1.0
