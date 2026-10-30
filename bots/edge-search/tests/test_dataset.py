from datetime import date

import numpy as np
import pytest
from conftest import day_ms, days, write_klines_zip

from edge_search.dataset import DatasetError, align, coverage, load_bars, parse_spec


def test_parse_spec():
    s = parse_spec("raw/binance/spot/spot-history 1d 2017-08-17..2026-10-26")
    assert (s.path, s.interval, s.start, s.end) == ("binance/spot/spot-history", "1d", date(2017, 8, 17), date(2026, 10, 26))
    assert s.bar_minutes == 1440
    assert s.end_ms - s.start_ms == (date(2026, 10, 27) - date(2017, 8, 17)).days * 86_400_000


@pytest.mark.parametrize(
    "text, needle",
    [
        ("raw/binance/usdm/btc 2026-10-27..2027-01-27", "isn't '<lake folder> <interval>"),
        ("raw/x 7m 2020-01-01..2021-01-01", "interval '7m'"),
        ("raw/x 1d 2021-01-01..2020-01-01", "end must come after"),
        ("raw/../etc 1d 2020-01-01..2021-01-01", "climb out"),
        ("raw/x 1d 2020-02-30..2021-01-01", "dataset dates"),
    ],
)
def test_bad_specs_say_why(text, needle):
    with pytest.raises(DatasetError, match=needle.replace("(", r"\(").replace("'", "'")):
        parse_spec(text)


def test_spot_microseconds_and_futures_headers_both_read(tmp_path):
    spec = parse_spec("raw/lake 1d 2026-08-01..2026-08-10")
    d = days(date(2026, 8, 1), 10)
    # spot: no header, microseconds (from 2025 on), a monthly file
    folder = tmp_path / "lake" / "AAAUSDT" / "klines" / "1d"
    write_klines_zip(folder, "AAAUSDT", "1d", "2026-08",
                     [[day_ms(x) * 1000, 1, 2, 0.5, 10 + i, 5, 0] for i, x in enumerate(d[:6])], header=False)
    # ...and daily files overlapping it, which must not double count
    for i, x in enumerate(d[4:], start=4):
        write_klines_zip(folder, "AAAUSDT", "1d", x.isoformat(), [[day_ms(x) * 1000, 1, 2, 0.5, 10 + i, 5, 0]], header=False)
    b = load_bars(tmp_path, spec, "AAAUSDT")
    assert list(b.open_ms) == [day_ms(x) for x in d]
    assert list(b.close) == [10 + i for i in range(10)]
    assert coverage(b, spec) == 1.0
    # futures: a header row, milliseconds
    folder = tmp_path / "lake" / "BBBUSDT" / "klines" / "1d"
    write_klines_zip(folder, "BBBUSDT", "1d", "2026-08",
                     [[day_ms(x), 1, 2, 0.5, 20, 5, 0] for x in d if x != date(2026, 8, 5)], header=True)
    b2 = load_bars(tmp_path, spec, "BBBUSDT")
    assert len(b2.open_ms) == 9
    assert coverage(b2, spec) == pytest.approx(0.9)
    times, closes = align([b, b2], spec.bar_ms)
    assert closes.shape == (2, 10)
    assert np.isnan(closes[1, 4]) and not np.isnan(closes[0, 4])


def test_a_late_listing_is_not_a_hole_but_a_missing_folder_is_an_error(tmp_path):
    spec = parse_spec("raw/lake 1d 2026-08-01..2026-08-10")
    folder = tmp_path / "lake" / "SOLUSDT" / "klines" / "1d"
    write_klines_zip(folder, "SOLUSDT", "1d", "2026-08",
                     [[day_ms(x), 1, 2, 0.5, 5, 5, 0] for x in days(date(2026, 8, 6), 5)], header=False)
    assert coverage(load_bars(tmp_path, spec, "SOLUSDT"), spec) == 1.0
    with pytest.raises(DatasetError, match="no 1d klines for ETHUSDT"):
        load_bars(tmp_path, spec, "ETHUSDT")
