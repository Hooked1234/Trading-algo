from datetime import UTC, date, datetime

from event_trader.calendar import NyseSessionCalendar


def test_entry_window_observes_new_york_dst() -> None:
    calendar = NyseSessionCalendar()
    assert calendar.is_entry_window(datetime(2026, 3, 9, 13, 40, tzinfo=UTC))
    assert not calendar.is_entry_window(datetime(2026, 3, 9, 13, 39, tzinfo=UTC))


def test_market_holiday_is_closed() -> None:
    calendar = NyseSessionCalendar()
    assert not calendar.is_entry_window(datetime(2026, 12, 25, 15, 0, tzinfo=UTC))


def test_post_close_event_moves_to_next_session() -> None:
    calendar = NyseSessionCalendar()
    value = datetime(2026, 8, 28, 21, 0, tzinfo=UTC)  # Friday 17:00 ET
    result = calendar.next_evaluation_time(value)
    assert result == datetime(2026, 8, 31, 13, 40, tzinfo=UTC)


def test_late_rth_event_is_skipped() -> None:
    calendar = NyseSessionCalendar()
    value = datetime(2026, 8, 25, 19, 0, tzinfo=UTC)  # 15:00 ET
    assert calendar.next_evaluation_time(value) is None


def test_previous_session_skips_weekend() -> None:
    calendar = NyseSessionCalendar()
    monday = datetime(2026, 8, 31, 14, 0, tzinfo=UTC)
    assert calendar.previous_session_date(monday) == date(2026, 8, 28)
