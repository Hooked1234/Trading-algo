"""NYSE session rules using an exchange-maintained calendar."""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import exchange_calendars as xcals

NEW_YORK = ZoneInfo("America/New_York")
UTC = UTC


class NyseSessionCalendar:
    def __init__(self) -> None:
        self._calendar = xcals.get_calendar("XNYS")

    def is_session(self, session_date: date) -> bool:
        return self._calendar.is_session(session_date.isoformat())

    def is_entry_window(self, value: datetime) -> bool:
        local = value.astimezone(NEW_YORK)
        return self.is_session(local.date()) and time(9, 40) <= local.time() <= time(14, 45)

    def force_flat_due(self, value: datetime) -> bool:
        local = value.astimezone(NEW_YORK)
        return self.is_session(local.date()) and local.time() >= time(15, 55)

    def previous_session_date(self, value: datetime) -> date:
        """Return the most recent completed NYSE session before ``value``'s local date."""

        search_date = value.astimezone(NEW_YORK).date() - timedelta(days=1)
        while not self.is_session(search_date):
            search_date -= timedelta(days=1)
        return search_date

    def next_evaluation_time(self, value: datetime) -> datetime | None:
        """Return the only allowed initial evaluation time for an event.

        RTH events before the entry cutoff wait five minutes. Events seen after
        the cutoff but before the close are intentionally skipped. Off-hours
        events are evaluated at 09:40 ET on the next exchange session.
        """

        local = value.astimezone(NEW_YORK)
        if self.is_session(local.date()) and time(9, 35) <= local.time() <= time(14, 45):
            return (value + timedelta(minutes=5)).astimezone(UTC)
        if self.is_session(local.date()) and time(14, 45) < local.time() < time(16, 0):
            return None

        search_date = local.date()
        if local.time() >= time(16, 0):
            search_date += timedelta(days=1)
        while not self.is_session(search_date):
            search_date += timedelta(days=1)
        return datetime.combine(search_date, time(9, 40), NEW_YORK).astimezone(UTC)
