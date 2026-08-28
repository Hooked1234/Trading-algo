from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from event_trader.eligibility import (
    CsvEligibilityResolver,
    EligibilityManifestError,
)

_HEADER = (
    "cik,symbol,valid_from,valid_through,known_at,common_stock,us_listing,"
    "corporate_actions_complete,source\n"
)


def _write(path: Path, rows: str) -> Path:
    path.write_text(_HEADER + rows, encoding="utf-8")
    return path


def test_csv_eligibility_resolves_exact_cik_symbol_and_historical_interval(
    tmp_path: Path, filing
) -> None:
    path = _write(
        tmp_path / "eligibility.csv",
        (
            "320193,AAPL,2018-01-01,,2020-01-01T00:00:00Z,true,true,true,"
            "authoritative-security-master\n"
        ),
    )
    resolver = CsvEligibilityResolver(path)

    result = resolver(filing, "AAPL")

    assert result is not None
    assert result.confirmed_eligible
    assert result.as_of == datetime(2020, 1, 1, tzinfo=UTC)
    assert resolver.manifest_sha256 in result.source


def test_csv_eligibility_refuses_knowledge_published_after_event(
    tmp_path: Path, filing
) -> None:
    path = _write(
        tmp_path / "eligibility.csv",
        (
            "320193,AAPL,2018-01-01,,2027-01-01T00:00:00Z,true,true,true,"
            "future-source\n"
        ),
    )

    assert CsvEligibilityResolver(path)(filing, "AAPL") is None


def test_csv_eligibility_rejects_overlapping_intervals(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "eligibility.csv",
        (
            "320193,AAPL,2018-01-01,2025-01-01,2020-01-01T00:00:00Z,true,true,true,a\n"
            "320193,AAPL,2024-01-01,,2020-01-01T00:00:00Z,true,true,true,b\n"
        ),
    )

    with pytest.raises(EligibilityManifestError, match="overlapping"):
        CsvEligibilityResolver(path)


@pytest.mark.parametrize("value", ["yes", "1", "maybe"])
def test_csv_eligibility_rejects_ambiguous_booleans(tmp_path: Path, value: str) -> None:
    path = _write(
        tmp_path / "eligibility.csv",
        (
            f"320193,AAPL,2018-01-01,,2020-01-01T00:00:00Z,{value},true,true,source\n"
        ),
    )

    with pytest.raises(EligibilityManifestError, match="must be true"):
        CsvEligibilityResolver(path)
