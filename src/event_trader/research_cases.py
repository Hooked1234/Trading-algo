"""Fail-closed assembly of reproducible historical backtest cases.

A research case is built from immutable data-lake facts only.  It deliberately
knows nothing about any insight, so the quant-only, keyword and AI variants all
evaluate the *same* case objects with the *same* ``case_input_sha256``.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import AwareDatetime, Field, model_validator

from .artifacts import HashedArtifact, Sha256, canonical_hash
from .backfill import CoverageRecord, CoverageStatus
from .backtest import (
    BacktestCase,
    BacktestEntryPoint,
    BacktestExitPoint,
    BacktestLineage,
)
from .calendar import NyseSessionCalendar
from .datasets import period_for
from .documents import DocumentIntegrityError, FilingDocumentLoader, verified_document_texts
from .domain import (
    Bar,
    DataSource,
    FilingEvent,
    FrozenModel,
    MarketSnapshot,
    PortfolioState,
    Quote,
)
from .features import build_feature_inputs, compute_market_features

_MAX_QUOTE_AGE = timedelta(seconds=5)
_SUPPORTED_LAGS = (1, 3, 5, 10)
_STARTING_EQUITY = 100_000
_CASE_HASH_SCHEMA = "research-case-input/1"
# Coverage bookkeeping that records *when the backfill ran*, never what it saw.
_COVERAGE_PROCESS_FIELDS = frozenset({"recorded_at", "detail"})


class ResearchCaseIntegrityError(ValueError):
    """Persisted inputs contradict each other and cannot form one case."""


class ResearchCaseExcluded(ResearchCaseIntegrityError):
    """Evidence required for a tradable case is absent, so the case is skipped.

    This is a deliberate, recorded exclusion rather than a data defect: missing
    evidence is never estimated and never becomes a zero-return trade.
    """


class HistoricalResearchData(Protocol):
    def read_filing(
        self,
        accession_number: str,
        *,
        source: DataSource = DataSource.SEC,
        quarter: str | None = None,
    ) -> FilingEvent: ...

    def read_bars(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        source: DataSource = DataSource.ALPACA_SIP,
        feed: str = "sip",
    ) -> tuple[Bar, ...]: ...

    def read_quotes(
        self,
        symbol: str,
        *,
        start: datetime,
        end: datetime,
        source: DataSource = DataSource.ALPACA_SIP,
        feed: str = "sip",
    ) -> tuple[Quote, ...]: ...


class HistoricalTradingState(FrozenModel):
    """Point-in-time halt and borrow evidence unavailable from OHLCV alone."""

    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,31}$")
    as_of: AwareDatetime
    known_at: AwareDatetime
    source: str = Field(min_length=1)
    halted: bool | None = None
    shortable: bool | None = None
    shortable_shares: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_state(self) -> HistoricalTradingState:
        if self.known_at < self.as_of:
            raise ValueError("trading state cannot be known before it is observed")
        if self.shortable is True and self.shortable_shares <= 0:
            raise ValueError("confirmed shortability requires positive available shares")
        if self.shortable is not True and self.shortable_shares != 0:
            raise ValueError("unconfirmed shortability cannot carry available shares")
        return self

    @property
    def short_enabled(self) -> bool:
        """Short evaluation stays disabled unless borrow evidence is explicit."""

        return self.shortable is True and self.shortable_shares > 0


class TradingStateManifest(HashedArtifact):
    """Versioned halt and borrow evidence used by every historical case."""

    manifest_version: Literal["1"] = "1"
    source: str = Field(min_length=1)
    entries: tuple[HistoricalTradingState, ...] = ()
    artifact_sha256: Sha256 = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_entries(self) -> TradingStateManifest:
        keys = tuple((entry.symbol, entry.as_of) for entry in self.entries)
        if len(set(keys)) != len(keys):
            raise ValueError("trading state manifest requires one entry per symbol and instant")
        return self

    def resolve(self, symbol: str, decision_time: datetime) -> HistoricalTradingState:
        """Return the newest entry observable at ``decision_time``.

        Both ``as_of`` and ``known_at`` must already have passed: an entry that
        only became knowable later cannot inform a point-in-time decision.
        """

        candidates = tuple(
            entry
            for entry in self.entries
            if entry.symbol == symbol
            and entry.as_of <= decision_time
            and entry.known_at <= decision_time
        )
        if not candidates:
            raise ResearchCaseExcluded(
                f"no point-in-time trading state for {symbol} at {decision_time.isoformat()}"
            )
        return max(candidates, key=lambda entry: (entry.as_of, entry.known_at))


class ResearchCaseFailureKind(StrEnum):
    EXCLUDED = "excluded"
    INTEGRITY_ERROR = "integrity_error"


class ResearchCaseFailure(FrozenModel):
    coverage_record_id: str = Field(min_length=1)
    accession_number: str = Field(pattern=r"^\d{10}-\d{2}-\d{6}$")
    kind: ResearchCaseFailureKind
    reason: str = Field(min_length=1)


class ResearchCaseBuildArtifact(HashedArtifact):
    """Complete, hashed accounting of one case-building run."""

    artifact_version: Literal["1"] = "1"
    requested_lag_minutes: int
    provider: str = Field(min_length=1)
    feed: str = Field(min_length=1)
    trading_state_manifest_sha256: Sha256 = Field(pattern=r"^[0-9a-f]{64}$")
    dataset_manifest_sha256: Sha256 | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    coverage_count: int = Field(ge=0)
    cases: tuple[BacktestCase, ...] = ()
    failures: tuple[ResearchCaseFailure, ...] = ()
    artifact_sha256: Sha256 = Field(default="0" * 64, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_accounting(self) -> ResearchCaseBuildArtifact:
        if self.requested_lag_minutes not in _SUPPORTED_LAGS:
            raise ValueError("research case artifact lag is not registered")
        if self.coverage_count != len(self.cases) + len(self.failures):
            raise ValueError("every selected coverage record must have one build outcome")
        if any(case.lineage is None for case in self.cases):
            raise ValueError("every research case requires complete lineage")
        record_ids = tuple(
            case.lineage.coverage_record_id for case in self.cases if case.lineage is not None
        ) + tuple(failure.coverage_record_id for failure in self.failures)
        if len(record_ids) != self.coverage_count or len(record_ids) != len(set(record_ids)):
            raise ValueError("research case artifact requires unique complete coverage accounting")
        if any(case.availability_lag_minutes != self.requested_lag_minutes for case in self.cases):
            raise ValueError("every research case must use the requested availability lag")
        hashes = self.case_hashes
        if len(set(hashes)) != len(hashes):
            raise ValueError("research cases must be uniquely identified by their input hash")
        return self

    @property
    def case_hashes(self) -> tuple[str, ...]:
        return tuple(
            case.lineage.case_input_sha256 for case in self.cases if case.lineage is not None
        )


class HistoricalResearchCaseBuilder:
    """Rebuild a case from immutable data-lake facts and one coverage record."""

    def __init__(
        self,
        *,
        data: HistoricalResearchData,
        documents: FilingDocumentLoader,
        calendar: NyseSessionCalendar | None = None,
        market_source: DataSource = DataSource.ALPACA_SIP,
        provider_label: str = "alpaca",
    ) -> None:
        if market_source is not DataSource.ALPACA_SIP:
            raise ValueError("version 1 historical cases require Alpaca SIP data")
        if not provider_label.strip():
            raise ValueError("historical provider label must not be empty")
        self._data = data
        self._documents = documents
        self._calendar = calendar or NyseSessionCalendar()
        self._market_source = market_source
        self._provider_label = provider_label.strip().casefold()

    def build(
        self,
        coverage: CoverageRecord,
        trading_state: HistoricalTradingState,
    ) -> BacktestCase:
        try:
            return self._build(coverage, trading_state)
        except ResearchCaseIntegrityError:
            raise
        except (DocumentIntegrityError, LookupError, OSError, TypeError, ValueError) as exc:
            raise ResearchCaseIntegrityError(str(exc)) from exc

    def build_all(
        self,
        coverage_records: Iterable[CoverageRecord],
        manifest: TradingStateManifest,
        *,
        lag_minutes: int,
        dataset_manifest_sha256: str | None = None,
    ) -> ResearchCaseBuildArtifact:
        """Produce exactly one case or one typed failure per coverage record."""

        if lag_minutes not in _SUPPORTED_LAGS:
            raise ValueError("research case lag is not registered")
        manifest.verify()
        selected = self._select(coverage_records, lag_minutes)
        cases: list[BacktestCase] = []
        failures: list[ResearchCaseFailure] = []
        for coverage in selected:
            try:
                # A lag-selected record always carries its schedule; the
                # CoverageRecord validator enforces that invariant.
                assert coverage.evaluation_at is not None
                state = manifest.resolve(coverage.symbol or "", coverage.evaluation_at)
                cases.append(self.build(coverage, state))
            except ResearchCaseExcluded as exc:
                failures.append(self._failure(coverage, ResearchCaseFailureKind.EXCLUDED, exc))
            except ResearchCaseIntegrityError as exc:
                failures.append(
                    self._failure(coverage, ResearchCaseFailureKind.INTEGRITY_ERROR, exc)
                )
        artifact = ResearchCaseBuildArtifact(
            requested_lag_minutes=lag_minutes,
            provider=self._provider_label,
            feed="sip",
            trading_state_manifest_sha256=manifest.artifact_sha256,
            dataset_manifest_sha256=dataset_manifest_sha256,
            coverage_count=len(selected),
            cases=tuple(cases),
            failures=tuple(failures),
        )
        return artifact.sealed()

    @staticmethod
    def _select(
        coverage_records: Iterable[CoverageRecord],
        lag_minutes: int,
    ) -> tuple[CoverageRecord, ...]:
        selected = tuple(record for record in coverage_records if record.lag_minutes == lag_minutes)
        record_ids = tuple(record.record_id for record in selected)
        if len(set(record_ids)) != len(record_ids):
            raise ResearchCaseIntegrityError("coverage selection contains duplicate record ids")
        return tuple(sorted(selected, key=lambda record: record.record_id))

    @staticmethod
    def _failure(
        coverage: CoverageRecord,
        kind: ResearchCaseFailureKind,
        exc: Exception,
    ) -> ResearchCaseFailure:
        return ResearchCaseFailure(
            coverage_record_id=coverage.record_id,
            accession_number=coverage.accession_number,
            kind=kind,
            reason=str(exc) or kind.value,
        )

    def _build(
        self,
        coverage: CoverageRecord,
        trading_state: HistoricalTradingState,
    ) -> BacktestCase:
        self._require_complete_coverage(coverage)
        assert coverage.symbol is not None
        assert coverage.lag_minutes is not None
        assert coverage.available_at is not None
        assert coverage.evaluation_at is not None
        assert coverage.window_end is not None
        assert coverage.bundle_start is not None
        assert coverage.bundle_end is not None
        assert coverage.feed is not None
        assert coverage.eligibility is not None

        stored_filing = self._data.read_filing(
            coverage.accession_number,
            quarter=coverage.quarter,
        )
        filing = self._counterfactual_filing(stored_filing, coverage.available_at)
        symbol = coverage.symbol
        self._validate_identity(coverage, filing, trading_state)
        self._validate_timing(coverage, filing)
        self._validate_trading_state(trading_state, coverage.evaluation_at)

        document_text = self._documents.load_text(filing)
        document_hashes = self._verified_document_hashes(filing, document_text)

        bars = self._data.read_bars(
            symbol,
            start=coverage.bundle_start,
            end=coverage.bundle_end,
            source=self._market_source,
            feed=coverage.feed,
        )
        benchmark_bars = self._data.read_bars(
            "SPY",
            start=coverage.bundle_start,
            end=coverage.bundle_end,
            source=self._market_source,
            feed=coverage.feed,
        )
        quotes = self._data.read_quotes(
            symbol,
            start=coverage.evaluation_at - _MAX_QUOTE_AGE,
            end=coverage.window_end,
            source=self._market_source,
            feed=coverage.feed,
        )

        inputs = build_feature_inputs(
            symbol=symbol,
            symbol_one_minute_bars=bars,
            spy_one_minute_bars=benchmark_bars,
            as_of=coverage.evaluation_at,
        )
        features = compute_market_features(inputs)
        decision_quote = _fresh_quote_at(quotes, coverage.evaluation_at)
        reprice_at = coverage.evaluation_at + timedelta(seconds=5)
        entry_reprice = BacktestEntryPoint(
            attempted_at=reprice_at,
            quote=_fresh_quote_at(quotes, reprice_at),
        )
        bars_by_timestamp = {bar.timestamp: bar for bar in bars}
        exit_points = tuple(
            _exit_point(
                symbol=symbol,
                timestamp=coverage.evaluation_at + timedelta(minutes=minute),
                bars_by_timestamp=bars_by_timestamp,
                quotes=quotes,
            )
            for minute in range(1, 61)
        )

        eligibility = coverage.eligibility
        market = MarketSnapshot(
            symbol=symbol,
            as_of=coverage.evaluation_at,
            quote=decision_quote,
            last=features.last,
            session_vwap=features.session_vwap,
            median_dollar_volume_20d=features.median_dollar_volume_20d,
            beta_adjusted_return_z=features.beta_adjusted_return_z,
            relative_volume=features.relative_volume,
            atr_5m=features.atr_5m,
            data_fresh=True,
            market_data_live=False,
            halted=trading_state.halted is True,
            shortable=trading_state.short_enabled,
            shortable_shares=(trading_state.shortable_shares if trading_state.short_enabled else 0),
            security_type="common_stock" if eligibility.common_stock is True else "other",
            us_listed=eligibility.us_listing is True,
        )
        portfolio = PortfolioState(
            as_of=coverage.evaluation_at,
            nav=_STARTING_EQUITY,
            peak_nav=_STARTING_EQUITY,
            cash=_STARTING_EQUITY,
            strategy_equity=_STARTING_EQUITY,
            strategy_peak_equity=_STARTING_EQUITY,
            strategy_realized_pnl_today=0,
            strategy_unrealized_pnl=0,
        )
        sample_period = period_for(filing.accepted_at)
        case = BacktestCase(
            decision_time=coverage.evaluation_at,
            snapshot={
                "filing": filing,
                "market": market,
                "document_text": document_text,
            },
            portfolio=portfolio,
            entry_reprice=entry_reprice,
            exit_points=exit_points,
            availability_lag_minutes=coverage.lag_minutes,
            out_of_sample=sample_period in {"validation", "holdout"},
        )
        case_hash = _case_input_hash(
            case,
            coverage=coverage,
            trading_state=trading_state,
            document_hashes=document_hashes,
        )
        return case.model_copy(
            update={
                "lineage": BacktestLineage(
                    coverage_record_id=coverage.record_id,
                    scenario=coverage.scenario or "missing",
                    provider=coverage.provider or "missing",
                    feed=coverage.feed,
                    sample_period=sample_period,
                    case_input_sha256=case_hash,
                )
            }
        )

    @staticmethod
    def _counterfactual_filing(filing: FilingEvent, available_at: datetime) -> FilingEvent:
        """Replace operational stamps with the registered availability counterfactual.

        ``first_seen_at``/``retrieved_at`` record when the ingestion process ran.
        Historical availability is an explicit counterfactual (SEC acceptance plus
        the selected lag), so the case uses that instant for both.  The live
        staleness rule therefore measures the same distance it measures in
        production, and the case hash stays free of process timestamps.
        """

        if available_at < filing.accepted_at:
            raise ResearchCaseIntegrityError("coverage availability predates SEC acceptance")
        return filing.model_copy(
            update={"first_seen_at": available_at, "retrieved_at": available_at}
        )

    def _require_complete_coverage(self, coverage: CoverageRecord) -> None:
        if coverage.status is not CoverageStatus.AVAILABLE:
            raise ResearchCaseExcluded("coverage status is not available")
        if not coverage.tradable_coverage_complete or coverage.scenario_covered is not True:
            raise ResearchCaseExcluded("coverage is not tradable and complete")
        if coverage.feature_history is None or not coverage.feature_history.complete:
            raise ResearchCaseExcluded("feature history coverage is incomplete")
        if coverage.eligibility is None or not coverage.eligibility.confirmed_eligible:
            raise ResearchCaseExcluded("point-in-time eligibility is incomplete")
        if coverage.lag_minutes not in _SUPPORTED_LAGS:
            raise ResearchCaseIntegrityError("coverage lag is not registered")
        if coverage.provider is None or coverage.provider.casefold() != self._provider_label:
            raise ResearchCaseIntegrityError("coverage provider does not match the data reader")
        if coverage.feed is None or coverage.feed.casefold() != "sip":
            raise ResearchCaseIntegrityError("version 1 research requires the SIP feed")
        if coverage.benchmark_symbol != "SPY":
            raise ResearchCaseIntegrityError("version 1 research requires SPY benchmark coverage")

    @staticmethod
    def _validate_identity(
        coverage: CoverageRecord,
        filing: FilingEvent,
        trading_state: HistoricalTradingState,
    ) -> None:
        symbol = coverage.symbol
        eligibility = coverage.eligibility
        if filing.accession_number != coverage.accession_number:
            raise ResearchCaseIntegrityError("coverage and filing accession mismatch")
        if not filing.complete or filing.form not in {"8-K", "8-K/A"}:
            raise ResearchCaseIntegrityError("filing is incomplete or unsupported")
        if len(filing.symbols) != 1 or filing.symbols[0] != symbol:
            raise ResearchCaseIntegrityError("coverage and filing symbol mismatch")
        if trading_state.symbol != symbol:
            raise ResearchCaseIntegrityError("trading state and filing symbol mismatch")
        if eligibility is None or eligibility.accession_number != filing.accession_number:
            raise ResearchCaseIntegrityError("eligibility and filing accession mismatch")
        if eligibility.as_of > filing.accepted_at:
            raise ResearchCaseIntegrityError("eligibility evidence was not known at acceptance")

    def _validate_timing(self, coverage: CoverageRecord, filing: FilingEvent) -> None:
        assert coverage.lag_minutes is not None
        assert coverage.available_at is not None
        assert coverage.evaluation_at is not None
        assert coverage.window_end is not None
        expected_available = filing.accepted_at + timedelta(minutes=coverage.lag_minutes)
        if coverage.available_at != expected_available:
            raise ResearchCaseIntegrityError("coverage availability does not match the lag")
        expected_evaluation = self._calendar.next_evaluation_time(expected_available)
        if expected_evaluation is None:
            raise ResearchCaseExcluded("filing is outside the registered entry schedule")
        if coverage.evaluation_at != expected_evaluation:
            raise ResearchCaseIntegrityError("coverage evaluation is not live-equivalent")
        if coverage.window_end < coverage.evaluation_at + timedelta(minutes=60):
            raise ResearchCaseIntegrityError("coverage does not include the full exit horizon")

    @staticmethod
    def _validate_trading_state(
        trading_state: HistoricalTradingState,
        decision_time: datetime,
    ) -> None:
        if trading_state.halted is None:
            raise ResearchCaseExcluded("historical halt state is unknown")
        if trading_state.as_of > decision_time or trading_state.known_at > decision_time:
            raise ResearchCaseIntegrityError("historical trading state is from the future")
        if decision_time - trading_state.as_of > _MAX_QUOTE_AGE:
            raise ResearchCaseIntegrityError("historical trading state is stale")

    @staticmethod
    def _verified_document_hashes(filing: FilingEvent, document_text: str) -> tuple[str, ...]:
        document_texts = verified_document_texts(filing, document_text)
        if not document_texts:
            raise ResearchCaseIntegrityError("filing document boundaries are unverifiable")
        return tuple(sorted(document_texts))


def _fresh_quote_at(quotes: tuple[Quote, ...], timestamp: datetime) -> Quote:
    eligible = tuple(
        quote
        for quote in quotes
        if quote.timestamp <= timestamp and timestamp - quote.timestamp <= _MAX_QUOTE_AGE
    )
    if not eligible:
        raise ResearchCaseIntegrityError(f"no fresh quote at {timestamp.isoformat()}")
    return max(eligible, key=lambda quote: quote.timestamp)


def _exit_point(
    *,
    symbol: str,
    timestamp: datetime,
    bars_by_timestamp: dict[datetime, Bar],
    quotes: tuple[Quote, ...],
) -> BacktestExitPoint:
    bar = bars_by_timestamp.get(timestamp)
    if bar is None or bar.symbol != symbol:
        raise ResearchCaseIntegrityError(f"missing exit bar at {timestamp.isoformat()}")
    return BacktestExitPoint(
        timestamp=timestamp,
        bar=bar,
        quote=_fresh_quote_at(quotes, timestamp),
    )


def _case_input_hash(
    case: BacktestCase,
    *,
    coverage: CoverageRecord,
    trading_state: HistoricalTradingState,
    document_hashes: Sequence[str],
) -> str:
    """Hash exactly the point-in-time inputs a live decision would have had.

    Local file paths, ingestion timestamps and any model output are excluded by
    construction, so quant-only, keyword and AI runs share one case identity.
    """

    filing = case.snapshot.filing
    documents = sorted(
        (
            {"kind": document.kind, "sha256": document.sha256, "url": document.url}
            for document in filing.documents
        ),
        key=lambda document: (document["sha256"], document["kind"], document["url"]),
    )
    payload = {
        "schema": _CASE_HASH_SCHEMA,
        "coverage": coverage.model_dump(mode="json", exclude=set(_COVERAGE_PROCESS_FIELDS)),
        "filing": {
            "accession_number": filing.accession_number,
            "cik": filing.cik,
            "form": filing.form,
            "items": list(filing.items),
            "symbols": list(filing.symbols),
            "accepted_at": filing.accepted_at.isoformat(),
            "source": filing.source.value,
            "complete": filing.complete,
            "documents": documents,
        },
        "verified_document_sha256": sorted(document_hashes),
        "market": case.snapshot.market.model_dump(mode="json"),
        "entry_reprice": (
            case.entry_reprice.model_dump(mode="json") if case.entry_reprice is not None else None
        ),
        "exit_points": [point.model_dump(mode="json") for point in case.exit_points],
        "portfolio": case.portfolio.model_dump(mode="json"),
        "trading_state": trading_state.model_dump(mode="json"),
        "decision_time": case.decision_time.isoformat(),
        "availability_lag_minutes": case.availability_lag_minutes,
        "out_of_sample": case.out_of_sample,
    }
    return canonical_hash(payload)


__all__ = [
    "HistoricalResearchCaseBuilder",
    "HistoricalResearchData",
    "HistoricalTradingState",
    "ResearchCaseBuildArtifact",
    "ResearchCaseExcluded",
    "ResearchCaseFailure",
    "ResearchCaseFailureKind",
    "ResearchCaseIntegrityError",
    "TradingStateManifest",
]
