"""Fail-closed Interactive Brokers paper adapter.

``ibapi`` is optional.  Importing this module never requires it; a native
backend is constructed only when ``connect`` is explicitly called.  Tests and
replays inject an ``IBKRBackend`` and therefore never open a socket.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from functools import wraps
from threading import Event, RLock, Thread
from typing import Any, ClassVar, Protocol, runtime_checkable

from pydantic import ValidationError

from event_trader.broker import (
    BrokerError,
    BrokerReadiness,
    Clock,
    OrderStateMachine,
    PaperAccountGuard,
    PaperAccountViolation,
    ReadinessCheck,
    ReadinessProfile,
    ReconciliationResult,
    UnknownOrder,
)
from event_trader.domain import (
    Direction,
    ExecutionFill,
    ExecutionReport,
    ExecutionStatus,
    OrderIntent,
    OrderSide,
    PortfolioState,
    Position,
    is_paper_account_id,
    money,
    utc_now,
)
from event_trader.risk import pending_entry_exposures

try:  # pragma: no cover - availability depends on the local optional extra
    from ibapi.client import EClient as _EClient
    from ibapi.contract import Contract as _Contract
    from ibapi.execution import ExecutionFilter as _ExecutionFilter
    from ibapi.order import Order as _Order
    from ibapi.wrapper import EWrapper as _EWrapper

    _IBAPI_IMPORT_ERROR: Exception | None = None
except (ImportError, ModuleNotFoundError) as exc:  # pragma: no cover
    _EClient = None  # type: ignore[assignment]
    _Contract = None  # type: ignore[assignment]
    _ExecutionFilter = None  # type: ignore[assignment]
    _Order = None  # type: ignore[assignment]
    _EWrapper = object  # type: ignore[assignment,misc]
    _IBAPI_IMPORT_ERROR = exc


class IBKRError(BrokerError):
    """Base class for IBKR adapter failures."""


class IBAPINotInstalled(IBKRError):
    """The native backend was requested without the optional dependency."""


class IBKRTransportError(IBKRError):
    """IB Gateway/TWS did not complete an operation safely."""


class IBKRRecoveryIncomplete(IBKRTransportError):
    """Persisted open orders have not all been confirmed by IBKR."""


@dataclass(frozen=True, slots=True)
class IBKRRemoteOrderSnapshot:
    """Authoritative result of one bounded IBKR order reconciliation."""

    reports: tuple[ExecutionReport, ...]
    seen_order_ids: frozenset[str]
    unknown_remote_order_ids: frozenset[str] = frozenset()
    fills: tuple[ExecutionFill, ...] = ()
    complete: bool = True


@dataclass(frozen=True, slots=True)
class IBKRConnectionConfig:
    """Local TWS/IB Gateway connection settings for a paper session."""

    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 0
    timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        if not self.host:
            raise ValueError("host must not be empty")
        if not 1 <= self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        if self.client_id < 0:
            raise ValueError("client_id cannot be negative")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


@runtime_checkable
class IBKRBackend(Protocol):
    """Narrow transport seam used by the safe broker adapter."""

    def connect(self) -> None: ...

    def disconnect(self) -> None: ...

    def is_connected(self) -> bool: ...

    def account_ids(self) -> tuple[str, ...]: ...

    def ready_for_orders(self) -> bool: ...

    def market_data_live(self) -> bool: ...

    def order_scope_authoritative(self) -> bool: ...

    def submit_order(self, intent: OrderIntent) -> str: ...

    def cancel_order(self, broker_order_id: str) -> None: ...

    def reconcile_orders(
        self,
        account_id: str,
        known_orders: tuple[tuple[OrderIntent, ExecutionReport], ...],
    ) -> IBKRRemoteOrderSnapshot: ...

    def portfolio_state(self, account_id: str) -> PortfolioState: ...


@runtime_checkable
class IBKRRecoveryStore(Protocol):
    """Minimal durable read interface needed before IBKR reconciliation."""

    async def list_orders_for_reconciliation(
        self, *, limit: int = 1_000
    ) -> tuple[OrderIntent, ...]: ...

    async def get_execution_report(self, order_id: str) -> ExecutionReport | None: ...

    async def list_order_intents(self, *, limit: int = 1_000) -> tuple[OrderIntent, ...]: ...


def ibapi_available() -> bool:
    """Return whether the optional official ``ibapi`` package is importable."""

    return _IBAPI_IMPORT_ERROR is None


def _assert_paper_submission(intent: OrderIntent) -> None:
    if intent.submission_mode != "paper":
        raise PaperAccountViolation("shadow intents are research artifacts and cannot reach IBKR")


def _latch_callback_faults(cls: type) -> type:
    """Route every wrapper callback through the owner's fault latch.

    ``EClient.run`` catches only ``KeyboardInterrupt``, ``SystemExit`` and
    ``BadMessage``; anything else ends the reader thread, and with it every
    further order, fill and cancel callback.  Swallowing the error here is only
    defensible because ``_record_callback_failure`` then refuses every
    authoritative operation.  Without that latch this wrapper would remove the
    single signal the process has today and turn fail-closed into fail-silent.
    """

    for name, attribute in list(vars(cls).items()):
        if name.startswith("__") or not callable(attribute):
            continue
        setattr(cls, name, _guarded_callback(name, attribute))
    return cls


def _guarded_callback(name: str, method: Any) -> Any:
    @wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return method(self, *args, **kwargs)
        # Broad by intent: the latch below is the handler of last resort.
        except Exception as exc:
            self._owner._record_callback_failure(name, exc)
            return None

    return wrapper


if _IBAPI_IMPORT_ERROR is None:  # pragma: no branch

    @_latch_callback_faults
    class _NativeClient(_EWrapper, _EClient):  # type: ignore[misc,valid-type]
        def __init__(self, owner: NativeIBAPIBackend) -> None:
            _EWrapper.__init__(self)
            _EClient.__init__(self, wrapper=self)
            self._owner = owner

        def nextValidId(self, order_id: int) -> None:
            self._owner._on_next_valid_id(order_id)

        def managedAccounts(self, accounts_list: str) -> None:
            self._owner._on_managed_accounts(accounts_list)

        def tickPrice(self, req_id: int, tick_type: int, price: float, attrib: Any) -> None:
            if price > 0:
                self._owner._on_live_market_data()
            hook = self._owner._market_data_hook
            if hook is not None:
                hook.on_tick_price(req_id, tick_type, price)

        def tickSize(self, req_id: int, tick_type: int, size: Any) -> None:
            hook = self._owner._market_data_hook
            if hook is not None:
                hook.on_tick_size(req_id, tick_type, size)

        def tickGeneric(self, req_id: int, tick_type: int, value: float) -> None:
            hook = self._owner._market_data_hook
            if hook is not None:
                hook.on_tick_generic(req_id, tick_type, value)

        def marketDataType(self, req_id: int, market_data_type: int) -> None:
            market_hook = self._owner._market_data_hook
            if market_hook is not None:
                market_hook.on_market_data_type(req_id, market_data_type)
            bar_hook = self._owner._bar_hook
            if bar_hook is not None:
                bar_hook.on_market_data_type(req_id, market_data_type)

        def orderStatus(
            self,
            order_id: int,
            status: str,
            filled: Any,
            remaining: Any,
            avg_fill_price: float,
            perm_id: int,
            parent_id: int,
            last_fill_price: float,
            client_id: int,
            why_held: str,
            mkt_cap_price: float = 0.0,
        ) -> None:
            self._owner._on_order_status(order_id, status, filled, avg_fill_price)

        def openOrder(
            self,
            order_id: int,
            contract: Any,
            order: Any,
            order_state: Any,
        ) -> None:
            self._owner._on_remote_order(
                order_id,
                contract,
                order,
                order_state,
                completed=False,
            )

        def openOrderEnd(self) -> None:
            self._owner._on_open_order_end()

        def execDetails(self, req_id: int, contract: Any, execution: Any) -> None:
            self._owner._on_exec_details(req_id, contract, execution)

        def execDetailsEnd(self, req_id: int) -> None:
            self._owner._on_exec_details_end(req_id)

        def commissionReport(self, commission_report: Any) -> None:
            self._owner._on_commission_report(commission_report)

        def historicalData(self, req_id: int, bar: Any) -> None:
            hook = self._owner._bar_hook
            if hook is not None:
                hook.on_historical_data(req_id, bar)

        def historicalDataEnd(self, req_id: int, start: str, end: str) -> None:
            hook = self._owner._bar_hook
            if hook is not None:
                hook.on_historical_data_end(req_id, start, end)

        def realtimeBar(
            self,
            req_id: int,
            time_: int,
            open_: float,
            high: float,
            low: float,
            close: float,
            volume: Any,
            wap: Any,
            count: int,
        ) -> None:
            hook = self._owner._bar_hook
            if hook is not None:
                hook.on_realtime_bar(req_id, time_, open_, high, low, close, volume, wap, count)

        def completedOrder(
            self,
            contract: Any,
            order: Any,
            order_state: Any,
        ) -> None:
            self._owner._on_remote_order(
                int(getattr(order, "orderId", -1)),
                contract,
                order,
                order_state,
                completed=True,
            )

        def completedOrdersEnd(self) -> None:
            self._owner._on_completed_orders_end()

        def updateAccountValue(
            self, key: str, value: str, currency: str, account_name: str
        ) -> None:
            self._owner._on_account_value(key, value, currency, account_name)

        def updatePortfolio(
            self,
            contract: Any,
            position: Any,
            market_price: float,
            market_value: float,
            average_cost: float,
            unrealized_pnl: float,
            realized_pnl: float,
            account_name: str,
        ) -> None:
            self._owner._on_portfolio(
                contract,
                position,
                market_price,
                average_cost,
                account_name,
            )

        def accountDownloadEnd(self, account_name: str) -> None:
            self._owner._on_account_download_end(account_name)

        def error(
            self,
            req_id: int,
            error_code: int,
            error_string: str,
            advanced_order_reject_json: str = "",
        ) -> None:
            self._owner._on_error(req_id, error_code, error_string)

        def connectionClosed(self) -> None:
            hook = self._owner._market_data_hook
            if hook is not None:
                hook.on_connection_lost()

else:

    class _NativeClient:  # pragma: no cover - used only to explain missing extra
        def __init__(self, owner: NativeIBAPIBackend) -> None:
            raise IBAPINotInstalled(
                "optional dependency 'ibapi' is required for the native IBKR backend"
            )


class NativeIBAPIBackend:
    """Thin official-API backend; it never connects during construction.

    Only the methods that actually speak to IB Gateway are excluded from
    coverage.  The callback reducers below turn broker messages into local
    facts without touching the transport, and they carry the order and fill
    truth of this system - measuring them is the point of the metric.
    """

    _SUBMITTED_STATUSES: ClassVar[set[str]] = {
        "ApiPending",
        "PendingSubmit",
        "PreSubmitted",
        "Submitted",
        "PendingCancel",
    }

    def __init__(
        self,
        config: IBKRConnectionConfig | None = None,
        *,
        clock: Clock = utc_now,
    ) -> None:
        if not ibapi_available():
            raise IBAPINotInstalled(
                "optional dependency 'ibapi' is required for the native IBKR backend"
            ) from _IBAPI_IMPORT_ERROR
        self._init_state(config, clock=clock)
        self._client = _NativeClient(self)

    @classmethod
    def without_transport(
        cls,
        config: IBKRConnectionConfig | None = None,
        *,
        clock: Clock = utc_now,
    ) -> NativeIBAPIBackend:
        """Build a backend that owns its callback state but opens no socket.

        The callback reducers turn broker messages into local facts and do not
        touch the transport.  Exercising them must therefore not require
        ``ibapi`` or a running Gateway; this is also the only way this layer can
        be tested on a machine that has neither.
        """

        backend = object.__new__(cls)
        backend._init_state(config, clock=clock)
        backend._client = None
        return backend

    def _init_state(
        self,
        config: IBKRConnectionConfig | None = None,
        *,
        clock: Clock = utc_now,
    ) -> None:
        self._config = config or IBKRConnectionConfig()
        self._clock = clock
        self._lock = RLock()
        self._ready_event = Event()
        self._account_event = Event()
        self._thread: Thread | None = None
        self._next_order_id: int | None = None
        self._account_ids: tuple[str, ...] = ()
        self._market_data_seen = False
        self._intents: dict[int, OrderIntent] = {}
        self._reports: dict[int, ExecutionReport] = {}
        self._fills_by_execution_id: dict[str, ExecutionFill] = {}
        self._callback_failures: list[tuple[str, str]] = []
        self._deferred_inconsistencies: set[str] = set()
        self._fill_ids_by_broker_order: dict[int, set[str]] = {}
        self._pending_commissions: dict[str, Decimal] = {}
        self._account_values: dict[str, dict[str, Decimal]] = {}
        self._positions: dict[str, dict[str, Position]] = {}
        self._download_events: dict[str, Event] = {}
        self._peak_nav: dict[str, Decimal] = {}
        self._last_error: tuple[int, int, str] | None = None
        self._reconciliation_account: str | None = None
        self._known_intents_by_key: dict[str, OrderIntent] = {}
        self._remote_seen_order_ids: set[str] = set()
        self._unknown_remote_order_ids: set[str] = set()
        self._open_orders_event = Event()
        self._completed_orders_event = Event()
        self._execution_events: dict[int, Event] = {}
        self._next_request_id = 1_000_000
        self._market_data_hook: Any | None = None
        self._bar_hook: Any | None = None

    @property
    def client(self) -> Any:
        """Official client used by the market-data hooks in the same session."""

        return self._client

    def attach_market_data_hooks(
        self,
        *,
        market_data: Any,
        bars: Any,
    ) -> None:
        """Forward official callbacks into the quote and bar accumulators."""

        self._market_data_hook = market_data
        self._bar_hook = bars

    def connect(self) -> None:  # pragma: no cover - live IB Gateway
        if self._client is None:
            raise IBKRTransportError("this backend was built without a transport")
        if self.is_connected():
            return
        try:
            connected = self._client.connect(
                self._config.host, self._config.port, self._config.client_id
            )
        except Exception as exc:
            raise IBKRTransportError(f"IBKR connection failed: {exc.__class__.__name__}") from exc
        if connected is False:
            raise IBKRTransportError("IBKR connection was refused")
        self._thread = Thread(target=self._client.run, name="ibkr-api", daemon=True)
        self._thread.start()
        if not self._ready_event.wait(self._config.timeout_seconds):
            self.disconnect()
            raise IBKRTransportError("IBKR did not provide a valid order id in time")
        self._client.reqManagedAccts()
        self._account_event.wait(self._config.timeout_seconds)
        if self._config.client_id == 0:
            self._client.reqAutoOpenOrders(True)

    def disconnect(self) -> None:
        if self._client is not None and self._client.isConnected():
            self._client.disconnect()

    def is_connected(self) -> bool:
        return self._client is not None and bool(self._client.isConnected())

    def account_ids(self) -> tuple[str, ...]:
        with self._lock:
            return self._account_ids

    def ready_for_orders(self) -> bool:
        with self._lock:
            return (
                self.is_connected()
                and self._next_order_id is not None
                and not self._callback_failures
            )

    def market_data_live(self) -> bool:
        with self._lock:
            return self._market_data_seen

    def order_scope_authoritative(self) -> bool:
        """Manual TWS orders are authoritative only for the binding client 0."""

        return self._config.client_id == 0

    def mark_market_data_live(self) -> None:
        """Allow an external market-data probe to mark the session live."""

        self._on_live_market_data()

    def submit_order(self, intent: OrderIntent) -> str:  # pragma: no cover - live IB Gateway
        _assert_paper_submission(intent)
        if not self.ready_for_orders():
            raise IBKRTransportError("IBKR has no valid next order id")
        with self._lock:
            assert self._next_order_id is not None
            broker_order_id = self._next_order_id
            self._next_order_id += 1
            self._intents[broker_order_id] = intent

        contract = _Contract()
        contract.symbol = intent.symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        order = _Order()
        order.action = "BUY" if intent.side in {OrderSide.BUY, OrderSide.BUY_TO_COVER} else "SELL"
        order.orderType = "LMT"
        order.totalQuantity = intent.quantity
        order.lmtPrice = float(intent.limit_price)
        order.tif = intent.time_in_force
        order.account = intent.account_id
        order.orderRef = intent.idempotency_key
        order.transmit = True
        try:
            self._client.placeOrder(broker_order_id, contract, order)
        except Exception as exc:
            raise IBKRTransportError(
                f"IBKR submit outcome is unknown: {exc.__class__.__name__}"
            ) from exc
        return str(broker_order_id)

    def cancel_order(self, broker_order_id: str) -> None:
        try:
            numeric_id = int(broker_order_id)
        except ValueError as exc:
            raise IBKRTransportError("invalid IBKR broker order id") from exc
        try:
            self._client.cancelOrder(numeric_id, "")
        except TypeError:  # older official API signature
            self._client.cancelOrder(numeric_id)
        except Exception as exc:
            raise IBKRTransportError(
                f"IBKR cancel outcome is unknown: {exc.__class__.__name__}"
            ) from exc

    def reconcile_orders(
        self,
        account_id: str,
        known_orders: tuple[tuple[OrderIntent, ExecutionReport], ...],
    ) -> IBKRRemoteOrderSnapshot:
        """Request open, completed, and execution streams and await all end markers."""

        # A broken callback layer invalidates everything this session could
        # report, so it is refused before any transport question is asked.
        with self._lock:
            if self._callback_failures:
                raise IBKRTransportError(
                    f"{len(self._callback_failures)} IBKR callback(s) could not be "
                    "processed; this session's order view is not authoritative"
                )
        if not self.is_connected():
            raise IBKRTransportError("IBKR is disconnected during order reconciliation")
        if _ExecutionFilter is None:
            raise IBAPINotInstalled("official ibapi execution filter is unavailable")
        with self._lock:
            if self._reconciliation_account is not None:
                raise IBKRTransportError("an IBKR order reconciliation is already active")
            # Everything that can still refuse the run happens first.  Marking
            # the session as reconciling is what the finally below has to undo,
            # so nothing that throws may sit between the mark and the try.
            for intent, _report in known_orders:
                _assert_paper_submission(intent)
            bound: list[tuple[int, OrderIntent, ExecutionReport]] = []
            for intent, report in known_orders:
                if report.broker_order_id is None:
                    continue
                try:
                    bound.append((int(report.broker_order_id), intent, report))
                except ValueError as exc:
                    raise IBKRTransportError(
                        "persisted IBKR broker order id is not numeric"
                    ) from exc

            self._known_intents_by_key = {
                intent.idempotency_key: intent for intent, _report in known_orders
            }
            for broker_order_id, intent, report in bound:
                self._intents[broker_order_id] = intent
                self._reports.setdefault(broker_order_id, report)
            self._remote_seen_order_ids = set()
            # Discards from normal trading are only decidable here.
            self._unknown_remote_order_ids = set(self._deferred_inconsistencies)
            self._deferred_inconsistencies = set()
            self._open_orders_event = Event()
            self._completed_orders_event = Event()
            request_id = self._next_request_id
            self._next_request_id += 1
            execution_event = Event()
            self._execution_events = {request_id: execution_event}
            self._reconciliation_account = account_id

        completed = False
        try:
            execution_filter = _ExecutionFilter()
            execution_filter.acctCode = account_id
            try:
                self._client.reqAllOpenOrders()
                self._client.reqExecutions(request_id, execution_filter)
                self._client.reqCompletedOrders(False)
            except Exception as exc:
                raise IBKRTransportError(
                    f"IBKR reconciliation request failed: {exc.__class__.__name__}"
                ) from exc

            markers = (
                ("openOrderEnd", self._open_orders_event),
                ("execDetailsEnd", execution_event),
                ("completedOrdersEnd", self._completed_orders_event),
            )
            for marker, event in markers:
                if not event.wait(self._config.timeout_seconds):
                    raise IBKRTransportError(f"IBKR reconciliation timed out before {marker}")

            with self._lock:
                seen = frozenset(self._remote_seen_order_ids)
                unknown = frozenset(self._unknown_remote_order_ids)
                reports = tuple(
                    sorted(
                        (
                            report
                            for broker_id, report in self._reports.items()
                            if report.order_id in seen
                            and self._intents[broker_id].account_id == account_id
                        ),
                        key=lambda report: (report.occurred_at, report.order_id),
                    )
                )
                fills = tuple(
                    sorted(
                        (
                            fill
                            for fill in self._fills_by_execution_id.values()
                            if fill.order_id in seen
                        ),
                        key=lambda fill: (fill.occurred_at, fill.execution_id),
                    )
                )
                # Closed inside the same lock as the snapshot: afterwards no
                # callback can still write into a set that was already returned.
                self._reconciliation_account = None
                self._execution_events = {}
                completed = True
            return IBKRRemoteOrderSnapshot(
                reports=reports,
                seen_order_ids=seen,
                unknown_remote_order_ids=unknown,
                fills=fills,
            )
        finally:
            if not completed:
                with self._lock:
                    # An aborted reconciliation decides nothing.  The discards it
                    # took over are still undecided and have to survive for the
                    # next attempt instead of vanishing with the failed run.
                    self._deferred_inconsistencies |= self._unknown_remote_order_ids
                    # Released on every abort: a single timeout must not leave
                    # the session marked as reconciling forever.
                    self._reconciliation_account = None
                    self._execution_events = {}

    # pragma-scoped below: this method needs a live IB Gateway session.
    def portfolio_state(  # pragma: no cover
        self, account_id: str
    ) -> PortfolioState:
        if account_id not in self.account_ids():
            raise IBKRTransportError("paper account is not present in IBKR session")
        event = Event()
        with self._lock:
            self._download_events[account_id] = event
            self._positions[account_id] = {}
        self._client.reqAccountUpdates(True, account_id)
        try:
            if not event.wait(self._config.timeout_seconds):
                raise IBKRTransportError("IBKR account reconciliation timed out")
        finally:
            self._client.reqAccountUpdates(False, account_id)

        with self._lock:
            values = self._account_values.get(account_id, {})
            nav = values.get("NetLiquidation")
            cash = values.get("TotalCashValue", values.get("CashBalance"))
            if nav is None or nav <= 0 or cash is None:
                raise IBKRTransportError("IBKR account values are incomplete")
            peak_nav = max(nav, self._peak_nav.get(account_id, nav))
            self._peak_nav[account_id] = peak_nav
            return PortfolioState(
                as_of=self._clock(),
                nav=nav,
                peak_nav=peak_nav,
                cash=cash,
                realized_pnl_today=values.get("RealizedPnL", Decimal("0")),
                unrealized_pnl=values.get("UnrealizedPnL", Decimal("0")),
                positions=tuple(
                    sorted(
                        self._positions.get(account_id, {}).values(),
                        key=lambda position: position.symbol,
                    )
                ),
                broker_connected=self.is_connected(),
                reconciled=True,
            )

    def _on_next_valid_id(self, order_id: int) -> None:
        with self._lock:
            self._next_order_id = order_id
        self._ready_event.set()

    def _on_managed_accounts(self, accounts_list: str) -> None:
        with self._lock:
            self._account_ids = tuple(
                account.strip() for account in accounts_list.split(",") if account.strip()
            )
        self._account_event.set()

    def _on_live_market_data(self) -> None:
        with self._lock:
            self._market_data_seen = True

    def _on_order_status(
        self, broker_order_id: int, raw_status: str, filled: Any, avg_price: float
    ) -> None:
        with self._lock:
            intent = self._intents.get(broker_order_id)
            if intent is None:
                return
            if self._reconciliation_account == intent.account_id:
                self._remote_seen_order_ids.add(intent.order_id)
            try:
                filled_decimal = Decimal(str(filled))
                if (
                    not filled_decimal.is_finite()
                    or filled_decimal < 0
                    or filled_decimal != filled_decimal.to_integral_value()
                    or filled_decimal > intent.quantity
                ):
                    raise ValueError("invalid cumulative fill quantity")
                filled_quantity = int(filled_decimal)
                average_fill_price = (
                    Decimal(str(avg_price)) if filled_quantity > 0 else Decimal("0")
                )
                if not average_fill_price.is_finite() or (
                    filled_quantity > 0 and average_fill_price <= 0
                ):
                    raise ValueError("invalid average fill price")
            except (ArithmeticError, TypeError, ValueError):
                self._mark_inconsistent(intent, broker_order_id)
                return

            current = self._reports.get(broker_order_id)
            if current is not None and filled_quantity < current.filled_quantity:
                # IBKR can replay an older cumulative orderStatus callback after
                # a newer execDetails callback.  It is evidence of reordering,
                # never permission to move the canonical report backwards.
                self._mark_inconsistent(intent, broker_order_id)
                return

            fills = self._fills_for_order(broker_order_id)
            fill_quantity = max(
                (fill.cumulative_quantity for fill in fills),
                default=0,
            )
            aggregate_quantity = max(
                filled_quantity,
                fill_quantity,
                current.filled_quantity if current is not None else 0,
            )
            if aggregate_quantity > intent.quantity:
                self._mark_inconsistent(intent, broker_order_id)
                return

            summed_fill_quantity = sum(fill.quantity for fill in fills)
            if summed_fill_quantity > intent.quantity:
                self._mark_inconsistent(intent, broker_order_id)
                return
            try:
                if fills and summed_fill_quantity == aggregate_quantity:
                    aggregate_price = sum(
                        (fill.price * fill.quantity for fill in fills),
                        Decimal("0"),
                    ) / Decimal(summed_fill_quantity)
                elif filled_quantity == aggregate_quantity and average_fill_price > 0:
                    aggregate_price = average_fill_price
                elif current is not None:
                    aggregate_price = current.average_fill_price
                else:
                    aggregate_price = Decimal("0")
                # A weighted average is a division: it is the one place in this
                # callback where the contract's precision can be exceeded.
                aggregate_price = money(aggregate_price)
            except ArithmeticError:
                self._mark_inconsistent(intent, broker_order_id)
                return
            if aggregate_quantity > 0 and aggregate_price <= 0:
                self._mark_inconsistent(intent, broker_order_id)
                return

            status = self._status_for_callback(
                raw_status,
                aggregate_quantity,
                intent.quantity,
                current.status if current is not None else None,
            )
            if status is None:
                self._mark_inconsistent(intent, broker_order_id)
                return
            fill_fees = sum(
                (fill.commission for fill in fills if fill.commission_final),
                Decimal("0"),
            )
            fees = max(
                fill_fees,
                current.fees if current is not None else Decimal("0"),
            )
            fill_count = max(
                len(fills),
                current.fill_count if current is not None else 0,
            )
            observed_pending = bool(fills) and any(not fill.commission_final for fill in fills)
            if current is None or len(fills) > current.fill_count:
                pending_commission = observed_pending
            elif len(fills) < current.fill_count:
                pending_commission = current.pending_commission
            else:
                pending_commission = current.pending_commission and observed_pending
            try:
                candidate = ExecutionReport(
                    order_id=intent.order_id,
                    idempotency_key=intent.idempotency_key,
                    status=status,
                    filled_quantity=aggregate_quantity,
                    average_fill_price=aggregate_price,
                    fees=fees,
                    slippage_bps=self._slippage_bps(intent, aggregate_price),
                    broker_order_id=str(broker_order_id),
                    message=raw_status,
                    occurred_at=self._clock(),
                    fill_count=fill_count,
                    pending_commission=pending_commission,
                    update_sequence=(current.update_sequence + 1 if current is not None else 1),
                )
            except (ArithmeticError, ValidationError, ValueError) as exc:
                self._record_callback_failure("orderStatus", exc)
                self._mark_inconsistent(intent, broker_order_id)
                return
            if current is not None and self._same_report_fact(current, candidate):
                return
            self._reports[broker_order_id] = candidate

    def _fills_for_order(self, broker_order_id: int) -> tuple[ExecutionFill, ...]:
        identifiers = self._fill_ids_by_broker_order.get(broker_order_id, set())
        return tuple(
            sorted(
                (self._fills_by_execution_id[identifier] for identifier in identifiers),
                key=lambda fill: (fill.occurred_at, fill.execution_id),
            )
        )

    def _record_callback_failure(self, source: str, error: Exception) -> None:
        """Latch an adapter fault so no later operation trusts this session.

        A callback that cannot turn a broker message into a valid fact leaves
        the local order view incomplete.  Continuing would present a stale
        report as current, so the fault is latched and both submission and
        reconciliation refuse until the session is rebuilt.  The latch
        deliberately replaces the process-level signal that a raised callback
        used to give: catching the error must not be cheaper than crashing was.

        Cancelling is not gated by the latch itself, but it is not therefore
        available: the CANCEL readiness profile requires ``reconciled``, and the
        next reconciliation attempt clears that flag before it fails on the
        latch.  A cancel is possible until that attempt, and not afterwards.
        Whether a purely exposure-reducing cancel should outlive its session is
        a decision for ADR-023, not something this method may quietly assume.
        """

        with self._lock:
            self._callback_failures.append((source, error.__class__.__name__))

    @property
    def callback_failures(self) -> tuple[tuple[str, str], ...]:
        """Latched adapter faults; a non-empty result blocks every order path."""

        with self._lock:
            return tuple(self._callback_failures)

    @property
    def deferred_inconsistencies(self) -> frozenset[str]:
        """Callbacks discarded outside a reconciliation window."""

        with self._lock:
            return frozenset(self._deferred_inconsistencies)

    def _discard_remote_callback(self, remote_token: str) -> None:
        """Record one discarded remote callback, inside or outside a reconciliation.

        Mirrors :meth:`_mark_inconsistent` for the callbacks keyed by a remote
        token rather than by a known intent: a discard during normal trading is
        remembered and folded into the next reconciliation instead of
        disappearing together with the callback that caused it.
        """

        if self._reconciliation_account is not None:
            self._unknown_remote_order_ids.add(remote_token)
            return
        self._deferred_inconsistencies.add(remote_token)

    def _mark_inconsistent(self, intent: OrderIntent, broker_order_id: int) -> None:
        """Record one discarded callback, inside or outside a reconciliation.

        Outside an active reconciliation this used to be a silent no-op, so a
        contradictory callback during normal trading vanished.  The discard is
        now remembered and folded into the next reconciliation, which is the
        only place that can decide what it means.
        """

        token = f"inconsistent:{broker_order_id}"
        if self._reconciliation_account == intent.account_id:
            self._unknown_remote_order_ids.add(token)
            return
        self._deferred_inconsistencies.add(token)

    @classmethod
    def _status_for_callback(
        cls,
        raw_status: str,
        filled_quantity: int,
        order_quantity: int,
        current: ExecutionStatus | None,
    ) -> ExecutionStatus | None:
        if filled_quantity >= order_quantity:
            return ExecutionStatus.FILLED
        if current is ExecutionStatus.FILLED:
            return current
        if raw_status in {"Cancelled", "ApiCancelled"}:
            return ExecutionStatus.CANCELLED
        if current is ExecutionStatus.CANCELLED:
            return current
        if filled_quantity > 0:
            return ExecutionStatus.PARTIALLY_FILLED
        if raw_status == "Inactive" or current is ExecutionStatus.REJECTED:
            return ExecutionStatus.REJECTED
        if raw_status in cls._SUBMITTED_STATUSES:
            return ExecutionStatus.SUBMITTED
        return None

    @staticmethod
    def _slippage_bps(intent: OrderIntent, average_fill_price: Decimal) -> float:
        if average_fill_price <= 0:
            return 0.0
        if intent.side in {OrderSide.BUY, OrderSide.BUY_TO_COVER}:
            slippage = (
                (average_fill_price - intent.limit_price) / intent.limit_price * Decimal("10000")
            )
        else:
            slippage = (
                (intent.limit_price - average_fill_price) / intent.limit_price * Decimal("10000")
            )
        return float(slippage)

    @staticmethod
    def _same_report_fact(current: ExecutionReport, candidate: ExecutionReport) -> bool:
        excluded = {"occurred_at", "update_sequence"}
        return current.model_dump(exclude=excluded) == candidate.model_dump(exclude=excluded)

    def _on_remote_order(
        self,
        broker_order_id: int,
        contract: Any,
        order: Any,
        order_state: Any,
        *,
        completed: bool,
    ) -> None:
        """Validate and bind one open/completed callback to a known intent."""

        account_id = str(getattr(order, "account", "")).strip()
        order_ref = str(getattr(order, "orderRef", "")).strip()
        with self._lock:
            target_account = self._reconciliation_account
            if target_account is None or account_id != target_account:
                return
            intent = self._intents.get(broker_order_id)
            if intent is None and order_ref:
                intent = self._known_intents_by_key.get(order_ref)
            remote_token = str(broker_order_id) if broker_order_id >= 0 else f"ref:{order_ref}"
            if intent is None or not self._remote_order_matches(
                intent,
                contract,
                order,
                order_ref,
            ):
                self._unknown_remote_order_ids.add(remote_token)
                return
            if broker_order_id < 0:
                self._unknown_remote_order_ids.add(remote_token)
                return
            self._intents[broker_order_id] = intent
            self._remote_seen_order_ids.add(intent.order_id)
            if self._next_order_id is not None:
                self._next_order_id = max(self._next_order_id, broker_order_id + 1)
            current = self._reports.get(broker_order_id)

        filled = getattr(order, "filledQuantity", None)
        if filled is None:
            filled = current.filled_quantity if current is not None else 0
        avg_price = getattr(order_state, "avgFillPrice", None)
        if avg_price is None or Decimal(str(avg_price)) <= 0:
            avg_price = (
                current.average_fill_price
                if current is not None and current.average_fill_price > 0
                else 0
            )
        raw_status = str(
            getattr(order_state, "completedStatus", "")
            if completed
            else getattr(order_state, "status", "")
        ).strip()
        if not raw_status:
            raw_status = "Cancelled" if completed else "Submitted"
        self._on_order_status(broker_order_id, raw_status, filled, float(avg_price))

    @staticmethod
    def _remote_order_matches(
        intent: OrderIntent,
        contract: Any,
        order: Any,
        order_ref: str,
    ) -> bool:
        try:
            quantity = Decimal(str(order.totalQuantity))
            limit_price = Decimal(str(order.lmtPrice))
        except Exception:
            return False
        expected_action = (
            "BUY" if intent.side in {OrderSide.BUY, OrderSide.BUY_TO_COVER} else "SELL"
        )
        return all(
            (
                order_ref == intent.idempotency_key,
                str(getattr(contract, "symbol", "")).strip().upper()
                == intent.symbol.strip().upper(),
                str(getattr(order, "action", "")).strip().upper() == expected_action,
                quantity == intent.quantity,
                str(getattr(order, "orderType", "")).strip().upper() == "LMT",
                limit_price == intent.limit_price,
                str(getattr(order, "tif", "")).strip().upper() == intent.time_in_force,
                str(getattr(order, "account", "")).strip() == intent.account_id,
            )
        )

    def _on_open_order_end(self) -> None:
        self._open_orders_event.set()

    def _on_exec_details(self, req_id: int, contract: Any, execution: Any) -> None:
        del req_id
        account_id = str(getattr(execution, "acctNumber", "")).strip()
        broker_order_id = int(getattr(execution, "orderId", -1))
        order_ref = str(getattr(execution, "orderRef", "")).strip()
        with self._lock:
            target_account = self._reconciliation_account
            if target_account is not None and target_account != account_id:
                return
            intent = self._intents.get(broker_order_id)
            if intent is None and order_ref:
                intent = self._known_intents_by_key.get(order_ref)
            remote_token = (
                str(broker_order_id) if broker_order_id >= 0 else f"execution:{order_ref}"
            )
            if (
                intent is None
                or broker_order_id < 0
                or account_id != intent.account_id
                or str(getattr(contract, "symbol", "")).strip().upper()
                != intent.symbol.strip().upper()
            ):
                self._discard_remote_callback(remote_token)
                return
            self._intents[broker_order_id] = intent
            if target_account is not None:
                self._remote_seen_order_ids.add(intent.order_id)
            if self._next_order_id is not None:
                self._next_order_id = max(self._next_order_id, broker_order_id + 1)

            try:
                execution_id = str(execution.execId).strip()
                quantity_decimal = Decimal(str(execution.shares))
                price = Decimal(str(execution.price))
                cumulative_decimal = Decimal(str(execution.cumQty))
                average = Decimal(str(execution.avgPrice))
                values = (quantity_decimal, price, cumulative_decimal, average)
                if (
                    not execution_id
                    or any(not value.is_finite() for value in values)
                    or quantity_decimal <= 0
                    or quantity_decimal != quantity_decimal.to_integral_value()
                    or price <= 0
                    or cumulative_decimal <= 0
                    or cumulative_decimal != cumulative_decimal.to_integral_value()
                    or average <= 0
                ):
                    raise ValueError("invalid execution details")
                quantity = int(quantity_decimal)
                cumulative = int(cumulative_decimal)
                if cumulative < quantity or cumulative > intent.quantity:
                    raise ValueError("invalid cumulative execution quantity")
                price = money(price)
                average = money(average)
                if price <= 0 or average <= 0:
                    raise ValueError("execution price rounds below the contract")
            except (ArithmeticError, AttributeError, TypeError, ValueError):
                self._discard_remote_callback(remote_token)
                return

            existing = self._fills_by_execution_id.get(execution_id)
            pending_commission = self._pending_commissions.get(execution_id)
            try:
                fill = ExecutionFill(
                    order_id=intent.order_id,
                    execution_id=execution_id,
                    broker_order_id=str(broker_order_id),
                    symbol=intent.symbol,
                    side=intent.side,
                    quantity=quantity,
                    price=price,
                    cumulative_quantity=cumulative,
                    commission=(pending_commission or Decimal("0")),
                    commission_final=pending_commission is not None,
                    occurred_at=(existing.occurred_at if existing is not None else self._clock()),
                )
            except (ArithmeticError, ValidationError, ValueError) as exc:
                self._record_callback_failure("execDetails", exc)
                self._mark_inconsistent(intent, broker_order_id)
                return
            if existing is not None:
                same_fill = fill.model_copy(
                    update={
                        "commission": existing.commission,
                        "commission_final": existing.commission_final,
                    }
                )
                if same_fill != existing:
                    self._discard_remote_callback(remote_token)
                    return
                if pending_commission is None:
                    fill = existing
                elif existing.commission_final:
                    if existing.commission != pending_commission:
                        self._discard_remote_callback(remote_token)
                        return
                    fill = existing
            # Only now is the fill actually taken over.  Consuming the pending
            # commission any earlier would lose it on every path that returns
            # above, and a commission is never recoverable from a later replay.
            self._pending_commissions.pop(execution_id, None)
            self._fills_by_execution_id[execution_id] = fill
            self._fill_ids_by_broker_order.setdefault(broker_order_id, set()).add(execution_id)

        raw_status = "Filled" if cumulative >= intent.quantity else "Submitted"
        self._on_order_status(
            broker_order_id,
            raw_status,
            cumulative,
            float(average),
        )

    def _on_commission_report(self, commission_report: Any) -> None:
        """Finalize one fill commission, including commission-before-fill ordering."""

        try:
            execution_id = str(commission_report.execId).strip()
            commission = Decimal(str(commission_report.commission))
            if not execution_id or not commission.is_finite() or commission < 0:
                raise ValueError("invalid commission report")
            commission = money(commission)
        except (ArithmeticError, AttributeError, TypeError, ValueError):
            return

        with self._lock:
            existing = self._fills_by_execution_id.get(execution_id)
            if existing is None:
                pending = self._pending_commissions.get(execution_id)
                if pending is None or pending == commission:
                    self._pending_commissions[execution_id] = commission
                return
            if existing.commission_final:
                if existing.commission != commission:
                    try:
                        broker_order_id = int(existing.broker_order_id or "-1")
                        intent = self._intents[broker_order_id]
                    except (KeyError, ValueError):
                        return
                    self._mark_inconsistent(intent, broker_order_id)
                return
            # ``model_copy`` does not validate.  A commission is the only value a
            # stored fill may still gain, so it is written through the contract.
            try:
                finalized = ExecutionFill.model_validate(
                    existing.model_dump() | {"commission": commission, "commission_final": True}
                )
            except (ArithmeticError, ValidationError, ValueError) as exc:
                self._record_callback_failure("commissionReport", exc)
                return
            self._fills_by_execution_id[execution_id] = finalized
            try:
                broker_order_id = int(finalized.broker_order_id or "-1")
            except ValueError:
                return
            current = self._reports.get(broker_order_id)
            if current is None:
                return
            raw_status = current.message or (
                "Filled" if current.status is ExecutionStatus.FILLED else "Submitted"
            )
            filled_quantity = current.filled_quantity
            average_fill_price = current.average_fill_price

        self._on_order_status(
            broker_order_id,
            raw_status,
            filled_quantity,
            float(average_fill_price),
        )

    def _on_exec_details_end(self, req_id: int) -> None:
        with self._lock:
            event = self._execution_events.get(req_id)
        if event is not None:
            event.set()

    def _on_completed_orders_end(self) -> None:
        self._completed_orders_event.set()

    def _on_account_value(self, key: str, value: str, currency: str, account_name: str) -> None:
        if currency not in {"BASE", ""}:
            return
        if key not in {
            "NetLiquidation",
            "TotalCashValue",
            "CashBalance",
            "RealizedPnL",
            "UnrealizedPnL",
        }:
            return
        try:
            parsed = Decimal(value)
        except Exception:
            return
        with self._lock:
            self._account_values.setdefault(account_name, {})[key] = parsed

    def _on_portfolio(
        self,
        contract: Any,
        raw_position: Any,
        market_price: float,
        average_cost: float,
        account_name: str,
    ) -> None:
        try:
            position_decimal = Decimal(str(raw_position))
            quantity = int(abs(position_decimal))
            if position_decimal == 0 or Decimal(quantity) != abs(position_decimal):
                return
            parsed_market_price = money(Decimal(str(market_price)))
            parsed_average_cost = money(abs(Decimal(str(average_cost))))
            if parsed_market_price <= 0 or parsed_average_cost <= 0:
                return
            symbol = str(contract.symbol)
            position = Position(
                symbol=symbol,
                direction=(Direction.LONG if position_decimal > 0 else Direction.SHORT),
                quantity=quantity,
                market_price=parsed_market_price,
                average_price=parsed_average_cost,
            )
        except (ArithmeticError, AttributeError, TypeError, ValueError) as exc:
            # Dropping a position here would surface later as a mismatch between
            # broker and local state, naming the wrong cause.  The fault is
            # latched where it happens instead.
            self._record_callback_failure("updatePortfolio", exc)
            return
        with self._lock:
            self._positions.setdefault(account_name, {})[symbol] = position

    def _on_account_download_end(self, account_name: str) -> None:
        with self._lock:
            event = self._download_events.get(account_name)
        if event is not None:
            event.set()

    def _on_error(self, req_id: int, code: int, message: str) -> None:
        # Store only a bounded provider message; never include advanced reject JSON.
        with self._lock:
            self._last_error = (req_id, code, message[:500])
            hook = self._bar_hook
        if hook is not None:
            hook.on_error(req_id, code, message)


class IBKRBrokerAdapter:
    """Idempotent, paper-only broker facade over an ``IBKRBackend``."""

    def __init__(
        self,
        *,
        account_id: str,
        paper_account_allowlist: Iterable[str],
        environment: str = "paper",
        backend: IBKRBackend | None = None,
        connection: IBKRConnectionConfig | None = None,
        require_live_market_data: bool = True,
        clock: Clock = utc_now,
    ) -> None:
        self.account_id = account_id
        self.environment = environment
        self._guard = PaperAccountGuard(paper_account_allowlist)
        # Reject live configuration before constructing or touching any transport.
        self._assert_du_paper_account(account_id)
        self._guard.assert_paper(account_id, environment)
        self._backend = backend
        self._connection = connection or IBKRConnectionConfig()
        self._require_live_market_data = require_live_market_data
        self._clock = clock
        self._state = OrderStateMachine()
        self._reconciled = False
        self._restored_open_orders: set[str] = set()
        self._recovery_unconfirmed: set[str] = set()
        self._cancel_requested: set[str] = set()
        self._last_portfolio: PortfolioState | None = None

    @staticmethod
    def _assert_du_paper_account(account_id: str) -> None:
        if not is_paper_account_id(account_id):
            raise PaperAccountViolation(
                "IBKR execution requires a paper account id matching 'DU<digits>'"
            )

    def connect(self) -> None:
        self._assert_du_paper_account(self.account_id)
        self._guard.assert_paper(self.account_id, self.environment)
        if self._backend is None:
            self._backend = NativeIBAPIBackend(self._connection, clock=self._clock)
        self._backend.connect()

    def disconnect(self) -> None:
        if self._backend is not None:
            self._backend.disconnect()
        self._reconciled = False

    def readiness(self, profile: ReadinessProfile = ReadinessProfile.SUBMIT) -> BrokerReadiness:
        profile = ReadinessProfile(profile)
        backend = self._backend
        dependency_ready = backend is not None or ibapi_available()
        connected = backend is not None and backend.is_connected()
        accounts = backend.account_ids() if connected else ()
        account_present = self.account_id in accounts
        order_channel = connected and backend is not None and backend.ready_for_orders()
        order_scope_authoritative = (
            connected and backend is not None and backend.order_scope_authoritative()
        )
        market_data = (
            connected and backend is not None and backend.market_data_live()
            if self._require_live_market_data
            else True
        )
        du_paper_id = is_paper_account_id(self.account_id)
        checks = (
            ReadinessCheck(
                "du_paper_account_id",
                du_paper_id,
                "IBKR paper account id must match 'DU<digits>'",
            ),
            ReadinessCheck(
                "ibapi_dependency",
                dependency_ready,
                "optional dependency 'ibapi' is unavailable and no backend was injected",
            ),
            ReadinessCheck("connected", connected, "IB Gateway/TWS is disconnected"),
            ReadinessCheck(
                "paper_account_present",
                account_present,
                "allowlisted paper account is absent from the IBKR session",
            ),
            ReadinessCheck("order_channel", order_channel, "IBKR has no valid next order id"),
            ReadinessCheck(
                "account_order_scope_authoritative",
                order_scope_authoritative,
                "manual/API orders are not fully visible for this IBKR session",
            ),
            ReadinessCheck("market_data_live", market_data, "live market data is not confirmed"),
            ReadinessCheck("reconciled", self._reconciled, "broker state has not been reconciled"),
            ReadinessCheck(
                "recovery_orders_terminal",
                not self._restored_open_orders,
                (
                    f"{len(self._restored_open_orders)} restored order(s) remain open"
                    if self._restored_open_orders
                    else ""
                ),
            ),
        )
        common = {
            "du_paper_account_id",
            "ibapi_dependency",
            "connected",
            "paper_account_present",
            "account_order_scope_authoritative",
        }
        required = {
            ReadinessProfile.RECONCILE: common,
            ReadinessProfile.CANCEL: common | {"reconciled"},
            ReadinessProfile.EXIT: common | {"order_channel", "market_data_live", "reconciled"},
            ReadinessProfile.SUBMIT: common
            | {
                "order_channel",
                "market_data_live",
                "reconciled",
                "recovery_orders_terminal",
            },
        }[profile]
        selected = tuple(check for check in checks if check.name in required)
        return BrokerReadiness(self.account_id, self._clock(), selected)

    def _require_backend(self) -> IBKRBackend:
        if self._backend is None:
            raise IBKRTransportError("IBKR backend is not connected")
        return self._backend

    async def restore_from_storage(
        self,
        store: IBKRRecoveryStore,
        *,
        max_orders: int = 1_000,
    ) -> tuple[ExecutionReport, ...]:
        """Restore persisted open orders locally before broker reconciliation.

        This method never calls the IBKR backend. A missing execution report is
        restored as an unknown-outcome ``PENDING`` order and must be positively
        confirmed by a later authoritative reconciliation. The extra row makes
        a truncated recovery set fail closed instead of silently omitting work.
        """

        if max_orders <= 0:
            raise ValueError("max_orders must be greater than zero")
        self._assert_du_paper_account(self.account_id)
        self._guard.assert_paper(self.account_id, self.environment)
        self._reconciled = False
        open_intents = await store.list_orders_for_reconciliation(limit=max_orders + 1)
        intents_by_id = {intent.order_id: intent for intent in open_intents}
        all_intents = await store.list_order_intents(limit=max_orders + 1)
        for persisted_intent in all_intents:
            intents_by_id.setdefault(persisted_intent.order_id, persisted_intent)
        intents = tuple(
            sorted(
                (intent for intent in intents_by_id.values() if intent.submission_mode == "paper"),
                key=lambda item: (item.created_at, item.order_id),
            )
        )
        if len(open_intents) > max_orders or len(intents_by_id) > max_orders:
            raise IBKRRecoveryIncomplete(
                f"more than {max_orders} persisted orders require reconciliation"
            )
        for persisted_intent in intents:
            self._assert_du_paper_account(persisted_intent.account_id)
            self._guard.assert_paper(persisted_intent.account_id, persisted_intent.environment)
            if persisted_intent.account_id != self.account_id:
                raise PaperAccountViolation("persisted intent account differs from broker account")

        restored: list[ExecutionReport] = []
        terminal = {
            ExecutionStatus.FILLED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        }
        for persisted_intent in intents:
            latest_report = await store.get_execution_report(persisted_intent.order_id)
            canonical = self._state.restore(persisted_intent, latest_report)
            restored.append(canonical)
            if canonical.status in terminal:
                self._restored_open_orders.discard(canonical.order_id)
                self._recovery_unconfirmed.discard(canonical.order_id)
            else:
                self._restored_open_orders.add(canonical.order_id)
                self._recovery_unconfirmed.add(canonical.order_id)
        return tuple(restored)

    def submit(self, intent: OrderIntent) -> ExecutionReport:
        _assert_paper_submission(intent)
        self._assert_du_paper_account(intent.account_id)
        self._guard.assert_paper(intent.account_id, intent.environment)
        if intent.account_id != self.account_id:
            raise PaperAccountViolation("intent account differs from broker account")
        profile = (
            ReadinessProfile.SUBMIT
            if intent.side in {OrderSide.BUY, OrderSide.SELL_SHORT}
            else ReadinessProfile.EXIT
        )
        self.readiness(profile).require()
        pending, created = self._state.begin(intent, self._clock())
        if not created:
            return pending
        backend = self._require_backend()
        try:
            broker_order_id = backend.submit_order(intent)
        except Exception as exc:
            # Keep PENDING: the socket may have failed after IBKR accepted the order.
            # A retry with the same key will not submit again; reconciliation decides.
            raise IBKRTransportError(
                f"IBKR submit outcome is unknown: {exc.__class__.__name__}"
            ) from exc
        return self._state.transition(
            pending.model_copy(
                update={
                    "status": ExecutionStatus.SUBMITTED,
                    "broker_order_id": broker_order_id,
                    "occurred_at": self._clock(),
                }
            )
        )

    def submit_order(self, intent: OrderIntent) -> ExecutionReport:
        return self.submit(intent)

    def cancel(self, order_id: str) -> ExecutionReport:
        self._assert_du_paper_account(self.account_id)
        self._guard.assert_paper(self.account_id, self.environment)
        self.readiness(ReadinessProfile.CANCEL).require()
        current = self._state.current(order_id)
        _assert_paper_submission(self._state.intent(order_id))
        if current.status in {
            ExecutionStatus.FILLED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        }:
            return current
        if current.broker_order_id is None:
            raise IBKRTransportError(
                "order has no broker id; reconcile before attempting cancellation"
            )
        if order_id not in self._cancel_requested:
            try:
                self._require_backend().cancel_order(current.broker_order_id)
            except Exception as exc:
                raise IBKRTransportError(
                    f"IBKR cancel outcome is unknown: {exc.__class__.__name__}"
                ) from exc
            self._cancel_requested.add(order_id)
        return self._state.transition(
            current.model_copy(update={"message": "cancel requested", "occurred_at": self._clock()})
        )

    def cancel_order(self, order_id: str) -> ExecutionReport:
        return self.cancel(order_id)

    def reconcile(self) -> ReconciliationResult:
        self._assert_du_paper_account(self.account_id)
        self._guard.assert_paper(self.account_id, self.environment)
        self._reconciled = False
        backend = self._require_backend()
        if not backend.is_connected():
            raise IBKRTransportError("IB Gateway/TWS is disconnected")
        if self.account_id not in backend.account_ids():
            raise PaperAccountViolation("allowlisted paper account is absent from the IBKR session")
        if not backend.order_scope_authoritative():
            raise IBKRRecoveryIncomplete(
                "IBKR session cannot authoritatively observe manual and API orders"
            )
        self.readiness(ReadinessProfile.RECONCILE).require()
        current_reports = self._state.reports()
        known_orders = tuple(
            (self._state.intent(report.order_id), report) for report in current_reports
        )
        remote_snapshot = backend.reconcile_orders(self.account_id, known_orders)
        if not remote_snapshot.complete:
            raise IBKRRecoveryIncomplete("IBKR order snapshot is incomplete")
        if remote_snapshot.unknown_remote_order_ids:
            raise IBKRRecoveryIncomplete("IBKR returned unknown or manual remote order activity")
        remote_reports: list[ExecutionReport] = []
        for raw_report in remote_snapshot.reports:
            try:
                persisted_intent = self._state.intent(raw_report.order_id)
                report = self._normalize_remote_report(
                    persisted_intent,
                    raw_report,
                )
                if report.order_id in self._recovery_unconfirmed:
                    self._state.restore(persisted_intent, report)
                else:
                    self._state.transition(report)
            except UnknownOrder as exc:
                raise IBKRTransportError(
                    "IBKR returned an execution not owned by this adapter"
                ) from exc
            remote_reports.append(report)
        remote_order_ids = {report.order_id for report in remote_reports}
        terminal = {
            ExecutionStatus.FILLED,
            ExecutionStatus.CANCELLED,
            ExecutionStatus.REJECTED,
        }
        for order_id in tuple(self._restored_open_orders):
            if self._state.current(order_id).status in terminal:
                self._restored_open_orders.discard(order_id)
                self._recovery_unconfirmed.discard(order_id)
        unresolved = self._recovery_unconfirmed - remote_order_ids
        if unresolved:
            raise IBKRRecoveryIncomplete(
                f"IBKR did not confirm {len(unresolved)} restored open order(s)"
            )
        local_open_orders = {
            report.order_id for report in self._state.reports() if report.status not in terminal
        }
        missing_local_orders = local_open_orders - remote_snapshot.seen_order_ids
        if missing_local_orders:
            raise IBKRRecoveryIncomplete(
                f"IBKR did not confirm {len(missing_local_orders)} local open order(s)"
            )
        portfolio = backend.portfolio_state(self.account_id)
        if not portfolio.broker_connected or not portfolio.reconciled:
            raise IBKRTransportError("IBKR portfolio snapshot is not reconciled")
        self._validate_positions(portfolio)
        current_reports = self._state.reports()
        current_intents = tuple(
            self._state.intent(report.order_id)
            for report in current_reports
            if self._state.intent(report.order_id).submission_mode == "paper"
        )
        paper_order_ids = {intent.order_id for intent in current_intents}
        paper_reports = tuple(
            report for report in current_reports if report.order_id in paper_order_ids
        )
        portfolio = portfolio.model_copy(
            update={
                "pending_orders": pending_entry_exposures(
                    current_intents,
                    paper_reports,
                )
            }
        )
        self._recovery_unconfirmed.difference_update(remote_order_ids)
        self._last_portfolio = portfolio
        self._reconciled = True
        now = self._clock()
        return ReconciliationResult(
            account_id=self.account_id,
            reconciled_at=now,
            executions=self._state.reports(),
            portfolio=portfolio,
            fills=remote_snapshot.fills,
        )

    @staticmethod
    def _normalize_remote_report(
        intent: OrderIntent,
        report: ExecutionReport,
    ) -> ExecutionReport:
        if (
            report.filled_quantity >= intent.quantity
            and report.status is not ExecutionStatus.FILLED
        ):
            return report.model_copy(
                update={
                    "status": ExecutionStatus.FILLED,
                    "filled_quantity": intent.quantity,
                }
            )
        return report

    def _validate_positions(self, portfolio: PortfolioState) -> None:
        expected: dict[str, int] = {}
        for report in self._state.reports():
            if report.filled_quantity <= 0:
                continue
            intent = self._state.intent(report.order_id)
            if intent.submission_mode != "paper":
                continue
            sign = 1 if intent.side in {OrderSide.BUY, OrderSide.BUY_TO_COVER} else -1
            symbol = intent.symbol.strip().upper()
            expected[symbol] = expected.get(symbol, 0) + sign * report.filled_quantity
        expected = {symbol: quantity for symbol, quantity in expected.items() if quantity}
        actual = {
            position.symbol.strip().upper(): (
                position.quantity if position.direction is Direction.LONG else -position.quantity
            )
            for position in portfolio.positions
        }
        if actual != expected:
            raise IBKRRecoveryIncomplete(
                "IBKR portfolio positions differ from locally owned executions"
            )

    @property
    def reports(self) -> tuple[ExecutionReport, ...]:
        return self._state.reports()


# Concise public alias for callers that do not need to name the adapter pattern.
IBKRBroker = IBKRBrokerAdapter
