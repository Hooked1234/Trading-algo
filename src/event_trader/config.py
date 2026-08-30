"""Application configuration with a hard paper-only invariant."""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .domain import is_paper_account_id

PLACEHOLDER_ACCOUNT_ID = "DU_NOT_CONFIGURED"
"""Inert default: deliberately not a valid IBKR paper account id."""


class Settings(BaseSettings):
    """Environment-backed configuration.

    ``environment`` intentionally has no live value.  A future live release must
    introduce a distinct configuration model instead of toggling a boolean.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="TRADING_",
        extra="ignore",
        case_sensitive=False,
    )

    environment: Literal["paper"] = "paper"
    paper_account_id: str = PLACEHOLDER_ACCOUNT_ID
    allowed_paper_accounts: tuple[str, ...] = (PLACEHOLDER_ACCOUNT_ID,)

    sec_user_agent: str = "event-trader local-contact@example.invalid"
    sec_poll_seconds: float = Field(default=10.0, ge=5.0)
    sec_max_requests_per_second: float = Field(default=2.0, gt=0, le=2.0)

    state_db_path: Path = Path("data/state/trading.sqlite")
    raw_data_dir: Path = Path("data/raw/sec")
    market_data_dir: Path = Path("data/market")
    report_dir: Path = Path("data/reports")
    backfill_state_path: Path = Path("data/state/backfill.sqlite")
    promotion_artifact_path: Path = Path("data/state/promotion.json")
    historical_eligibility_path: Path = Path("data/reference/historical_eligibility.csv")

    alpaca_api_key: SecretStr | None = None
    alpaca_api_secret: SecretStr | None = None
    alpaca_data_url: str = "https://data.alpaca.markets"

    hermes_url: str = "http://127.0.0.1:8642/v1"
    hermes_api_token: SecretStr | None = None
    hermes_timeout_seconds: float = Field(default=30.0, gt=0, le=30.0)
    # Bounded by the insight provider's own hard cap; a larger value would
    # make the provider unconstructible.
    hermes_max_input_chars: int = Field(default=40_000, ge=1_000, le=100_000)
    model_daily_budget_eur: float = Field(default=1.0, ge=0, le=5)
    model_monthly_budget_eur: float = Field(default=30.0, ge=0, le=40)

    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = Field(default=7497, ge=1, le=65_535)
    # Client 0 is the only id whose session observes manual and API orders in
    # one authoritative scope; paper mode refuses anything else outright.
    ibkr_client_id: int = Field(default=0, ge=0)

    strategy_nav: float = Field(default=100_000, gt=0)
    risk_per_trade: float = Field(default=0.005, gt=0, le=0.005)
    max_positions: int = Field(default=5, ge=1, le=5)
    max_symbol_notional: float = Field(default=0.15, gt=0, le=0.15)
    max_gross_exposure: float = Field(default=0.75, gt=0, le=0.75)
    max_abs_net_exposure: float = Field(default=0.40, gt=0, le=0.40)
    max_daily_loss: float = Field(default=0.015, gt=0, le=0.015)
    max_drawdown: float = Field(default=0.05, gt=0, le=0.05)

    @field_validator("allowed_paper_accounts")
    @classmethod
    def accounts_must_be_paper(cls, accounts: tuple[str, ...]) -> tuple[str, ...]:
        if not accounts:
            raise ValueError("at least one paper account must be allowlisted")
        invalid = [
            account
            for account in accounts
            if account.upper() != PLACEHOLDER_ACCOUNT_ID and not is_paper_account_id(account)
        ]
        if invalid:
            raise ValueError("IBKR paper account ids must start with DU")
        return tuple(account.upper() for account in accounts)

    @field_validator("paper_account_id")
    @classmethod
    def selected_account_is_normalized(cls, account: str) -> str:
        return account.strip().upper()

    @field_validator("alpaca_data_url")
    @classmethod
    def alpaca_endpoint_is_official(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "data.alpaca.markets"
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("Alpaca credentials may only be sent to data.alpaca.markets")
        return value.rstrip("/")

    @field_validator("hermes_url")
    @classmethod
    def hermes_endpoint_is_loopback(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or parsed.path.rstrip("/") != "/v1"
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError("Hermes must use a loopback HTTP endpoint ending in /v1")
        return value.rstrip("/")

    @field_validator("ibkr_host")
    @classmethod
    def ibkr_gateway_is_local(cls, value: str) -> str:
        if value.lower() not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("IBKR Gateway must be local in version 1")
        return value

    @model_validator(mode="after")
    def selected_account_is_allowlisted(self) -> Settings:
        account = self.paper_account_id.upper()
        if account != PLACEHOLDER_ACCOUNT_ID and not is_paper_account_id(account):
            raise ValueError("selected account is not an IBKR paper account")
        if account not in self.allowed_paper_accounts:
            raise ValueError("selected paper account is not allowlisted")
        return self

    @property
    def placeholder_credentials(self) -> bool:
        return self.paper_account_id == PLACEHOLDER_ACCOUNT_ID
