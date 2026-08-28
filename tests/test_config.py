import pytest
from pydantic import ValidationError

from event_trader.config import Settings


def test_default_configuration_is_inert_paper() -> None:
    settings = Settings(_env_file=None)
    assert settings.environment == "paper"
    assert settings.placeholder_credentials


def test_live_environment_is_not_a_valid_configuration() -> None:
    with pytest.raises(ValidationError):
        Settings(environment="live", _env_file=None)


def test_non_paper_account_is_rejected() -> None:
    with pytest.raises(ValidationError, match="paper account"):
        Settings(
            paper_account_id="U12345",
            allowed_paper_accounts=("U12345",),
            _env_file=None,
        )


def test_unlisted_paper_account_is_rejected() -> None:
    with pytest.raises(ValidationError, match="not allowlisted"):
        Settings(
            paper_account_id="DU222",
            allowed_paper_accounts=("DU111",),
            _env_file=None,
        )


def test_paper_account_ids_are_normalized_consistently() -> None:
    settings = Settings(
        paper_account_id=" du123 ",
        allowed_paper_accounts=("DU123",),
        _env_file=None,
    )
    assert settings.paper_account_id == "DU123"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("hermes_url", "https://remote.example/v1"),
        ("alpaca_data_url", "https://lookalike.example"),
        ("ibkr_host", "gateway.example"),
    ],
)
def test_sensitive_provider_endpoints_are_pinned(field: str, value: str) -> None:
    with pytest.raises(ValidationError):
        Settings(**{field: value})
