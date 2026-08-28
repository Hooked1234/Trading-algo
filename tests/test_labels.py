import pytest

from event_trader.domain import Direction, Materiality
from event_trader.labels import ReferenceLabel, ScoredPrediction, benchmark_predictions


def test_model_benchmark_uses_same_reference_contract() -> None:
    labels = [
        ReferenceLabel(
            accession_number=f"0000000001-26-{index:06d}",
            category="earnings",
            direction=direction,
            materiality=Materiality.HIGH,
        )
        for index, direction in enumerate(
            [Direction.LONG, Direction.SHORT, Direction.NEUTRAL], start=1
        )
    ]
    predictions = [
        ScoredPrediction(
            accession_number=label.accession_number,
            model_id="model",
            schema_valid=True,
            category=label.category,
            direction=label.direction,
            materiality=label.materiality,
            latency_seconds=1,
            cost_eur=0.001,
        )
        for label in labels
    ]
    result = benchmark_predictions(labels, predictions)[0]
    assert result.schema_valid_rate == 1
    assert result.actionable_precision == 1
    assert result.macro_f1 == 1
    assert result.reference_count == 3
    assert not result.passes


def test_model_benchmark_rejects_duplicate_predictions() -> None:
    label = ReferenceLabel(
        accession_number="0000000001-26-000001",
        category="earnings",
        direction=Direction.LONG,
        materiality=Materiality.HIGH,
    )
    prediction = ScoredPrediction(
        accession_number=label.accession_number,
        model_id="model",
        schema_valid=True,
        category=label.category,
        direction=Direction.LONG,
        materiality=Materiality.HIGH,
        latency_seconds=1,
        cost_eur=0.001,
    )
    with pytest.raises(ValueError, match="only once"):
        benchmark_predictions([label], [prediction, prediction])


def test_actionable_precision_requires_direction_and_high_materiality() -> None:
    labels = [
        ReferenceLabel(
            accession_number="0000000001-26-000001",
            category="earnings",
            direction=Direction.LONG,
            materiality=Materiality.HIGH,
        ),
        ReferenceLabel(
            accession_number="0000000001-26-000002",
            category="earnings",
            direction=Direction.SHORT,
            materiality=Materiality.MEDIUM,
        ),
        ReferenceLabel(
            accession_number="0000000001-26-000003",
            category="earnings",
            direction=Direction.SHORT,
            materiality=Materiality.HIGH,
        ),
    ]
    predictions = [
        ScoredPrediction(
            accession_number=label.accession_number,
            model_id="model",
            schema_valid=True,
            category=label.category,
            direction=label.direction,
            materiality=predicted_materiality,
            latency_seconds=1,
            cost_eur=0.001,
        )
        for label, predicted_materiality in zip(
            labels,
            (Materiality.HIGH, Materiality.HIGH, Materiality.MEDIUM),
            strict=True,
        )
    ]

    result = benchmark_predictions(labels, predictions)[0]

    assert result.actionable_precision == 0.5


def test_model_benchmark_rejects_predictions_outside_reference_set() -> None:
    label = ReferenceLabel(
        accession_number="0000000001-26-000001",
        category="earnings",
        direction=Direction.LONG,
        materiality=Materiality.HIGH,
    )
    prediction = ScoredPrediction(
        accession_number="0000000001-26-999999",
        model_id="model",
        schema_valid=True,
        category="earnings",
        direction=Direction.LONG,
        materiality=Materiality.HIGH,
        latency_seconds=1,
        cost_eur=0.001,
    )

    with pytest.raises(ValueError, match="reference set"):
        benchmark_predictions([label], [prediction])


def test_incomplete_hundred_item_reference_predictions_cannot_pass() -> None:
    labels = [
        ReferenceLabel(
            accession_number=f"0000000001-26-{index:06d}",
            category="earnings",
            direction=Direction.LONG,
            materiality=Materiality.HIGH,
        )
        for index in range(1, 101)
    ]
    predictions = [
        ScoredPrediction(
            accession_number=label.accession_number,
            model_id="model",
            schema_valid=True,
            category=label.category,
            direction=label.direction,
            materiality=label.materiality,
            latency_seconds=1,
            cost_eur=0.001,
        )
        for label in labels[:-1]
    ]

    result = benchmark_predictions(labels, predictions)[0]

    assert result.reference_count == 100
    assert result.prediction_count == 99
    assert result.schema_valid_rate == 0.99
    assert not result.passes
