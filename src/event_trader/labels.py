"""Human reference labels and provider-neutral model benchmark metrics."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Hashable
from statistics import mean

import numpy as np
from pydantic import Field

from .domain import Direction, FrozenModel, Materiality
from .research import ModelBenchmarkResult


class ReferenceLabel(FrozenModel):
    accession_number: str
    category: str = Field(min_length=1)
    direction: Direction
    materiality: Materiality


class ScoredPrediction(FrozenModel):
    accession_number: str
    model_id: str
    schema_valid: bool
    category: str | None = None
    direction: Direction | None = None
    materiality: Materiality | None = None
    latency_seconds: float
    cost_eur: float


def _macro_f1[LabelT: Hashable](actual: list[LabelT], predicted: list[LabelT]) -> float:
    scores: list[float] = []
    pairs = tuple(zip(actual, predicted, strict=True))
    for label in set(actual) | set(predicted):
        true_positive = sum(a == label and p == label for a, p in pairs)
        false_positive = sum(a != label and p == label for a, p in pairs)
        false_negative = sum(a == label and p != label for a, p in pairs)
        denominator = 2 * true_positive + false_positive + false_negative
        scores.append(2 * true_positive / denominator if denominator else 0.0)
    return mean(scores)


def benchmark_predictions(
    labels: list[ReferenceLabel],
    predictions: list[ScoredPrediction],
) -> list[ModelBenchmarkResult]:
    if not labels:
        raise ValueError("model benchmark requires reference labels")
    truth = {label.accession_number: label for label in labels}
    if len(truth) != len(labels):
        raise ValueError("reference accessions must be unique")
    by_model: dict[str, list[ScoredPrediction]] = defaultdict(list)
    seen_predictions: set[tuple[str, str]] = set()
    for prediction in predictions:
        prediction_key = (prediction.model_id, prediction.accession_number)
        if prediction_key in seen_predictions:
            raise ValueError("each model may predict an accession only once")
        seen_predictions.add(prediction_key)
        if prediction.accession_number not in truth:
            raise ValueError("predictions must use only accessions from the reference set")
        by_model[prediction.model_id].append(prediction)

    results: list[ModelBenchmarkResult] = []
    for model_id, records in by_model.items():
        total = len(labels)
        valid = [
            record
            for record in records
            if record.schema_valid
            and record.category is not None
            and record.direction is not None
            and record.materiality is not None
        ]
        actionable = [
            record
            for record in valid
            if record.direction is not Direction.NEUTRAL and record.materiality is Materiality.HIGH
        ]
        correct_actionable = sum(
            truth[record.accession_number].direction is record.direction
            and truth[record.accession_number].materiality is Materiality.HIGH
            for record in actionable
        )
        actual_direction = [truth[record.accession_number].direction for record in valid]
        predicted_direction = [record.direction for record in valid if record.direction is not None]
        actual_category = [truth[record.accession_number].category for record in valid]
        predicted_category = [record.category for record in valid if record.category is not None]
        actual_materiality = [truth[record.accession_number].materiality for record in valid]
        predicted_materiality = [
            record.materiality for record in valid if record.materiality is not None
        ]
        macro_f1 = (
            mean(
                (
                    _macro_f1(actual_direction, predicted_direction),
                    _macro_f1(actual_category, predicted_category),
                    _macro_f1(actual_materiality, predicted_materiality),
                )
            )
            if valid
            else 0
        )
        results.append(
            ModelBenchmarkResult(
                model_id=model_id,
                reference_count=total,
                prediction_count=len(records),
                schema_valid_rate=len(valid) / total if total else 0,
                actionable_precision=(correct_actionable / len(actionable) if actionable else 0),
                macro_f1=macro_f1,
                p95_latency_seconds=float(
                    np.quantile([record.latency_seconds for record in records], 0.95)
                ),
                average_cost_eur=mean(record.cost_eur for record in records),
            )
        )
    return sorted(results, key=lambda result: result.model_id)
