"""Canonical hashing and exclusive persistence for research artifacts.

Every research artifact is content-addressed: the stored ``artifact_sha256``
covers the canonical JSON form of all other fields.  Writing is exclusive by
construction, so a rerun can never silently replace evidence that a gate or a
promotion already referenced.
"""

from __future__ import annotations

import json
import os
from hashlib import sha256
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from .domain import FrozenModel

Sha256 = str
_HASH_FIELD = "artifact_sha256"


class ArtifactIntegrityError(ValueError):
    """A persisted artifact does not match its own content address."""


def canonical_json(payload: Any) -> str:
    """Return the one canonical JSON encoding used for every artifact hash."""

    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_hash(payload: Any) -> Sha256:
    return sha256(canonical_json(payload).encode()).hexdigest()


def model_hash(model: BaseModel, *, exclude: frozenset[str] = frozenset({_HASH_FIELD})) -> Sha256:
    """Hash a model without its own content address."""

    return canonical_hash(model.model_dump(mode="json", exclude=set(exclude)))


class HashedArtifact(FrozenModel):
    """Base class for artifacts that carry and verify their own hash."""

    def expected_hash(self) -> Sha256:
        return model_hash(self)

    def verify(self) -> None:
        stored = getattr(self, _HASH_FIELD, None)
        if stored != self.expected_hash():
            raise ArtifactIntegrityError(
                f"{type(self).__name__} does not match its stored content address"
            )

    def sealed(self) -> Any:
        """Return a copy whose ``artifact_sha256`` matches its content."""

        provisional = self.model_copy(update={_HASH_FIELD: "0" * 64})
        return provisional.model_copy(update={_HASH_FIELD: model_hash(provisional)})


def write_artifact(artifact: BaseModel, path: str | Path) -> Path:
    """Persist an artifact exclusively; an existing target is never overwritten."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(artifact, HashedArtifact):
        artifact.verify()
    descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(artifact.model_dump_json(indent=2))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return target


def read_artifact[ArtifactModel: BaseModel](
    model: type[ArtifactModel],
    path: str | Path,
) -> ArtifactModel:
    """Load an artifact and re-verify its content address before use."""

    loaded = model.model_validate_json(Path(path).read_text(encoding="utf-8"))
    if isinstance(loaded, HashedArtifact):
        loaded.verify()
    return loaded


__all__ = [
    "ArtifactIntegrityError",
    "HashedArtifact",
    "Sha256",
    "canonical_hash",
    "canonical_json",
    "model_hash",
    "read_artifact",
    "write_artifact",
]
