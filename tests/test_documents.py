from hashlib import sha256

import pytest

from event_trader.documents import (
    DocumentIntegrityError,
    FilingDocumentLoader,
    extract_visible_text,
    split_document_text,
)


def test_html_extraction_removes_script_and_normalizes_space() -> None:
    content = b"<html><style>secret</style><body>Hello  <b>world</b></body></html>"
    assert extract_visible_text(content) == "Hello world"


def test_document_loader_verifies_hash_and_root(tmp_path, filing) -> None:
    root = tmp_path / "raw"
    root.mkdir()
    target = root / "filing.html"
    target.write_bytes(b"<p>raised guidance</p>")
    document = filing.documents[0].model_copy(
        update={
            "local_path": str(target),
            "sha256": sha256(target.read_bytes()).hexdigest(),
        }
    )
    updated = filing.model_copy(update={"documents": (document,)})
    text = FilingDocumentLoader(root).load_text(updated)
    assert "raised guidance" in text
    target.write_bytes(b"tampered")
    with pytest.raises(DocumentIntegrityError, match="hash mismatch"):
        FilingDocumentLoader(root).load_text(updated)


def test_document_loader_rejects_path_escape(tmp_path, filing) -> None:
    outside = tmp_path / "outside.html"
    outside.write_bytes(b"data")
    document = filing.documents[0].model_copy(
        update={
            "local_path": str(outside),
            "sha256": sha256(outside.read_bytes()).hexdigest(),
        }
    )
    updated = filing.model_copy(update={"documents": (document,)})
    with pytest.raises(DocumentIntegrityError, match="escapes"):
        FilingDocumentLoader(tmp_path / "raw").load_text(updated)


def test_document_sections_preserve_exact_hash_binding() -> None:
    first_hash = "a" * 64
    second_hash = "b" * 64
    sections = split_document_text(
        f"[DOCUMENT kind=8-K sha256={first_hash}] Routine filing text.\n"
        f"[DOCUMENT kind=EX-99.1 sha256={second_hash}] Raised guidance."
    )

    assert [(section.sha256, section.text) for section in sections] == [
        (first_hash, "Routine filing text."),
        (second_hash, "Raised guidance."),
    ]


def test_document_sections_reject_ambiguous_envelope() -> None:
    digest = "a" * 64
    with pytest.raises(DocumentIntegrityError, match="precedes"):
        split_document_text(f"unbound text\n[DOCUMENT kind=8-K sha256={digest}] Filing text.")
