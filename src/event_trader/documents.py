"""Hash-verified SEC document loading and HTML-to-text extraction."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path

from .domain import FilingEvent


class DocumentIntegrityError(RuntimeError):
    pass


_DOCUMENT_HEADER_RE = re.compile(
    r"^\[DOCUMENT kind=(?P<kind>[^\]\r\n]{1,128}) "
    r"sha256=(?P<sha256>[0-9a-f]{64})\] ",
    flags=re.MULTILINE,
)


@dataclass(frozen=True, slots=True)
class DocumentTextSection:
    """One untrusted document body with its content-addressed identity."""

    kind: str
    sha256: str
    text: str


def split_document_text(value: str) -> tuple[DocumentTextSection, ...]:
    """Parse text produced by :class:`FilingDocumentLoader`.

    Document bodies are flattened to one line by :func:`extract_visible_text`,
    while loader-created headers always begin a new line.  Consequently an
    untrusted filing body cannot create a second valid section boundary.
    Callers must treat an empty result as unstructured legacy input, not as a
    verified multi-document mapping.
    """

    matches = tuple(_DOCUMENT_HEADER_RE.finditer(value))
    if not matches:
        return ()
    if value[: matches[0].start()].strip():
        raise DocumentIntegrityError("text precedes the first document header")

    sections: list[DocumentTextSection] = []
    seen_hashes: set[str] = set()
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(value)
        body = value[match.end() : end].strip()
        digest = match.group("sha256")
        if not body:
            raise DocumentIntegrityError("document section has no text")
        if digest in seen_hashes:
            raise DocumentIntegrityError("document text contains a duplicate hash")
        seen_hashes.add(digest)
        sections.append(
            DocumentTextSection(
                kind=match.group("kind"),
                sha256=digest,
                text=body,
            )
        )
    return tuple(sections)


def verified_document_texts(filing: FilingEvent, text: str) -> dict[str, str]:
    """Bind extracted text sections to exactly the filing's persisted hashes."""

    expected_hashes = tuple(document.sha256 for document in filing.documents)
    if not expected_hashes or len(expected_hashes) != len(set(expected_hashes)):
        return {}
    try:
        sections = split_document_text(text)
    except DocumentIntegrityError:
        return {}
    if not sections:
        return {expected_hashes[0]: text} if len(expected_hashes) == 1 else {}
    if {section.sha256 for section in sections} != set(expected_hashes):
        return {}
    return {section.sha256: section.text for section in sections}


def evidence_excerpt_occurs(document_text: str, excerpt: str) -> bool:
    """Match an evidence excerpt after Unicode and whitespace normalization."""

    normalize = lambda value: " ".join(  # noqa: E731
        unicodedata.normalize("NFKC", value).casefold().split()
    )
    normalized_excerpt = normalize(excerpt)
    return bool(normalized_excerpt) and normalized_excerpt in normalize(document_text)


class _VisibleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._hidden_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style", "noscript"}:
            self._hidden_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "noscript"} and self._hidden_depth:
            self._hidden_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self._hidden_depth and data.strip():
            self.parts.append(data)


def extract_visible_text(content: bytes) -> str:
    decoded = content.decode("utf-8", errors="replace")
    parser = _VisibleTextParser()
    parser.feed(decoded)
    text = " ".join(parser.parts) if parser.parts else decoded
    return re.sub(r"\s+", " ", text).strip()


class FilingDocumentLoader:
    def __init__(self, raw_root: Path, *, max_output_chars: int = 500_000) -> None:
        self.raw_root = raw_root.resolve()
        self.max_output_chars = max_output_chars

    def load_text(self, filing: FilingEvent) -> str:
        parts: list[str] = []
        for document in filing.documents:
            if document.local_path is None:
                raise DocumentIntegrityError("document has no persisted local path")
            path = Path(document.local_path).resolve()
            if not path.is_relative_to(self.raw_root):
                raise DocumentIntegrityError("document path escapes the raw data root")
            content = path.read_bytes()
            digest = hashlib.sha256(content).hexdigest()
            if digest != document.sha256:
                raise DocumentIntegrityError("persisted document hash mismatch")
            parts.append(
                f"[DOCUMENT kind={document.kind} sha256={document.sha256}] "
                f"{extract_visible_text(content)}"
            )
        combined = "\n".join(parts)
        if not combined:
            raise DocumentIntegrityError("filing has no documents")
        if len(combined) > self.max_output_chars:
            raise DocumentIntegrityError("combined filing text exceeds safety limit")
        return combined
