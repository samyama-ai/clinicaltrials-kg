"""Tests for the AACT batch loader (etl.aact_batch_loader).

Covers the pure helpers — Cypher escaping, property-dict serialization,
TSV reading, and batch chunking — without requiring a live server.
"""

from pathlib import Path

import pytest

from etl.aact_batch_loader import BATCH_SIZE, _escape, _props_str, read_tsv


# ── _escape ────────────────────────────────────────────────────────────────


def test_escape_none_is_null_literal():
    assert _escape(None) == "null"


def test_escape_bool_lowercase():
    assert _escape(True) == "true"
    assert _escape(False) == "false"


@pytest.mark.parametrize("val,expected", [
    (0, "0"),
    (42, "42"),
    (-7, "-7"),
    (3.14, "3.14"),
    (1.0e-6, "1e-06"),
])
def test_escape_numeric_unquoted(val, expected):
    assert _escape(val) == expected


def test_escape_string_single_quoted():
    assert _escape("hello") == "'hello'"


def test_escape_string_with_apostrophe_is_backslash_escaped():
    assert _escape("Alzheimer's") == r"'Alzheimer\'s'"


def test_escape_string_with_backslash_doubled_first():
    # Order matters: backslashes must be doubled BEFORE apostrophes are
    # escaped, otherwise `\'` injected by the apostrophe-escape step would
    # itself get doubled into `\\'` and break the literal.
    assert _escape(r"C:\tmp") == r"'C:\\tmp'"


def test_escape_cast_non_primitive_via_str():
    class Wrapper:
        def __str__(self) -> str:
            return "wrapped"
    assert _escape(Wrapper()) == "'wrapped'"


# ── _props_str ─────────────────────────────────────────────────────────────


def test_props_str_empty_dict_renders_empty_braces():
    assert _props_str({}) == "{}"


def test_props_str_single_key():
    assert _props_str({"name": "Alice"}) == "{name: 'Alice'}"


def test_props_str_multiple_keys_comma_separated():
    out = _props_str({"name": "Alice", "age": 30})
    assert out.startswith("{") and out.endswith("}")
    assert "name: 'Alice'" in out
    assert "age: 30" in out


def test_props_str_mixed_types():
    out = _props_str({"enabled": True, "count": 0, "tag": None})
    assert "enabled: true" in out
    assert "count: 0" in out
    assert "tag: null" in out


# ── read_tsv ───────────────────────────────────────────────────────────────


def _write_pipe(tmp_path: Path, name: str, header: str, rows: list[str]) -> str:
    path = tmp_path / name
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
    return str(path)


def test_read_tsv_parses_pipe_delimited_rows(tmp_path):
    path = _write_pipe(
        tmp_path,
        "studies.txt",
        "nct_id|brief_title|phase",
        [
            "NCT00000001|Cancer Trial|Phase 2",
            "NCT00000002|Diabetes Drug|Phase 3",
        ],
    )
    rows = read_tsv(path)
    assert len(rows) == 2
    assert rows[0]["nct_id"] == "NCT00000001"
    assert rows[0]["brief_title"] == "Cancer Trial"
    assert rows[1]["phase"] == "Phase 3"


def test_read_tsv_respects_max_rows(tmp_path):
    path = _write_pipe(
        tmp_path,
        "many.txt",
        "id|val",
        [f"{i}|v{i}" for i in range(100)],
    )
    rows = read_tsv(path, max_rows=5)
    assert len(rows) == 5
    assert rows[-1]["id"] == "4"


def test_read_tsv_missing_file_returns_empty(tmp_path):
    missing = str(tmp_path / "does_not_exist.txt")
    assert read_tsv(missing) == []


def test_read_tsv_tolerates_bad_utf8(tmp_path):
    path = tmp_path / "bad.txt"
    # 0xFF is invalid UTF-8. errors="replace" in read_tsv should not raise.
    path.write_bytes(b"nct_id|note\nNCT001|\xff-garbled\n")
    rows = read_tsv(str(path))
    assert len(rows) == 1
    assert rows[0]["nct_id"] == "NCT001"


# ── batch size constant sanity ─────────────────────────────────────────────


def test_batch_size_is_a_reasonable_positive_int():
    assert isinstance(BATCH_SIZE, int)
    assert 100 <= BATCH_SIZE <= 100_000
