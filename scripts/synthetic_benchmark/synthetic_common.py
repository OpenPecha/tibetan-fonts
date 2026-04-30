#!/usr/bin/env python3
"""Shared helpers for the BoCorpus synthetic benchmark pipeline."""

from __future__ import annotations

import csv
import hashlib
import math
import re
import sys
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Iterator

import pyarrow.parquet as pq

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
BENCHMARK_DIR = REPO_ROOT / "scripts" / "benchmark_gen"
COVERAGE_DIR = REPO_ROOT / "scripts" / "coverage_report"

DEFAULT_BENCHMARK_CSV = BENCHMARK_DIR / "catalog" / "Benchmark catalog - digital_fonts.csv"
DEFAULT_SCRIPTS_CSV = BENCHMARK_DIR / "catalog" / "Script lists - Scripts.csv"
DEFAULT_FONTS_CSV = BENCHMARK_DIR / "digital_fonts.filtered.csv"
DEFAULT_BOCORPUS_PARQUET = COVERAGE_DIR / ".cache" / "bocorpus" / "bo_corpus.parquet"
DEFAULT_STACKS_CSV = COVERAGE_DIR / "bocorpus_stacks.csv"
DEFAULT_CHUNKS_PARQUET = SCRIPT_DIR / "out" / "bocorpus_chunks.parquet"
DEFAULT_RENDER_PLAN = SCRIPT_DIR / "out" / "render_plan.parquet"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "out" / "dataset"

TIBETAN_START = 0x0F00
TIBETAN_END = 0x0FFF
TSHEG = "་"
SHAD = "།"
TOC_TSHIG_RUN = TSHEG * 5
EXCLUDED_SCRIPT_IDS = {"239"}
DIFFICULTY_TOP_K = 10
RARE_STACK_THRESHOLD = 0.8


@dataclass(frozen=True)
class FontCatalogRow:
    basename: str
    font_file: str
    font_path: str
    font_abs_path: Path
    ps_name: str
    ttc_face_index: str
    font_size_pt: float
    dpi: int
    skt_ok: str
    script_id: str
    script_category: str
    script_type: str
    script_name: str


def ensure_repo_imports() -> None:
    """Make sibling coverage_report helpers importable for direct script execution."""
    coverage_str = str(COVERAGE_DIR)
    if coverage_str not in sys.path:
        sys.path.insert(0, coverage_str)


def in_tibetan_block(ch: str) -> bool:
    cp = ord(ch)
    return TIBETAN_START <= cp <= TIBETAN_END


def is_tibetan_text_with_spaces(text: str) -> bool:
    """Return True when text contains only Tibetan Unicode and ASCII spaces."""
    return all(ch == " " or in_tibetan_block(ch) for ch in text)


def is_pure_tibetan_stack(stack: str) -> bool:
    return bool(stack) and all(in_tibetan_block(ch) for ch in stack)


def parse_int(value: object, default: int = 0) -> int:
    try:
        text = "" if value is None else str(value).strip()
        return int(text) if text else default
    except ValueError:
        return default


def parse_float(value: object, default: float) -> float:
    try:
        text = "" if value is None else str(value).strip()
        return float(text) if text else default
    except ValueError:
        return default


def clean_text(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"--\s*page\s+\d+\s*--", " ", text, flags=re.IGNORECASE)
    text = text.replace("\n", " ")
    text = re.sub(TSHEG + r"\s+", TSHEG, text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_bocorpus_text(text: str) -> str:
    from botok import normalize_unicode
    from botok.utils.lenient_normalization import normalize_graphical

    return clean_text(normalize_graphical(normalize_unicode(text)))


def tokenize_tibetan_stacks(text: str) -> list[str]:
    from botok import tokenize_in_stacks

    return [stack for stack in tokenize_in_stacks(text) if is_pure_tibetan_stack(stack)]


def has_tibetan_letter(stack: str) -> bool:
    return any(unicodedata.category(ch).startswith("L") for ch in stack)


@lru_cache(maxsize=4)
def load_stack_rarity_scores(stacks_csv: str = str(DEFAULT_STACKS_CSV)) -> dict[str, float]:
    """Load corpus stack counts and map each stack to a percentile-clipped rarity in [0, 1]."""
    counts: dict[str, int] = {}
    with Path(stacks_csv).open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            stack = (row.get("stack") or "").strip()
            if not stack or not is_pure_tibetan_stack(stack) or not has_tibetan_letter(stack):
                continue
            count = parse_int(row.get("nb_occurences") or row.get("nb_occurrences"), 0)
            if count > 0:
                counts[stack] = count
    if not counts:
        return {}

    total = sum(counts.values())
    infos = sorted(-math.log(count / total) for count in counts.values())
    lo = infos[int(0.05 * (len(infos) - 1))]
    hi = infos[int(0.95 * (len(infos) - 1))]
    scale = hi - lo
    if scale <= 0:
        return {stack: 0.0 for stack in counts}

    return {
        stack: min(1.0, max(0.0, ((-math.log(count / total)) - lo) / scale))
        for stack, count in counts.items()
    }


def stack_difficulty_score(
    text: str,
    *,
    stacks_csv: Path | str = DEFAULT_STACKS_CSV,
    top_k: int = DIFFICULTY_TOP_K,
    rare_threshold: float = RARE_STACK_THRESHOLD,
) -> float:
    """Return a [0, 1] page difficulty score based on corpus stack rarity.

    Each Tibetan stack is assigned a rarity from `bocorpus_stacks.csv` using
    percentile-clipped self-information: very common stacks approach 0 and rare
    or unseen stacks approach 1. Punctuation-only stacks are ignored. The final
    score combines the mean rarity of the rarest `top_k` stack tokens, the
    density of rare stack tokens, and the ratio of unique rare stacks.
    """
    stacks = [stack for stack in tokenize_tibetan_stacks(text) if has_tibetan_letter(stack)]
    if not stacks:
        return 0.0

    rarity_by_stack = load_stack_rarity_scores(str(stacks_csv))
    rarities = [rarity_by_stack.get(stack, 1.0) for stack in stacks]
    rare_tokens = [value for value in rarities if value >= rare_threshold]
    unique_stacks = set(stacks)
    unique_rare_count = sum(1 for stack in unique_stacks if rarity_by_stack.get(stack, 1.0) >= rare_threshold)

    top_values = sorted(rarities, reverse=True)[: max(1, top_k)]
    top_k_mean = sum(top_values) / len(top_values)
    rare_density = len(rare_tokens) / len(rarities)
    unique_rare_ratio = unique_rare_count / len(unique_stacks)
    score = 0.5 * top_k_mean + 0.3 * rare_density + 0.2 * unique_rare_ratio
    return min(1.0, max(0.0, score))


def split_units(text: str) -> list[str]:
    """Split text into page-building units, roughly sentence/shad-sized."""
    parts: list[str] = []
    buf: list[str] = []
    for ch in text:
        buf.append(ch)
        if ch == SHAD:
            unit = "".join(buf).strip()
            if len(unit) >= 8:
                parts.append(unit)
            buf = []
    tail = "".join(buf).strip()
    if len(tail) >= 8:
        parts.append(tail)
    return parts


def chunk_units(
    units: Iterable[str],
    *,
    target_chars: int,
    min_chars: int,
    max_chars: int,
) -> Iterator[tuple[str, int]]:
    buf: list[str] = []
    char_count = 0
    start_offset = 0
    running_offset = 0
    for unit in units:
        if not buf:
            start_offset = running_offset
        candidate_len = char_count + (1 if buf else 0) + len(unit)
        if buf and candidate_len > max_chars and char_count >= min_chars:
            yield " ".join(buf), start_offset
            buf = []
            char_count = 0
            start_offset = running_offset
        if not buf:
            start_offset = running_offset
        buf.append(unit)
        char_count += (1 if len(buf) > 1 else 0) + len(unit)
        running_offset += len(unit) + 1
        if char_count >= target_chars and char_count >= min_chars:
            yield " ".join(buf), start_offset
            buf = []
            char_count = 0
    if buf and char_count >= min_chars:
        yield " ".join(buf), start_offset


def looks_like_toc_chunk(text: str) -> bool:
    """Table-of-contents rows often contain long tsheg leader runs."""
    return TOC_TSHIG_RUN in text


def stable_chunk_id(row_index: int, char_start: int, text: str) -> str:
    h = hashlib.sha1()
    h.update(f"{row_index}:{char_start}:".encode("utf-8"))
    h.update(text.encode("utf-8"))
    return h.hexdigest()[:16]


def iter_bocorpus_texts_parquet(path: Path, *, limit_rows: int | None = None) -> Iterator[tuple[int, str]]:
    pf = pq.ParquetFile(str(path))
    row_index = 0
    for batch in pf.iter_batches(batch_size=64, columns=["text"]):
        for text in batch.column(0).to_pylist():
            if limit_rows is not None and row_index >= limit_rows:
                return
            if text is not None:
                yield row_index, text if isinstance(text, str) else str(text)
            row_index += 1


def load_scripts(path: Path) -> dict[str, dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return {row["id"].strip(): row for row in csv.DictReader(f) if row.get("id", "").strip()}


def load_font_size_map(path: Path) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    if not path.exists():
        return result
    with path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            font_path = row.get("font_path", "").strip()
            if not font_path:
                continue
            result[font_path] = {
                "basename": row.get("basename", "").strip(),
                "font_size_pt": row.get("font_size_pt", "").strip(),
                "dpi": row.get("dpi", "").strip(),
                "skt_ok": row.get("skt_ok", "").strip(),
            }
    return result


def load_font_catalog(
    benchmark_csv: Path = DEFAULT_BENCHMARK_CSV,
    scripts_csv: Path = DEFAULT_SCRIPTS_CSV,
    fonts_csv: Path = DEFAULT_FONTS_CSV,
    *,
    exclude_script_ids: set[str] | None = None,
) -> list[FontCatalogRow]:
    exclude_script_ids = exclude_script_ids or EXCLUDED_SCRIPT_IDS
    scripts = load_scripts(scripts_csv)
    font_meta = load_font_size_map(fonts_csv)
    rows: list[FontCatalogRow] = []
    with benchmark_csv.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            script_id = row.get("script id", "").strip()
            font_path = row.get("font_path", "").strip()
            if not font_path or not script_id or script_id in exclude_script_ids:
                continue
            meta = scripts.get(script_id, {})
            size_meta = font_meta.get(font_path, {})
            basename = size_meta.get("basename") or Path(font_path).stem
            font_abs_path = BENCHMARK_DIR / font_path
            rows.append(
                FontCatalogRow(
                    basename=basename,
                    font_file=Path(font_path).name,
                    font_path=font_path,
                    font_abs_path=font_abs_path,
                    ps_name=row.get("font ps_name", "").strip(),
                    ttc_face_index=row.get("ttc_face_index", "").strip(),
                    font_size_pt=parse_float(size_meta.get("font_size_pt") or row.get("font_size_pt"), 24.0),
                    dpi=parse_int(size_meta.get("dpi") or row.get("dpi"), 300),
                    skt_ok=size_meta.get("skt_ok") or row.get("skt_ok", ""),
                    script_id=script_id,
                    script_category=meta.get("8 categories", "").strip(),
                    script_type=meta.get("3 types", "").strip(),
                    script_name=meta.get("name (phonetics, Wylie in parentheses, and English)", "").strip(),
                )
            )
    return rows


def tex_escape(text: str) -> str:
    return (
        text.replace("\\", "")
        .replace("%", "")
        .replace("$", "")
        .replace("&", "")
        .replace("#", "")
        .replace("_", "")
        .replace("{", "")
        .replace("}", "")
    )


def add_tibetan_breakpoints(text: str) -> str:
    text = re.sub("་(?!།)", r"་\\allowbreak{}", text)
    text = text.replace(" ", r"\hspace{0.35em}\allowbreak{}")
    return text

