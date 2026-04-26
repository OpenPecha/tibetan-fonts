#!/usr/bin/env python3
"""Shared helpers for Tibetan font coverage reports."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import subprocess
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import uharfbuzz as hb
from fontTools.ttLib import TTCollection, TTFont


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
BENCHMARK_DIR = REPO_ROOT / "scripts" / "benchmark_gen"
DEFAULT_FONTS_CSV = BENCHMARK_DIR / "digital_fonts.filtered.csv"
DEFAULT_OUT_DIR = SCRIPT_DIR / "out"

TIBETAN_START = 0x0F00
TIBETAN_END = 0x0FFF
DOTTED_CIRCLE = "\u25cc"

TIBETAN_BASE_LETTER_RANGES = ((0x0F40, 0x0F6C),)
TIBETAN_SUBJOINED_RANGES = ((0x0F90, 0x0FBC),)
TIBETAN_VOWEL_AND_DIACRITIC_RANGES = (
    (0x0F71, 0x0F84),
    (0x0F86, 0x0F87),
    (0x0FC6, 0x0FC6),
)
BOTTOM_VOWELS = {
    "\u0F71",
    "\u0F73",
    "\u0F74",
    "\u0F75",
    "\u0F76",
    "\u0F77",
    "\u0F80",
    "\u0F81",
}
TOP_DIACRITICS = {
    "\u0F7E",
    "\u0F82",
    "\u0F83",
}
TOP_MARKS = {
    "\u0F72",
    "\u0F7A",
    "\u0F7B",
    "\u0F7C",
    "\u0F7D",
    "\u0F7E",
    "\u0F80",
    "\u0F82",
    "\u0F83",
}

# Ordinary text coverage probes. These are intentionally not a Sanskrit stress
# test; fonts with skt_ok=0 should still pass this tier to be useful.
NORMAL_TIBETAN_PROBES = [
    "ཀ",
    "ཁ",
    "ག",
    "ང",
    "ཅ",
    "ཉ",
    "ཏ",
    "ད",
    "ན",
    "པ",
    "བ",
    "མ",
    "ཙ",
    "ཚ",
    "ཛ",
    "ཝ",
    "ཞ",
    "ཟ",
    "འ",
    "ཡ",
    "ར",
    "ལ",
    "ཤ",
    "ས",
    "ཧ",
    "ཨ",
    "ཀི",
    "ཀུ",
    "ཀེ",
    "ཀོ",
    "ཀྱ",
    "ཀྲ",
    "ཀླ",
    "སྐ",
    "སྒ",
    "སྟ",
    "སྤ",
    "རྐ",
    "རྒ",
    "ལྟ",
    "བརྒྱ",
    "བསྒྲུབ",
    "མཁྱེན",
    "རྗེ",
    "རྒྱལ",
    "བོད་ཡིག།",
    "༄༅། །",
    "༠༡༢༣༤༥༦༧༨༩",
]


@dataclass(frozen=True)
class FontRow:
    basename: str
    font_path: Path
    font_path_csv: str
    ttc_face_index: int
    ttc_face_index_csv: str
    ps_name: str
    other_names: str
    font_size_pt: float
    dpi: int
    skt_ok: int | None


def parse_int(value: object, default: int = 0) -> int:
    text = "" if value is None else str(value).strip()
    if text == "":
        return default
    return int(float(text))


def parse_float(value: object, default: float = 0.0) -> float:
    text = "" if value is None else str(value).strip()
    if text == "":
        return default
    return float(text)


def resolve_font_path(font_path: str, csv_path: Path) -> Path:
    path = Path(font_path)
    if path.is_absolute():
        return path
    return (csv_path.parent / path).resolve()


def load_font_rows(csv_path: Path = DEFAULT_FONTS_CSV) -> list[FontRow]:
    rows: list[FontRow] = []
    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            font_path_csv = (row.get("font_path") or "").strip()
            basename = (row.get("basename") or "").strip()
            if not basename or not font_path_csv:
                continue
            ttc_face_index_csv = (row.get("ttc_face_index") or "").strip()
            skt_ok_raw = (row.get("skt_ok") or "").strip()
            rows.append(
                FontRow(
                    basename=basename,
                    font_path=resolve_font_path(font_path_csv, csv_path),
                    font_path_csv=font_path_csv,
                    ttc_face_index=parse_int(ttc_face_index_csv, 0),
                    ttc_face_index_csv=ttc_face_index_csv,
                    ps_name=(row.get("ps_name") or "").strip(),
                    other_names=(row.get("other_names") or "").strip(),
                    font_size_pt=parse_float(row.get("font_size_pt"), 24.0),
                    dpi=parse_int(row.get("dpi"), 300),
                    skt_ok=int(skt_ok_raw) if skt_ok_raw in {"0", "1"} else None,
                )
            )
    return rows


def select_font_rows(
    rows: Iterable[FontRow],
    *,
    basenames: set[str] | None = None,
    skt_ok: int | None = None,
    limit: int | None = None,
) -> list[FontRow]:
    selected: list[FontRow] = []
    for row in rows:
        if basenames is not None and row.basename not in basenames:
            continue
        if skt_ok is not None and row.skt_ok != skt_ok:
            continue
        selected.append(row)
        if limit is not None and len(selected) >= limit:
            break
    return selected


def in_tibetan_block(ch: str) -> bool:
    return TIBETAN_START <= ord(ch) <= TIBETAN_END


def is_tibetan_line(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    return all(in_tibetan_block(ch) for ch in stripped)


def read_stack_lines(path: Path, *, limit: int | None = None) -> list[str]:
    """Read NFD stack lines, preserving normalization and filtering metadata."""
    stacks: list[str] = []
    seen: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        stack = raw.strip()
        if not is_tibetan_line(stack):
            continue
        if stack in seen:
            continue
        seen.add(stack)
        stacks.append(stack)
        if limit is not None and len(stacks) >= limit:
            break
    return stacks


def read_probe_lines(path: Path, *, limit: int | None = None) -> list[str]:
    probes = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        probe = raw.strip()
        if not probe or probe.startswith("#"):
            continue
        probes.append(probe)
        if limit is not None and len(probes) >= limit:
            break
    return probes


def codepoints(text: str) -> list[str]:
    return [f"U+{ord(ch):04X}" for ch in text]


def in_ranges(ch: str, ranges: tuple[tuple[int, int], ...]) -> bool:
    cp = ord(ch)
    return any(start <= cp <= end for start, end in ranges)


def stack_metrics(text: str) -> dict[str, object]:
    names = []
    for ch in text:
        try:
            names.append(unicodedata.name(ch))
        except ValueError:
            names.append("UNKNOWN")

    base_count = sum(1 for ch in text if in_ranges(ch, TIBETAN_BASE_LETTER_RANGES))
    subjoined_count = sum(1 for ch in text if in_ranges(ch, TIBETAN_SUBJOINED_RANGES))
    vowel_diacritic_count = sum(
        1 for ch in text if in_ranges(ch, TIBETAN_VOWEL_AND_DIACRITIC_RANGES)
    )
    combining_count = sum(1 for ch in text if unicodedata.combining(ch))
    return {
        "stack": text,
        "stack_len": len(text),
        "codepoints": " ".join(codepoints(text)),
        "unicode_names": " | ".join(names),
        "base_count": base_count,
        "subjoined_count": subjoined_count,
        "vowel_diacritic_count": vowel_diacritic_count,
        "combining_count": combining_count,
        "has_subjoined": subjoined_count > 0,
        "has_vowel_diacritic": vowel_diacritic_count > 0,
        "complexity": subjoined_count + vowel_diacritic_count + max(0, base_count - 1),
    }


def has_bottom_vowel(text: str) -> bool:
    return any(ch in BOTTOM_VOWELS for ch in text)


def font_file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class HarfbuzzShaper:
    def __init__(self, font_row: FontRow):
        self.font_row = font_row
        self.data = font_row.font_path.read_bytes()
        self.face = hb.Face(self.data, font_row.ttc_face_index)
        self.font = hb.Font(self.face)
        hb.ot_font_set_funcs(self.font)
        self.upem = self.face.upem

    def shape(self, text: str) -> dict[str, object]:
        buf = hb.Buffer()
        buf.add_str(text)
        buf.script = "tibt"
        buf.language = "bo"
        buf.direction = "ltr"
        try:
            hb.shape(self.font, buf, {})
        except Exception as exc:  # HarfBuzz errors are rare but worth preserving.
            return {
                "ok": False,
                "support_class": "shape_error",
                "reason": f"shape_error:{type(exc).__name__}:{exc}",
                "notdef_count": 0,
                "has_dotted_circle": False,
                "glyph_count": 0,
                "cluster_count": 0,
                "glyph_ids": "",
                "glyph_names": "",
                "clusters": "",
                "advances": "",
                "offsets": "",
                "bbox": "",
                "ink_area": 0,
                "glyph_boxes": "",
                "placement_warnings": "",
                "placement_warning_count": 0,
            }

        infos = list(buf.glyph_infos)
        positions = list(buf.glyph_positions)
        glyph_ids: list[int] = []
        glyph_names: list[str] = []
        clusters: list[int] = []
        advances: list[str] = []
        offsets: list[str] = []
        extents = []
        glyph_boxes = []
        ink_area = 0

        x_cursor = 0
        y_cursor = 0
        for info, pos in zip(infos, positions):
            gid = int(info.codepoint)
            glyph_ids.append(gid)
            clusters.append(int(info.cluster))
            try:
                name = self.font.get_glyph_name(gid) or self.font.glyph_to_string(gid)
            except Exception:
                name = str(gid)
            glyph_names.append(name)
            advances.append(f"{pos.x_advance},{pos.y_advance}")
            offsets.append(f"{pos.x_offset},{pos.y_offset}")

            extent = self.font.get_glyph_extents(gid)
            if extent is not None:
                x0 = x_cursor + pos.x_offset + extent.x_bearing
                y0 = y_cursor + pos.y_offset + extent.y_bearing
                x1 = x0 + extent.width
                y1 = y0 + extent.height
                extents.append((x0, y0, x1, y1))
                glyph_boxes.append(
                    {
                        "gid": gid,
                        "name": name,
                        "x0": x0,
                        "ytop": y0,
                        "x1": x1,
                        "ybot": y1,
                        "x_offset": pos.x_offset,
                        "y_offset": pos.y_offset,
                    }
                )
                ink_area += abs(extent.width * extent.height)
            x_cursor += pos.x_advance
            y_cursor += pos.y_advance

        notdef_count = sum(1 for gid, name in zip(glyph_ids, glyph_names) if gid == 0 or name == ".notdef")
        has_dotted_circle = any("dotted" in name.lower() or "25cc" in name.lower() for name in glyph_names)
        cluster_count = len(set(clusters))
        bbox = ""
        if extents:
            xs0, ys0, xs1, ys1 = zip(*extents)
            bbox = f"{min(xs0)},{min(ys0)},{max(xs1)},{max(ys1)}"

        reasons = []
        if notdef_count:
            reasons.append("notdef")
        if has_dotted_circle:
            reasons.append("dotted_circle")
        if not glyph_ids:
            reasons.append("empty_shape")
        if ink_area == 0 and glyph_ids:
            reasons.append("zero_ink")
        placement_warnings = detect_placement_warnings(text, glyph_boxes)
        reasons.extend(placement_warnings)

        ok = not reasons
        return {
            "ok": ok,
            "support_class": "ok" if ok else "fail",
            "reason": ";".join(reasons) if reasons else "ok",
            "notdef_count": notdef_count,
            "has_dotted_circle": has_dotted_circle,
            "glyph_count": len(glyph_ids),
            "cluster_count": cluster_count,
            "glyph_ids": " ".join(str(gid) for gid in glyph_ids),
            "glyph_names": " ".join(glyph_names),
            "clusters": " ".join(str(c) for c in clusters),
            "advances": " ".join(advances),
            "offsets": " ".join(offsets),
            "bbox": bbox,
            "ink_area": int(ink_area),
            "glyph_boxes": json.dumps(glyph_boxes, ensure_ascii=False, separators=(",", ":")),
            "placement_warnings": ";".join(placement_warnings),
            "placement_warning_count": len(placement_warnings),
        }


def detect_placement_warnings(text: str, glyph_boxes: list[dict[str, object]]) -> list[str]:
    """Flag obvious geometry failures that HarfBuzz can shape but not place well."""
    metrics = stack_metrics(text)
    if len(glyph_boxes) < 2:
        return []

    ybots = [int(box["ybot"]) for box in glyph_boxes]
    # Use the first glyph's top as the body top so top marks such as anusvara
    # do not hide middle-floating bottom vowels by inflating the stack height.
    stack_top = int(glyph_boxes[0]["ytop"])
    stack_bottom = min(ybots)
    stack_height = stack_top - stack_bottom
    if stack_height <= 0:
        return []

    warnings = []
    if detect_top_diacritic_collision(text, glyph_boxes):
        warnings.append("top_diacritic_collision")
    if detect_top_mark_overlap(text, glyph_boxes):
        warnings.append("top_mark_overlap")
    if detect_mark_horizontal_misalignment(text, glyph_boxes):
        warnings.append("mark_horizontal_misalignment")

    if has_bottom_vowel(text):
        # The bottom-vowel glyph is usually one of the final glyphs, but top
        # marks such as anusvara can come after it. In the Aathup failure this
        # glyph reaches low enough, yet its top begins above the previous stack
        # layer, making ya-tag / shabkyu float in the middle of the cluster.
        bottom_idx = len(glyph_boxes) - 2 if text[-1:] in TOP_MARKS else len(glyph_boxes) - 1
        if bottom_idx > 0:
            bottom_box = glyph_boxes[bottom_idx]
            previous_box = glyph_boxes[bottom_idx - 1]
            bottom_top = int(bottom_box["ytop"])
            previous_top = int(previous_box["ytop"])

            # Coordinates increase upwards. A bottom-vowel component should
            # start below the preceding lower-stack component by a meaningful
            # margin.
            min_layer_gap = 0.10 * stack_height
            previous_bottom = int(previous_box["ybot"])
            previous_height = previous_top - previous_bottom
            bottom_bottom = int(bottom_box["ybot"])
            bottom_too_high = (
                previous_height > 0
                and bottom_bottom > previous_bottom + 0.10 * previous_height
            )
            if bottom_top > previous_top - min_layer_gap or bottom_too_high:
                warnings.append("floating_bottom_vowel")

    if metrics["subjoined_count"] < 2 or len(glyph_boxes) < 2:
        return warnings

    if detect_subscript_layer_collision(text, glyph_boxes, stack_height):
        warnings.append("subscript_layer_collision")
    if detect_subscript_containment(text, glyph_boxes):
        warnings.append("subscript_containment")
    elif detect_subscript_overlap(text, glyph_boxes):
        warnings.append("subscript_overlap")
    elif detect_subscript_insufficient_descent(text, glyph_boxes):
        warnings.append("subscript_insufficient_descent")

    return warnings


def detect_top_diacritic_collision(text: str, glyph_boxes: list[dict[str, object]]) -> bool:
    top_count = sum(1 for ch in text if ch in TOP_DIACRITICS)
    if top_count < 2 or len(glyph_boxes) < 2:
        return False

    box_counts: dict[tuple[object, ...], int] = {}
    for box in glyph_boxes[1:]:
        key = (
            box["name"],
            int(box["x0"]),
            int(box["ytop"]),
            int(box["x1"]),
            int(box["ybot"]),
        )
        box_counts[key] = box_counts.get(key, 0) + 1
    if any(count >= 2 for count in box_counts.values()):
        return True

    base_top = int(glyph_boxes[0]["ytop"])
    # Top marks usually sit above the body. Repeated top marks that share the
    # same box are a clear pile-up, not a valid vertical composition.
    top_boxes = [
        box
        for box in glyph_boxes[1:]
        if int(box["ybot"]) >= base_top - 0.05 * abs(base_top or 1)
    ]
    if len(top_boxes) < 2:
        return False

    centers = [
        (
            (int(box["x0"]) + int(box["x1"])) / 2,
            (int(box["ytop"]) + int(box["ybot"])) / 2,
        )
        for box in top_boxes
    ]
    x_spread = max(x for x, _ in centers) - min(x for x, _ in centers)
    y_spread = max(y for _, y in centers) - min(y for _, y in centers)
    avg_height = sum(abs(int(box["ytop"]) - int(box["ybot"])) for box in top_boxes) / len(top_boxes)
    avg_width = sum(abs(int(box["x1"]) - int(box["x0"])) for box in top_boxes) / len(top_boxes)
    return x_spread <= 0.08 * avg_width and y_spread <= 0.08 * avg_height


def detect_top_mark_overlap(text: str, glyph_boxes: list[dict[str, object]]) -> bool:
    top_mark_count = sum(1 for ch in text if ch in TOP_MARKS)
    if top_mark_count < 2 or len(glyph_boxes) < 2:
        return False

    last = glyph_boxes[-1]
    previous = glyph_boxes[-2]
    last_top = int(last["ytop"])
    last_bottom = int(last["ybot"])
    previous_top = int(previous["ytop"])
    previous_bottom = int(previous["ybot"])
    last_height = last_top - last_bottom
    if last_height <= 0:
        return False

    overlap = min(last_top, previous_top) - max(last_bottom, previous_bottom)
    if overlap <= 0:
        return False
    overlap_ratio = overlap / last_height

    # Top marks should sit above the preceding body/stack glyph. If a separate
    # top mark is mostly inside that preceding glyph's vertical span, it is
    # colliding with a top vowel/mark already present in the composite glyph.
    return overlap_ratio >= 0.65 and last_bottom < previous_top


def detect_mark_horizontal_misalignment(text: str, glyph_boxes: list[dict[str, object]]) -> bool:
    # U+0F37 is a below/side mark. In invalid Amdo-style renderings it appears
    # as a separate glyph almost entirely outside the base stack's horizontal
    # span. Treat that as unsupported; a few false negatives are acceptable for
    # this coverage report.
    if "\u0F37" not in text or len(glyph_boxes) < 2:
        return False

    mark = glyph_boxes[-1]
    base_boxes = glyph_boxes[:-1]
    base_x0 = min(int(box["x0"]) for box in base_boxes)
    base_x1 = max(int(box["x1"]) for box in base_boxes)
    mark_x0 = int(mark["x0"])
    mark_x1 = int(mark["x1"])
    mark_width = mark_x1 - mark_x0
    if mark_width <= 0:
        return False

    overlap = min(mark_x1, base_x1) - max(mark_x0, base_x0)
    overlap_ratio = max(0, overlap) / mark_width
    mark_center = (mark_x0 + mark_x1) / 2
    base_width = max(1, base_x1 - base_x0)
    center_outside = mark_center < base_x0 - 0.10 * base_width or mark_center > base_x1 + 0.10 * base_width
    return overlap_ratio < 0.40 or center_outside


def detect_subscript_layer_collision(
    text: str,
    glyph_boxes: list[dict[str, object]],
    stack_height: float,
) -> bool:
    metrics = stack_metrics(text)
    if int(metrics["subjoined_count"]) < 4 or len(glyph_boxes) < 5:
        return False

    trailing_top_marks = 0
    for ch in reversed(text):
        if ch in TOP_MARKS:
            trailing_top_marks += 1
        else:
            break

    end_idx = len(glyph_boxes) - trailing_top_marks
    # Drop the base glyph and any trailing top diacritic. What remains should
    # form descending visual layers in a valid tall stack.
    subscript_boxes = glyph_boxes[1:end_idx]
    if len(subscript_boxes) < 4:
        return False

    ytops = [int(box["ytop"]) for box in subscript_boxes]
    ytop_spread = max(ytops) - min(ytops)
    if ytop_spread > 0.18 * stack_height:
        return False

    # Avoid flagging compact ligature glyphs: this detector is for multiple
    # visible subscript components all drawn in the same vertical band.
    distinct_glyphs = {str(box["name"]) for box in subscript_boxes}
    return len(distinct_glyphs) >= 4


def detect_subscript_containment(text: str, glyph_boxes: list[dict[str, object]]) -> bool:
    metrics = stack_metrics(text)
    if int(metrics["subjoined_count"]) < 2 or len(glyph_boxes) < 2:
        return False

    trailing_top_marks = 0
    for ch in reversed(text):
        if ch in TOP_MARKS:
            trailing_top_marks += 1
        else:
            break

    end_idx = len(glyph_boxes) - trailing_top_marks
    stack_boxes = glyph_boxes[:end_idx]
    if len(stack_boxes) < 2:
        return False

    last = stack_boxes[-1]
    previous = stack_boxes[-2]
    last_top = int(last["ytop"])
    last_bottom = int(last["ybot"])
    previous_top = int(previous["ytop"])
    previous_bottom = int(previous["ybot"])

    # Coordinates increase upwards. If the final subscript glyph mostly sits
    # inside the previous stack component, it is likely overlayed rather than
    # placed as the next lower layer. This catches shallow failures such as
    # Amdo_classic_1 rendering U+0F40 U+0F9F U+0FB2 and Amdo_classic_3
    # rendering U+0F62 U+0F90 U+0FB5 U+0FB1.
    if last_bottom <= previous_bottom:
        return False

    previous_height = previous_top - previous_bottom
    last_height = last_top - last_bottom
    if previous_height <= 0 or last_height <= 0:
        return False

    containment_margin = 0.05 * previous_height
    overlap = min(last_top, previous_top) - max(last_bottom, previous_bottom)
    overlap_ratio = overlap / last_height
    swallowed = (
        overlap_ratio >= 0.85
        and last_bottom > previous_bottom - containment_margin
        and last_height <= 0.80 * previous_height
    )
    return swallowed


def detect_subscript_overlap(text: str, glyph_boxes: list[dict[str, object]]) -> bool:
    metrics = stack_metrics(text)
    if int(metrics["subjoined_count"]) < 2 or len(glyph_boxes) < 3:
        return False

    trailing_top_marks = 0
    for ch in reversed(text):
        if ch in TOP_MARKS:
            trailing_top_marks += 1
        else:
            break

    end_idx = len(glyph_boxes) - trailing_top_marks
    stack_boxes = glyph_boxes[:end_idx]
    if len(stack_boxes) < 2:
        return False

    last = stack_boxes[-1]
    previous = stack_boxes[-2]
    last_top = int(last["ytop"])
    last_bottom = int(last["ybot"])
    previous_top = int(previous["ytop"])
    previous_bottom = int(previous["ybot"])
    last_height = last_top - last_bottom
    if last_height <= 0 or last_bottom >= previous_bottom:
        return False

    overlap = min(last_top, previous_top) - max(last_bottom, previous_bottom)
    if overlap <= 0:
        return False
    overlap_ratio = overlap / last_height

    # Valid adjacent subscript layers can touch or overlap slightly. If the
    # later layer spends a large share of its height inside the previous layer,
    # it still appears collapsed rather than stacked.
    return overlap_ratio >= 0.35


def detect_subscript_insufficient_descent(text: str, glyph_boxes: list[dict[str, object]]) -> bool:
    metrics = stack_metrics(text)
    if int(metrics["subjoined_count"]) < 2 or len(glyph_boxes) < 3:
        return False

    trailing_top_marks = 0
    for ch in reversed(text):
        if ch in TOP_MARKS:
            trailing_top_marks += 1
        else:
            break

    end_idx = len(glyph_boxes) - trailing_top_marks
    stack_boxes = glyph_boxes[:end_idx]
    if len(stack_boxes) < 3:
        return False

    last = stack_boxes[-1]
    previous = stack_boxes[-2]
    last_top = int(last["ytop"])
    previous_top = int(previous["ytop"])
    previous_bottom = int(previous["ybot"])
    previous_height = previous_top - previous_bottom
    if previous_height <= 0:
        return False

    # The next subscript layer should start noticeably below the previous one.
    # If its top is aligned with or above the previous layer, the stack has
    # visually collapsed even if the glyph descends.
    min_descent = 0.20 * previous_height
    return last_top > previous_top - min_descent


def shape_rows(
    fonts: Iterable[FontRow],
    stacks: Iterable[str],
    *,
    test_kind: str,
) -> Iterator[dict[str, object]]:
    stack_list = list(stacks)
    hb_version = hb.version_string()
    for font_row in fonts:
        try:
            shaper = HarfbuzzShaper(font_row)
        except Exception as exc:
            for stack in stack_list:
                metrics = stack_metrics(stack)
                yield base_result_row(font_row, metrics, test_kind) | {
                    "hb_version": hb_version,
                    "ok": False,
                    "support_class": "font_error",
                    "reason": f"font_error:{type(exc).__name__}:{exc}",
                    "notdef_count": 0,
                    "has_dotted_circle": False,
                    "glyph_count": 0,
                    "cluster_count": 0,
                    "glyph_ids": "",
                    "glyph_names": "",
                    "clusters": "",
                    "advances": "",
                    "offsets": "",
                    "bbox": "",
                    "ink_area": 0,
                    "glyph_boxes": "",
                    "placement_warnings": "",
                    "placement_warning_count": 0,
                }
            continue

        for stack in stack_list:
            metrics = stack_metrics(stack)
            shaped = shaper.shape(stack)
            support_class = shaped["support_class"]
            if shaped["ok"]:
                support_class = "normal_tibetan_ok" if test_kind == "normal" else "complex_stack_ok"
            yield base_result_row(font_row, metrics, test_kind) | {
                "hb_version": hb_version,
                **shaped,
                "support_class": support_class,
            }


def base_result_row(font_row: FontRow, metrics: dict[str, object], test_kind: str) -> dict[str, object]:
    return {
        "test_kind": test_kind,
        "basename": font_row.basename,
        "font_path": font_row.font_path_csv,
        "font_path_abs": str(font_row.font_path),
        "ttc_face_index": font_row.ttc_face_index,
        "ttc_face_index_csv": font_row.ttc_face_index_csv,
        "ps_name": font_row.ps_name,
        "skt_ok": font_row.skt_ok,
        "font_size_pt": font_row.font_size_pt,
        "dpi": font_row.dpi,
        **metrics,
    }


class ParquetRowWriter:
    def __init__(self, output_path: Path, *, batch_size: int = 10000):
        self.output_path = output_path
        self.batch_size = batch_size
        self.rows: list[dict[str, object]] = []
        self.writer: pq.ParquetWriter | None = None
        self.count = 0
        output_path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, row: dict[str, object]) -> None:
        self.rows.append(row)
        if len(self.rows) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.output_path, table.schema, compression="zstd")
        else:
            table = table.cast(self.writer.schema)
        self.writer.write_table(table)
        self.count += len(self.rows)
        self.rows = []

    def close(self) -> None:
        self.flush()
        if self.writer is not None:
            self.writer.close()

    def __enter__(self) -> "ParquetRowWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def write_csv_summary(rows: Iterable[dict[str, object]], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return 0
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def summarize_support_parquet(parquet_path: Path) -> list[dict[str, object]]:
    table = pq.read_table(
        parquet_path,
        columns=["test_kind", "basename", "skt_ok", "ok", "reason", "complexity"],
    )
    rows = table.to_pylist()
    grouped: dict[tuple[str, str], dict[str, object]] = {}
    for row in rows:
        key = (row["test_kind"], row["basename"])
        item = grouped.setdefault(
            key,
            {
                "test_kind": row["test_kind"],
                "basename": row["basename"],
                "skt_ok": row["skt_ok"],
                "tested": 0,
                "ok": 0,
                "failed": 0,
                "max_failed_complexity": 0,
                "reasons": {},
            },
        )
        item["tested"] += 1
        if row["ok"]:
            item["ok"] += 1
        else:
            item["failed"] += 1
            item["max_failed_complexity"] = max(
                item["max_failed_complexity"], row.get("complexity") or 0
            )
            reasons = item["reasons"]
            reasons[row["reason"]] = reasons.get(row["reason"], 0) + 1

    out = []
    for item in grouped.values():
        tested = item["tested"] or 1
        item["ok_rate"] = item["ok"] / tested
        item["supported_percent"] = round(100 * item["ok_rate"], 4)
        item["supported_stacks_percent"] = (
            item["supported_percent"] if item["test_kind"] == "stack" else ""
        )
        item["reasons"] = json.dumps(item["reasons"], ensure_ascii=False, sort_keys=True)
        out.append(item)
    return sorted(out, key=lambda row: (row["test_kind"], row["ok_rate"], row["basename"]))


def write_support_matrix_csv(
    parquet_path: Path,
    output_path: Path,
    *,
    test_kind: str = "stack",
) -> int:
    """Write a compact stack/probe x font matrix with 1/0 support values."""
    table = pq.read_table(
        parquet_path,
        columns=["test_kind", "basename", "stack", "codepoints", "ok"],
    )
    rows = [row for row in table.to_pylist() if row["test_kind"] == test_kind]
    if not rows:
        return 0

    fonts = sorted({row["basename"] for row in rows})
    stack_rows: dict[str, dict[str, object]] = {}
    for row in rows:
        item = stack_rows.setdefault(
            row["stack"],
            {"stack": row["stack"], "codepoints": row["codepoints"]},
        )
        item[row["basename"]] = 1 if row["ok"] else 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["stack", "codepoints"] + fonts
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for stack in sorted(stack_rows):
            row = stack_rows[stack]
            for font in fonts:
                row.setdefault(font, "")
            writer.writerow(row)
    return len(stack_rows)


def get_font_tables(font_row: FontRow) -> TTFont:
    if font_row.font_path.suffix.lower() == ".ttc":
        collection = TTCollection(str(font_row.font_path), lazy=True)
        return collection.fonts[font_row.ttc_face_index]
    return TTFont(str(font_row.font_path), fontNumber=0, lazy=True)


def extract_static_features(font_row: FontRow) -> dict[str, object]:
    base = {
        "basename": font_row.basename,
        "font_path": font_row.font_path_csv,
        "font_path_abs": str(font_row.font_path),
        "ttc_face_index": font_row.ttc_face_index,
        "ps_name": font_row.ps_name,
        "skt_ok": font_row.skt_ok,
        "font_sha256": "",
        "read_error": "",
        "tibetan_cmap_count": 0,
        "tibetan_cmap_coverage": 0.0,
        "has_gsub": False,
        "gsub_scripts": "",
        "gsub_features": "",
        "has_gpos": False,
        "gpos_scripts": "",
        "gpos_features": "",
        "has_mark_positioning": False,
        "has_mark_to_mark": False,
    }
    try:
        base["font_sha256"] = font_file_sha256(font_row.font_path)
        tt = get_font_tables(font_row)
        cmap: set[int] = set()
        for table in tt["cmap"].tables if "cmap" in tt else []:
            cmap.update(cp for cp in table.cmap.keys() if TIBETAN_START <= cp <= TIBETAN_END)
        base["tibetan_cmap_count"] = len(cmap)
        base["tibetan_cmap_coverage"] = len(cmap) / (TIBETAN_END - TIBETAN_START + 1)
        add_layout_features(tt, "GSUB", base)
        add_layout_features(tt, "GPOS", base)
        tt.close()
    except Exception as exc:
        base["read_error"] = f"{type(exc).__name__}:{exc}"
    return base


def add_layout_features(tt: TTFont, table_tag: str, row: dict[str, object]) -> None:
    has_key = f"has_{table_tag.lower()}"
    script_key = f"{table_tag.lower()}_scripts"
    feature_key = f"{table_tag.lower()}_features"
    if table_tag not in tt:
        return
    row[has_key] = True
    table = tt[table_tag].table
    scripts = []
    features = []
    try:
        scripts = [record.ScriptTag for record in table.ScriptList.ScriptRecord]
    except Exception:
        scripts = []
    try:
        features = [record.FeatureTag for record in table.FeatureList.FeatureRecord]
    except Exception:
        features = []
    row[script_key] = " ".join(sorted(set(scripts)))
    row[feature_key] = " ".join(sorted(set(features)))
    if table_tag == "GPOS":
        feature_set = set(features)
        row["has_mark_positioning"] = bool(feature_set & {"mark", "mkmk", "abvm", "blwm"})
        row["has_mark_to_mark"] = "mkmk" in feature_set


def hb_view_render(
    font_row: FontRow,
    text: str,
    output_path: Path,
    *,
    hb_view_bin: str = "hb-view",
    margin: int = 32,
) -> tuple[bool, str]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        hb_view_bin,
        "--font-file",
        str(font_row.font_path),
        "--output-file",
        str(output_path),
        "--output-format",
        "png",
        "--font-ptem",
        str(font_row.font_size_pt or 48),
        "--margin",
        str(margin),
        "--script",
        "tibt",
        "--language",
        "bo",
        text,
    ]
    if font_row.ttc_face_index_csv != "":
        cmd.extend(["--face-index", font_row.ttc_face_index_csv])
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return True, ""
    except (OSError, subprocess.CalledProcessError) as exc:
        output = getattr(exc, "output", str(exc))
        return False, output[:500]


def slugify(text: str, *, max_len: int = 48) -> str:
    ascii_text = re.sub(r"[^A-Za-z0-9._-]+", "_", text)
    ascii_text = ascii_text.strip("._-") or hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]
    return ascii_text[:max_len]


def finite_float(value: object, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if math.isfinite(number):
        return number
    return default
