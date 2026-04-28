#!/usr/bin/env python3
"""Build a balanced font/text render plan with whole-page stack coverage checks."""

from __future__ import annotations

import argparse
import csv
import random
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from synthetic_common import (
    DEFAULT_BENCHMARK_CSV,
    DEFAULT_CHUNKS_PARQUET,
    DEFAULT_FONTS_CSV,
    DEFAULT_RENDER_PLAN,
    DEFAULT_SCRIPTS_CSV,
    FontCatalogRow,
    load_font_catalog,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build balanced synthetic benchmark render plan.")
    parser.add_argument("--chunks", type=Path, default=DEFAULT_CHUNKS_PARQUET)
    parser.add_argument("--support-parquet", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_RENDER_PLAN)
    parser.add_argument("--summary-csv", type=Path, default=None)
    parser.add_argument("--benchmark-csv", type=Path, default=DEFAULT_BENCHMARK_CSV)
    parser.add_argument("--scripts-csv", type=Path, default=DEFAULT_SCRIPTS_CSV)
    parser.add_argument("--fonts-csv", type=Path, default=DEFAULT_FONTS_CSV)
    parser.add_argument("--target-images", type=int, default=500_000)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--max-attempt-multiplier", type=int, default=80)
    parser.add_argument("--batch-size", type=int, default=5000)
    return parser.parse_args()


def load_supported_stacks(path: Path) -> dict[str, set[str]]:
    schema = pq.read_schema(path)
    columns = ["test_kind", "basename", "stack", "ok"]
    has_warning_count = "placement_warning_count" in schema.names
    if has_warning_count:
        columns.append("placement_warning_count")
    table = pq.read_table(path, columns=columns)
    supported: dict[str, set[str]] = defaultdict(set)
    for row in table.to_pylist():
        if row["test_kind"] != "stack" or not row["ok"]:
            continue
        if has_warning_count and (row.get("placement_warning_count") or 0) != 0:
            continue
        supported[row["basename"]].add(row["stack"])
    return dict(supported)


def load_chunks(path: Path) -> list[dict[str, object]]:
    table = pq.read_table(path)
    chunks = table.to_pylist()
    for chunk in chunks:
        stack_text = chunk.get("stacks") or ""
        chunk["_stack_set"] = frozenset(str(stack_text).split())
    return chunks


def category_quotas(categories: list[str], target: int) -> dict[str, int]:
    base, rem = divmod(target, len(categories))
    return {category: base + (1 if i < rem else 0) for i, category in enumerate(categories)}


def font_supports_chunk(font: FontCatalogRow, chunk: dict[str, object], supported: dict[str, set[str]]) -> bool:
    stacks = chunk["_stack_set"]
    if not stacks:
        return False
    font_supported = supported.get(font.basename)
    if not font_supported:
        return False
    return stacks.issubset(font_supported)


def interleave_by_script(fonts: list[FontCatalogRow], rng: random.Random) -> list[FontCatalogRow]:
    by_script: dict[str, list[FontCatalogRow]] = defaultdict(list)
    for font in fonts:
        by_script[font.script_id].append(font)
    for group in by_script.values():
        rng.shuffle(group)
    script_ids = list(by_script)
    rng.shuffle(script_ids)
    out: list[FontCatalogRow] = []
    while script_ids:
        next_ids = []
        for script_id in script_ids:
            group = by_script[script_id]
            if group:
                out.append(group.pop())
            if group:
                next_ids.append(script_id)
        script_ids = next_ids
    return out


def make_plan_rows(
    *,
    fonts: list[FontCatalogRow],
    chunks: list[dict[str, object]],
    supported: dict[str, set[str]],
    target_images: int,
    seed: int,
    max_attempt_multiplier: int,
) -> list[dict[str, object]]:
    rng = random.Random(seed)
    categories = sorted({font.script_category for font in fonts if font.script_category})
    quotas = category_quotas(categories, target_images)
    fonts_by_category: dict[str, list[FontCatalogRow]] = defaultdict(list)
    for font in fonts:
        if font.basename in supported and font.script_category:
            fonts_by_category[font.script_category].append(font)

    rows: list[dict[str, object]] = []
    used_font_chunks: set[tuple[str, str]] = set()
    image_id = 1
    chunk_indices = list(range(len(chunks)))
    for category in categories:
        category_fonts = interleave_by_script(fonts_by_category.get(category, []), rng)
        if not category_fonts:
            print(f"WARNING: no supported fonts for category {category!r}")
            continue
        quota = quotas[category]
        attempts = 0
        max_attempts = max(quota * max_attempt_multiplier, quota + 1000)
        font_i = 0
        progress = tqdm(total=quota, desc=f"Plan {category}", unit="image")
        while quota > 0 and attempts < max_attempts:
            attempts += 1
            font = category_fonts[font_i % len(category_fonts)]
            font_i += 1
            chunk = chunks[rng.choice(chunk_indices)]
            key = (font.basename, str(chunk["chunk_id"]))
            if key in used_font_chunks:
                continue
            if not font_supports_chunk(font, chunk, supported):
                continue
            used_font_chunks.add(key)
            rows.append(
                {
                    "image_id": image_id,
                    "chunk_id": chunk["chunk_id"],
                    "bocorpus_row": chunk["bocorpus_row"],
                    "char_start": chunk["char_start"],
                    "char_end": chunk["char_end"],
                    "text": chunk["text"],
                    "char_count": chunk["char_count"],
                    "stack_count": chunk["stack_count"],
                    "unique_stack_count": chunk["unique_stack_count"],
                    "basename": font.basename,
                    "font_file": font.font_file,
                    "font_path": font.font_path,
                    "font_abs_path": str(font.font_abs_path),
                    "ps_name": font.ps_name,
                    "ttc_face_index": font.ttc_face_index,
                    "font_size_pt": font.font_size_pt,
                    "dpi": font.dpi,
                    "skt_ok": font.skt_ok,
                    "script_id": font.script_id,
                    "script_category": font.script_category,
                    "script_type": font.script_type,
                    "script_name": font.script_name,
                }
            )
            image_id += 1
            quota -= 1
            progress.update(1)
        progress.close()
        if quota:
            print(f"WARNING: category {category!r} short by {quota} image(s) after {attempts} attempts")
    return rows


def write_summary(rows: list[dict[str, object]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    counts = {
        "script_category": Counter(row["script_category"] for row in rows),
        "script_id": Counter(row["script_id"] for row in rows),
        "basename": Counter(row["basename"] for row in rows),
    }
    with output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["group", "value", "count"])
        for group, counter in counts.items():
            for value, count in sorted(counter.items()):
                writer.writerow([group, value, count])


def main() -> None:
    args = parse_args()
    if not args.chunks.exists():
        raise SystemExit(f"Missing chunks parquet: {args.chunks}")
    if not args.support_parquet.exists():
        raise SystemExit(f"Missing support parquet: {args.support_parquet}")

    fonts = load_font_catalog(args.benchmark_csv, args.scripts_csv, args.fonts_csv)
    supported = load_supported_stacks(args.support_parquet)
    chunks = load_chunks(args.chunks)
    if not chunks:
        raise SystemExit(f"No chunks found in {args.chunks}")

    rows = make_plan_rows(
        fonts=fonts,
        chunks=chunks,
        supported=supported,
        target_images=args.target_images,
        seed=args.seed,
        max_attempt_multiplier=args.max_attempt_multiplier,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, args.output, compression="zstd")
    summary = args.summary_csv or args.output.with_suffix(".summary.csv")
    write_summary(rows, summary)
    print(f"Wrote {args.output} ({len(rows)} image plan row(s))")
    print(f"Wrote {summary}")


if __name__ == "__main__":
    main()

