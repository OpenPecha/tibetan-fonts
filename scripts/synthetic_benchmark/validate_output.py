#!/usr/bin/env python3
"""Validate synthetic benchmark outputs and print distribution summaries."""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path

from PIL import Image
import pyarrow.parquet as pq

from synthetic_common import DEFAULT_OUTPUT_DIR


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate synthetic benchmark image/alignment output.")
    parser.add_argument("out_dir", type=Path, nargs="?", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--sample-images", type=int, default=25)
    return parser.parse_args()


def load_alignment_rows(out_dir: Path) -> tuple[list[dict[str, object]], list[Path]]:
    rows: list[dict[str, object]] = []
    parquet_paths = sorted((out_dir / "alignments").glob("*/*/*.parquet"))
    for parquet_path in parquet_paths:
        table = pq.read_table(parquet_path)
        parts = parquet_path.stem.split("-")
        if len(parts) < 2:
            continue
        i_id = parts[0]
        for row in table.to_pylist():
            item = dict(row)
            item["i_id"] = i_id
            rows.append(item)
    return rows, parquet_paths


def main() -> None:
    args = parse_args()
    rows, parquet_paths = load_alignment_rows(args.out_dir)
    if not rows:
        raise SystemExit(f"No alignment parquet files found under {args.out_dir / 'alignments'}")

    missing: list[str] = []
    for row in rows:
        image_path = (
            args.out_dir
            / "images"
            / "W1BCS001"
            / str(row["i_id"])
            / "v001"
            / str(row["img_file_name"])
        )
        if not image_path.exists():
            missing.append(str(image_path))

    widths = Counter()
    heights = Counter()
    modes = Counter()
    for row in rows[: args.sample_images]:
        image_path = (
            args.out_dir
            / "images"
            / "W1BCS001"
            / str(row["i_id"])
            / "v001"
            / str(row["img_file_name"])
        )
        if not image_path.exists():
            continue
        with Image.open(image_path) as img:
            widths[img.width] += 1
            heights[img.height] += 1
            modes[img.mode] += 1

    print(f"alignment rows: {len(rows)}")
    print(f"missing files: {len(missing)}")
    if missing:
        for item in missing[:20]:
            print(f"  {item}")
    print(f"\nsampled widths: {dict(widths)}")
    print(f"sampled heights: {dict(heights)}")
    print(f"sampled modes: {dict(modes)}")
    print(f"\nalignment parquet files: {len(parquet_paths)}")


if __name__ == "__main__":
    main()

