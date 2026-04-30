#!/usr/bin/env python3
"""Create an embedded-image training subset from a rendered benchmark export."""

from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from synthetic_common import DEFAULT_OUTPUT_DIR

BENCHMARK_VERSION = "202604"
DATASET_ID = "BECSynthetic_01"
SUBSET_ID = "bec_synthetic_01_full_embedded"
W_ID = "W1BCS001"
I_VERSION = "v001"
VE_VERSION = "v001"
MODE = "ptt"
DEFAULT_S3_ROOT = "s3://bec.bdrc.io/ocr_benchmark"
DEFAULT_MAX_SHARD_BYTES = 450 * 1024 * 1024
OPTIONAL_FIELDS = [
    "pagination",
    "font_name",
    "script_8",
    "etext_source",
    "stack_difficulty_score",
    "suggested_split",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create embedded-image subset parquet shards.")
    parser.add_argument("out_dir", type=Path, nargs="?", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--subset-id", default=SUBSET_ID)
    parser.add_argument("--s3-root", default=DEFAULT_S3_ROOT)
    parser.add_argument("--seed", type=int, default=20260430)
    parser.add_argument("--max-shard-bytes", type=int, default=DEFAULT_MAX_SHARD_BYTES)
    parser.add_argument("--author", default="BDRC")
    parser.add_argument("--force", action="store_true", help="Replace an existing subset directory.")
    return parser.parse_args()


def parse_alignment_path(path: Path) -> tuple[str, str, str]:
    stem = path.stem
    mode = MODE
    if stem.endswith(f"_{MODE}"):
        stem = stem[: -len(f"_{MODE}")]
    if "-" not in stem:
        raise ValueError(f"Cannot parse alignment parquet name: {path.name}")
    i_id, ve_id = stem.split("-", 1)
    return i_id, ve_id, mode


def load_volume_metadata(dataset_root: Path) -> dict[str, dict[str, str]]:
    path = dataset_root / "catalog_volumes.csv"
    volumes: dict[str, dict[str, str]] = {}
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            volumes[str(row["vol_id"])] = row
    return volumes


def alignment_paths(out_dir: Path) -> list[Path]:
    dataset_root = out_dir / "alignments" / BENCHMARK_VERSION / DATASET_ID
    return sorted(dataset_root.glob(f"*-*_{MODE}.parquet"))


def image_path_for_row(out_dir: Path, i_id: str, img_file_name: str) -> Path:
    return out_dir / "images" / W_ID / i_id / I_VERSION / img_file_name


def iter_alignment_metadata(out_dir: Path) -> Iterator[dict[str, object]]:
    dataset_root = out_dir / "alignments" / BENCHMARK_VERSION / DATASET_ID
    volumes = load_volume_metadata(dataset_root)
    for parquet_path in alignment_paths(out_dir):
        i_id, ve_id, mode = parse_alignment_path(parquet_path)
        i_meta = volumes.get(i_id, {})
        ve_meta = volumes.get(ve_id, {})
        table = pq.read_table(parquet_path)
        for row in table.to_pylist():
            img_file_name = str(row["img_file_name"])
            yield {
                "image_path": image_path_for_row(out_dir, i_id, img_file_name),
                "img_file_name": img_file_name,
                "i_id": i_id,
                "ve_id": ve_id,
                "mode": mode,
                "mw_id": i_meta.get("mw_id") or ve_meta.get("mw_id") or "",
                "line_breaks": i_meta.get("line_breaks") or ve_meta.get("line_breaks") or "",
                "access": i_meta.get("access") or ve_meta.get("access") or "",
                "technology": i_meta.get("technology") or ve_meta.get("technology") or "",
                "script": i_meta.get("script") or ve_meta.get("script") or "",
                **row,
            }


def split_rows(rows: list[dict[str, object]], seed: int) -> dict[str, list[dict[str, object]]]:
    """Group rows by split and deterministically shuffle each split before sharding."""
    by_split: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        split = str(row.get("suggested_split") or "data")
        by_split[split].append(row)
    for split, items in sorted(by_split.items()):
        rng = random.Random(f"{seed}:{split}")
        rng.shuffle(items)
        print(f"Shuffled {len(items)} row(s) in split {split!r} with seed {seed}:{split}")
    return dict(by_split)


def subset_row(row: dict[str, object]) -> dict[str, object]:
    image_path = Path(str(row["image_path"]))
    img_file_name = str(row["img_file_name"])
    ve_id = str(row["ve_id"])
    item = {
        "image_bytes": image_path.read_bytes(),
        "id": (
            f"{BENCHMARK_VERSION}:{DATASET_ID}:"
            f"IE1BCS001:{ve_id}:{VE_VERSION}:{img_file_name}"
        ),
        "transcription": row.get("transcription") or "",
        "mw_id": row.get("mw_id") or "",
        "mode": row.get("mode") or MODE,
        "line_breaks": row.get("line_breaks") or "",
        "access": row.get("access") or "",
        "technology": row.get("technology") or "",
        "script": row.get("script") or "",
    }
    for field in OPTIONAL_FIELDS:
        item[field] = row.get(field)
    return item


def estimate_row_bytes(row: dict[str, object]) -> int:
    size = len(row["image_bytes"])
    for value in row.values():
        if isinstance(value, str):
            size += len(value.encode("utf-8"))
        elif isinstance(value, (int, float)):
            size += 16
        elif value is None:
            size += 1
    return size


def write_shard(path: Path, rows: list[dict[str, object]]) -> int:
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path, compression="zstd")
    return path.stat().st_size


def write_split_shards(
    subset_root: Path,
    split: str,
    rows: list[dict[str, object]],
    *,
    max_shard_bytes: int,
) -> tuple[list[Path], int]:
    tmp_paths: list[Path] = []
    current_rows: list[dict[str, object]] = []
    current_bytes = 0

    def flush() -> None:
        nonlocal current_rows, current_bytes
        if not current_rows:
            return
        tmp_path = subset_root / f"{split}-{len(tmp_paths):05d}.tmp.parquet"
        write_shard(tmp_path, current_rows)
        tmp_paths.append(tmp_path)
        current_rows = []
        current_bytes = 0

    for meta_row in tqdm(rows, desc=f"Write {split}", unit="image"):
        image_path = Path(str(meta_row["image_path"]))
        if not image_path.exists():
            raise FileNotFoundError(f"Missing image for subset row: {image_path}")
        item = subset_row(meta_row)
        item_bytes = estimate_row_bytes(item)
        if current_rows and current_bytes + item_bytes > max_shard_bytes:
            flush()
        current_rows.append(item)
        current_bytes += item_bytes
    flush()

    final_paths: list[Path] = []
    total_shards = len(tmp_paths)
    for i, tmp_path in enumerate(tmp_paths):
        final_path = subset_root / f"{split}-{i:05d}-of-{total_shards:05d}.parquet"
        tmp_path.replace(final_path)
        final_paths.append(final_path)
    return final_paths, sum(path.stat().st_size for path in final_paths)


def write_catalog(subsets_root: Path, subset_id: str) -> None:
    subsets_root.mkdir(parents=True, exist_ok=True)
    with (subsets_root / "catalog.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["subset_id"])
        writer.writeheader()
        writer.writerow({"subset_id": subset_id})


def write_readme(subset_root: Path, subset_id: str, split_counts: dict[str, int]) -> None:
    counts = "\n".join(f"- `{split}`: {count:,} images" for split, count in sorted(split_counts.items()))
    subset_root.joinpath("README.md").write_text(
        f"# {subset_id}\n\n"
        "This is the full embedded-image subset of the BEC Synthetic Tibetan OCR "
        "Benchmark, created by the Buddhist Digital Resource Center (BDRC) for the "
        "BDRC Etext Corpus project. It is derived from page-level benchmark alignments "
        "generated from the [OpenPecha BoCorpus](https://huggingface.co/datasets/openpecha/BoCorpus) "
        "and the fonts/code in the [OpenPecha Tibetan Fonts repository](https://github.com/openpecha/tibetan-fonts).\n\n"
        "## Contents\n\n"
        "Each row contains JPEG `image_bytes`, the rendered Tibetan transcription, routing "
        "metadata (`mw_id`, `mode`, `line_breaks`, `access`, `technology`, `script`), and "
        "synthetic metadata (`pagination`, `font_name`, `script_8`, `etext_source`, "
        "`stack_difficulty_score`, `suggested_split`). The subset is sharded by split in "
        "Hugging Face-compatible parquet files.\n\n"
        "## Construction\n\n"
        "BoCorpus text was normalized, chunked into page-sized passages, and tokenized into "
        "Tibetan stacks. Candidate font/chunk pairs were filtered using font-specific stack "
        "support data from HarfBuzz shaping and conservative geometric placement checks. "
        "Accepted pages were rendered with LuaLaTeX/fontspec using HarfBuzz and rasterized "
        "as grayscale JPEGs. Transcriptions preserve rendered line breaks.\n\n"
        "## Splits\n\n"
        "Splits are taken from the benchmark alignment metadata. The planning process aims "
        "to balance `script_8`, keep `uchen` and `ume` in separate image volumes, and avoid "
        "font overlap between `train`, `val`, and `test`. Chunks are considered from highest "
        "to lowest stack difficulty, and chunk reuse is kept below 10% and confined to one split.\n\n"
        "## Split Counts\n\n"
        f"{counts}\n\n"
        "## Rights\n\n"
        "The source texts are considered public domain, and the synthetic images in this "
        "subset are also public domain.\n\n"
        "## How to cite\n\n"
        "Please cite: Buddhist Digital Resource Center, BDRC Etext Corpus project, "
        "BEC Synthetic Tibetan OCR Benchmark. Please also acknowledge the OpenPecha "
        "BoCorpus and the OpenPecha Tibetan Fonts repository.\n",
        encoding="utf-8",
    )


def write_subset_info(
    subset_root: Path,
    *,
    subset_id: str,
    author: str,
    nb_images: int,
    parquet_total_size: int,
    shard_paths: dict[str, list[Path]],
    s3_root: str,
    seed: int,
) -> None:
    data_files = {
        split: f"{s3_root.rstrip('/')}/subsets/{subset_id}/{split}-*.parquet"
        for split in sorted(shard_paths)
    }
    info = {
        "description": "Full synthetic BoCorpus Tibetan OCR benchmark subset with embedded JPEG image bytes.",
        "creation_date": date.today().isoformat(),
        "author": author,
        "nb_images": nb_images,
        "parquet_total_size": parquet_total_size,
        "image_format": "image_bytes",
        "extra_fields": OPTIONAL_FIELDS,
        "shuffle": {
            "method": "deterministic random shuffle within each suggested_split before sharding",
            "seed": seed,
        },
        "data_files": data_files,
    }
    subset_root.joinpath("subset_info.json").write_text(
        json.dumps(info, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    dataset_root = args.out_dir / "alignments" / BENCHMARK_VERSION / DATASET_ID
    if not dataset_root.exists():
        raise SystemExit(f"Missing rendered alignment dataset: {dataset_root}")

    subsets_root = args.out_dir / "subsets"
    subset_root = subsets_root / args.subset_id
    if subset_root.exists():
        if not args.force:
            raise SystemExit(f"Subset already exists, pass --force to replace: {subset_root}")
        shutil.rmtree(subset_root)
    subset_root.mkdir(parents=True, exist_ok=True)

    rows = list(tqdm(iter_alignment_metadata(args.out_dir), desc="Load alignment metadata", unit="row"))
    if not rows:
        raise SystemExit(f"No alignment rows found under {dataset_root}")

    split_map = split_rows(rows, args.seed)
    shard_paths: dict[str, list[Path]] = {}
    parquet_total_size = 0
    for split in sorted(split_map):
        paths, size = write_split_shards(
            subset_root,
            split,
            split_map[split],
            max_shard_bytes=args.max_shard_bytes,
        )
        shard_paths[split] = paths
        parquet_total_size += size

    split_counts = {split: len(items) for split, items in split_map.items()}
    write_catalog(subsets_root, args.subset_id)
    write_readme(subset_root, args.subset_id, split_counts)
    write_subset_info(
        subset_root,
        subset_id=args.subset_id,
        author=args.author,
        nb_images=sum(split_counts.values()),
        parquet_total_size=parquet_total_size,
        shard_paths=shard_paths,
        s3_root=args.s3_root,
        seed=args.seed,
    )
    print(f"Wrote subset: {subset_root}")
    print(f"Images: {sum(split_counts.values())}")
    print(f"Parquet total size: {parquet_total_size} bytes")


if __name__ == "__main__":
    main()
