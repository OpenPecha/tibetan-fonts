#!/usr/bin/env python3
"""Build page-sized text chunks directly from OpenPecha BoCorpus parquet."""

from __future__ import annotations

import argparse
import ssl
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

from synthetic_common import (
    DEFAULT_BOCORPUS_PARQUET,
    DEFAULT_CHUNKS_PARQUET,
    chunk_units,
    iter_bocorpus_texts_parquet,
    is_tibetan_text_with_spaces,
    looks_like_toc_chunk,
    normalize_bocorpus_text,
    split_units,
    stable_chunk_id,
    stack_difficulty_score,
    tokenize_tibetan_stacks,
)

BOCORPUS_HF_RELPATH = "datasets/openpecha/BoCorpus/resolve/main/bo_corpus.parquet"
BOCORPUS_HF_URL = f"https://huggingface.co/{BOCORPUS_HF_RELPATH}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create BoCorpus page-sized chunks.")
    parser.add_argument("--bocorpus-parquet", type=Path, default=DEFAULT_BOCORPUS_PARQUET)
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--output", type=Path, default=DEFAULT_CHUNKS_PARQUET)
    parser.add_argument("--target-chars", type=int, default=1300)
    parser.add_argument("--min-chars", type=int, default=900)
    parser.add_argument("--max-chars", type=int, default=1800)
    parser.add_argument("--limit-rows", type=int, default=None)
    parser.add_argument("--limit-chunks", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=5000)
    return parser.parse_args()


def download_bocorpus_parquet(dest: Path) -> None:
    """Download BoCorpus parquet from Hugging Face, replacing dest atomically."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    if tmp.exists():
        tmp.unlink()

    ctx = ssl.create_default_context()
    req = urllib.request.Request(
        BOCORPUS_HF_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; tibetan-fonts/synthetic_benchmark)"},
    )
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=60) as response, tmp.open("wb") as f:
            total = response.length or 0
            with tqdm(
                total=total or None,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc="Download BoCorpus",
                file=sys.stderr,
            ) as progress:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    progress.update(len(chunk))
    except (urllib.error.URLError, OSError) as e:
        if tmp.exists():
            tmp.unlink()
        raise SystemExit(
            f"Failed to download {BOCORPUS_HF_URL}: {e}\n"
            "Pass --bocorpus-parquet PATH to use a local BoCorpus parquet."
        ) from e
    tmp.replace(dest)
    sys.stderr.write(f"Wrote {dest} ({dest.stat().st_size // (1024 * 1024)} MiB)\n")


def ensure_bocorpus_parquet(path: Path, *, force_download: bool) -> Path:
    if path.exists() and not force_download:
        return path
    if path.exists() and force_download:
        path.unlink()
    sys.stderr.write(f"Downloading BoCorpus parquet to {path}\n")
    download_bocorpus_parquet(path)
    return path


def main() -> None:
    args = parse_args()
    ensure_bocorpus_parquet(args.bocorpus_parquet, force_download=args.force_download)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    writer: pq.ParquetWriter | None = None
    rows: list[dict[str, object]] = []
    total = 0

    def flush() -> None:
        nonlocal writer, rows
        if not rows:
            return
        table = pa.Table.from_pylist(rows)
        if writer is None:
            writer = pq.ParquetWriter(args.output, table.schema, compression="zstd")
        else:
            table = table.cast(writer.schema)
        writer.write_table(table)
        rows = []

    parquet_rows = pq.ParquetFile(str(args.bocorpus_parquet)).metadata.num_rows
    progress_total = (
        min(parquet_rows, args.limit_rows) if args.limit_rows is not None else parquet_rows
    )

    try:
        progress = tqdm(
            iter_bocorpus_texts_parquet(args.bocorpus_parquet, limit_rows=args.limit_rows),
            desc="Chunk BoCorpus",
            unit="row",
            total=progress_total,
        )
        for row_index, raw_text in progress:
            text = normalize_bocorpus_text(raw_text)
            units = split_units(text)
            if not units:
                continue
            for chunk_text, char_start in chunk_units(
                units,
                target_chars=args.target_chars,
                min_chars=args.min_chars,
                max_chars=args.max_chars,
            ):
                if looks_like_toc_chunk(chunk_text):
                    continue
                if not is_tibetan_text_with_spaces(chunk_text):
                    continue
                tokenized_stacks = tokenize_tibetan_stacks(chunk_text)
                stacks = sorted(set(tokenized_stacks))
                if not stacks:
                    continue
                char_end = char_start + len(chunk_text)
                rows.append(
                    {
                        "chunk_id": stable_chunk_id(row_index, char_start, chunk_text),
                        "bocorpus_row": row_index,
                        "char_start": char_start,
                        "char_end": char_end,
                        "text": chunk_text,
                        "char_count": len(chunk_text),
                        "stack_count": len(tokenized_stacks),
                        "unique_stack_count": len(stacks),
                        "stacks": " ".join(stacks),
                        "stack_difficulty_score": stack_difficulty_score(chunk_text),
                    }
                )
                total += 1
                progress.set_postfix(chunks=total)
                if len(rows) >= args.batch_size:
                    flush()
                if args.limit_chunks is not None and total >= args.limit_chunks:
                    flush()
                    return
        flush()
    finally:
        if writer is not None:
            writer.close()
    print(f"Wrote {args.output} ({total} chunk(s))")


if __name__ == "__main__":
    main()

