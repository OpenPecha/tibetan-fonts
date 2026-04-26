#!/usr/bin/env python3
"""
Build Tibetan stack frequency table from the OpenPecha BoCorpus (Hugging Face).

Uses botok: normalize_unicode, normalize_graphical, then tokenize_in_stacks.
Only stacks whose every code point is in the Tibetan block (U+0F00–U+0FFF) are
counted; ASCII, spaces, and other non-Tibetan code points drop the whole stack.

Requires: botok, pyarrow, tqdm (and network on first run to download the Parquet, unless
  --parquet is passed). Optional: pip install datasets to use the HuggingFace cache
  via --use-datasets.
"""

from __future__ import annotations

import argparse
import os
import ssl
import sys
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator

from tqdm import tqdm

if TYPE_CHECKING:
    import pyarrow.parquet as pq  # noqa: F401

from coverage_common import DEFAULT_STACKS_CSV, in_tibetan_block

SCRIPT_DIR = Path(__file__).resolve().parent
BOCORPUS_HF_RELPATH = "datasets/openpecha/BoCorpus/resolve/main/bo_corpus.parquet"
BOCORPUS_HF = f"https://huggingface.co/{BOCORPUS_HF_RELPATH}"
DEFAULT_CACHE_PARQUET = SCRIPT_DIR / ".cache" / "bocorpus" / "bo_corpus.parquet"


def _default_parquet_path() -> Path:
    d = os.environ.get("BOCORPUS_STACKS_PARQUET")
    if d:
        return Path(d)
    return DEFAULT_CACHE_PARQUET


def download_bocorpus_parquet(dest: Path) -> None:
    """Stream-download BoCorpus single Parquet (LFS) to dest (atomic replace)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    if tmp.exists():
        tmp.unlink()

    ctx = ssl.create_default_context()
    req = urllib.request.Request(
        BOCORPUS_HF,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; tibetan-fonts/get_stacks_from_corpus)"
        },
    )
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
            total = r.length
            n = 0
            with open(tmp, "wb") as f:
                while True:
                    chunk = r.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    n += len(chunk)
                    if total and total > 0 and n % (50 * 1024 * 1024) < 1024 * 1024:
                        sys.stderr.write(
                            f"  downloaded {n // (1024 * 1024)} MiB / {total // (1024 * 1024)} MiB\n"
                        )
    except (urllib.error.URLError, OSError) as e:
        if tmp.exists():
            tmp.unlink()
        raise SystemExit(
            f"Failed to download {BOCORPUS_HF}: {e}\n"
            " Pass --parquet PATH to a local bo_corpus.parquet, or install datasets and use --use-datasets."
        ) from e

    tmp.replace(dest)
    sys.stderr.write(f"Wrote {dest} ({dest.stat().st_size // (1024 * 1024)} MiB)\n")


def ensure_parquet(path: Path, force_download: bool) -> Path:
    if path.exists() and not force_download:
        return path
    if path.exists() and force_download:
        path.unlink()
    download_bocorpus_parquet(path)
    return path


def iter_bocorpus_texts_use_datasets() -> Iterator[str]:
    from datasets import load_dataset

    ds = load_dataset("openpecha/BoCorpus", split="train", trust_remote_code=False)
    for ex in ds:
        t = ex.get("text")
        if t is None:
            continue
        if not isinstance(t, str):
            t = str(t)
        yield t


def iter_bocorpus_texts_parquet(path: Path) -> Iterator[str]:
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(str(path))
    for batch in pf.iter_batches(batch_size=32, columns=["text"]):
        for text in batch.column(0).to_pylist():
            if text is None:
                continue
            if not isinstance(text, str):
                text = str(text)
            yield text


def iter_bocorpus_texts(
    parquet_path: Path, use_datasets: bool, limit_rows: int | None
) -> Iterator[str]:
    if use_datasets:
        it: Iterator[str] = iter_bocorpus_texts_use_datasets()
    else:
        it = iter_bocorpus_texts_parquet(parquet_path)

    n = 0
    for t in it:
        if limit_rows is not None and n >= limit_rows:
            break
        n += 1
        yield t


def is_pure_tibetan_stack(stack: str) -> bool:
    return bool(stack) and all(in_tibetan_block(c) for c in stack)


def count_stacks_from_texts(
    texts: Iterable[str], *, total: int | None = None
) -> Counter[str]:
    from botok import normalize_unicode, tokenize_in_stacks
    from botok.utils.lenient_normalization import normalize_graphical

    counts: Counter[str] = Counter()
    it = tqdm(
        texts,
        desc="Counting stacks",
        unit="doc",
        total=total,
        file=sys.stderr,
    )
    for text in it:
        s = normalize_unicode(text)
        s = normalize_graphical(s)
        for stack in tokenize_in_stacks(s):
            if not is_pure_tibetan_stack(stack):
                continue
            counts[stack] += 1
    return counts


def write_stacks_csv(path: Path, counts: Counter[str]) -> None:
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["stack", "nb_occurences"])
        for stack, n in rows:
            w.writerow([stack, n])


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Count Tibetan stack occurrences in OpenPecha BoCorpus (botok stacks)."
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_STACKS_CSV,
        help=f"Output CSV (default: {DEFAULT_STACKS_CSV}, same as build_support_dataset --stacks default)",
    )
    ap.add_argument(
        "--parquet",
        type=Path,
        default=None,
        help="Path to bo_corpus.parquet; created on first run if missing (default: "
        f"env BOCORPUS_STACKS_PARQUET or {DEFAULT_CACHE_PARQUET})",
    )
    ap.add_argument(
        "--use-datasets",
        action="store_true",
        help="Load with HuggingFace `datasets` (uses its cache); no Parquet file needed",
    )
    ap.add_argument(
        "--force-download",
        action="store_true",
        help="Re-download Parquet (only when not using --use-datasets)",
    )
    ap.add_argument(
        "--limit-rows",
        type=int,
        default=None,
        help="Process only the first N corpus rows (for testing)",
    )
    args = ap.parse_args()

    if args.use_datasets:
        try:
            import datasets  # noqa: F401
        except ImportError as e:
            raise SystemExit(
                "This mode requires: pip install datasets\n"
            ) from e
        if args.parquet is not None:
            sys.stderr.write("Warning: --parquet ignored with --use-datasets\n")
        dummy_parquet = Path()
    else:
        parquet_path = args.parquet if args.parquet is not None else _default_parquet_path()
        if not parquet_path.exists() or args.force_download:
            ensure_parquet(parquet_path, force_download=args.force_download)
        dummy_parquet = parquet_path

    total_docs: int | None = None
    if not args.use_datasets and dummy_parquet.is_file():
        import pyarrow.parquet as pq

        nr = pq.ParquetFile(str(dummy_parquet)).metadata.num_rows
        total_docs = min(nr, args.limit_rows) if args.limit_rows is not None else nr
    elif args.use_datasets and args.limit_rows is not None:
        total_docs = args.limit_rows

    texts = iter_bocorpus_texts(
        dummy_parquet,
        use_datasets=args.use_datasets,
        limit_rows=args.limit_rows,
    )
    counts = count_stacks_from_texts(
        texts,
        total=total_docs,
    )
    write_stacks_csv(args.output, counts)
    sys.stderr.write(
        f"Wrote {args.output} ({len(counts)} distinct stacks, {sum(counts.values())} total)\n"
    )


if __name__ == "__main__":
    main()
