#!/usr/bin/env python3
"""
Build Tibetan stack frequency table from the OpenPecha BoCorpus (Hugging Face).

Uses botok: normalize_unicode, normalize_graphical, then tokenize_in_stacks.
Only stacks whose every code point is in the Tibetan block (U+0F00–U+0FFF) are
counted; ASCII, spaces, and other non-Tibetan code points drop the whole stack.

A third CSV column, ``hunspell_bo``, is 1 iff the stack appears when segmenting (with
botok, after the same Unicode/graphical normalization as above) at least one **valid
syllable** licensed by [hunspell-bo](https://github.com/eroux/hunspell-bo/) ``bo.aff`` /
``bo.dic``, **or** the stack is made only of hardcoded Tibetan punctuation / marks /
digits (``coverage_common`` ``HARDCODED_HUNSPELL_BO_EXTRA_CHARS``), since hunspell has no
syllable entries for those. Syllables are enumerated by expanding ``bo.dic`` stems
through the SFX rules (spylls reads the files; we BFS affix combinations and keep strings
where ``lookup`` is true). By default SFX ``C`` and ``S`` expansion is skipped for speed
(see ``--full-sfx``).

Requires: botok, pyarrow, tqdm, spylls (and network on first run to download the Parquet
  and/or hunspell-bo files, unless paths are provided). Optional: ``pip install datasets``
  for the HuggingFace cache via ``--use-datasets``.
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

from coverage_common import (
    DEFAULT_STACKS_CSV,
    in_tibetan_block,
    is_hunspell_bo_extra_stack,
)
from hunspell_bo_stacks import build_valid_stack_set

SCRIPT_DIR = Path(__file__).resolve().parent
BOCORPUS_HF_RELPATH = "datasets/openpecha/BoCorpus/resolve/main/bo_corpus.parquet"
BOCORPUS_HF = f"https://huggingface.co/{BOCORPUS_HF_RELPATH}"
DEFAULT_CACHE_PARQUET = SCRIPT_DIR / ".cache" / "bocorpus" / "bo_corpus.parquet"
# https://github.com/eroux/hunspell-bo/
HUNSPELL_BO_RAW = "https://raw.githubusercontent.com/eroux/hunspell-bo/master"
DEFAULT_HUNSPELL_BO_CACHE = SCRIPT_DIR / ".cache" / "hunspell-bo"


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


def _download_hunspell_bo_artifact(
    relpath: str, dest: Path, user_agent: str
) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    url = f"{HUNSPELL_BO_RAW.rstrip('/')}/{relpath}"
    tmp = dest.with_name(dest.name + ".part")
    if tmp.exists():
        tmp.unlink()
    ctx = ssl.create_default_context()
    req = urllib.request.Request(
        url,
        headers={"User-Agent": user_agent},
    )
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=60) as r, open(
            tmp, "wb"
        ) as f:
            f.write(r.read())
    except (urllib.error.URLError, OSError) as e:
        if tmp.exists():
            tmp.unlink()
        raise SystemExit(
            f"Failed to download {url}: {e}\n"
            " Or pass a local hunspell-bo directory: --hunspell-bo-dir PATH"
        ) from e
    tmp.replace(dest)


def ensure_hunspell_bo_aff_dic(
    cache_dir: Path, *, force_download: bool, user_agent: str
) -> None:
    """Download bo.aff and bo.dic from eroux/hunspell-bo if missing."""
    for name in ("bo.aff", "bo.dic"):
        dest = cache_dir / name
        if dest.is_file() and dest.stat().st_size > 0 and not force_download:
            continue
        sys.stderr.write(f"Downloading hunspell-bo {name}…\n")
        _download_hunspell_bo_artifact(name, dest, user_agent)


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


def write_stacks_csv(
    path: Path,
    counts: Counter[str],
    *,
    valid_stacks: frozenset[str],
    show_progress: bool,
) -> None:
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    it: Iterable[tuple[str, int]] = rows
    if show_progress:
        it = tqdm(
            rows,
            desc="Tag stacks (hunspell-bo syllable → stacks)",
            unit="stack",
            file=sys.stderr,
        )
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["stack", "nb_occurences", "hunspell_bo"])
        for stack, n in it:
            ok = (
                1
                if (stack in valid_stacks or is_hunspell_bo_extra_stack(stack))
                else 0
            )
            w.writerow([stack, n, ok])


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
    ap.add_argument(
        "--hunspell-bo-dir",
        type=Path,
        default=None,
        help=f"Directory containing bo.aff and bo.dic (default: download to {DEFAULT_HUNSPELL_BO_CACHE})",
    )
    ap.add_argument(
        "--force-download-hunspell",
        action="store_true",
        help="Re-download bo.aff and bo.dic from GitHub",
    )
    ap.add_argument(
        "--no-lookup-progress",
        action="store_true",
        help="Disable tqdm while building the valid-stack set and writing CSV",
    )
    ap.add_argument(
        "--full-sfx",
        action="store_true",
        help="Expand SFX C and S when enumerating hunspell-bo syllables (slower; more stacks marked 1)",
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
    h_dir = args.hunspell_bo_dir
    if h_dir is None:
        d = os.environ.get("HUNSPELL_BO_DIR")
        h_dir = Path(d) if d else DEFAULT_HUNSPELL_BO_CACHE
    try:
        ensure_hunspell_bo_aff_dic(
            h_dir,
            force_download=args.force_download_hunspell,
            user_agent="Mozilla/5.0 (compatible; tibetan-fonts/get_stacks_from_corpus)",
        )
        valid_stacks = build_valid_stack_set(
            h_dir,
            ignore_c_s_morphology=not args.full_sfx,
            show_progress=not args.no_lookup_progress,
        )
    except ImportError as e:
        raise SystemExit(
            "The hunspell_bo column requires: pip install spylls\n"
        ) from e
    write_stacks_csv(
        args.output,
        counts,
        valid_stacks=valid_stacks,
        show_progress=not args.no_lookup_progress,
    )
    sys.stderr.write(
        f"Wrote {args.output} ({len(counts)} distinct stacks, {sum(counts.values())} total; "
        f"hunspell_bo = stack in syllables from {h_dir / 'bo.aff'}+{h_dir / 'bo.dic'}, "
        f"SFX C/S expansion {'on' if args.full_sfx else 'off'})\n"
    )


if __name__ == "__main__":
    main()
