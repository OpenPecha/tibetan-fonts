#!/usr/bin/env python3
"""Build Parquet support data for Tibetan font/stack coverage."""

from __future__ import annotations

import argparse
from pathlib import Path

from tqdm import tqdm

from coverage_common import (
    DEFAULT_FONTS_CSV,
    DEFAULT_OUT_DIR,
    DEFAULT_STACKS_CSV,
    LATIN_DIGIT_PUNCT_PROBES,
    NORMAL_TIBETAN_PROBES,
    ParquetRowWriter,
    load_font_rows,
    read_probe_lines,
    read_stacks_path,
    select_font_rows,
    shape_rows,
    summarize_support_parquet,
    write_csv_summary,
    write_support_matrix_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Shape Tibetan stacks with every selected font and write Parquet evidence."
    )
    parser.add_argument("--fonts-csv", type=Path, default=DEFAULT_FONTS_CSV)
    parser.add_argument(
        "--stacks",
        type=Path,
        help=(
            "Stack probes: one NFD stack per line, or bocorpus_stacks.csv from "
            f"get_stacks_from_corpus.py. Default for --mode stacks/both: {DEFAULT_STACKS_CSV}"
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("stacks", "normal", "latin", "both"),
        default="both",
        help="Which probe set to run. 'normal' and 'latin' do not require --stacks.",
    )
    parser.add_argument("--normal-probes", type=Path, help="Optional newline-delimited normal probes.")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--output", type=Path, help="Output Parquet file.")
    parser.add_argument("--summary-csv", type=Path, help="Output CSV summary path.")
    parser.add_argument(
        "--matrix-csv",
        type=Path,
        help="Optional stack x font 1/0 CSV. Defaults next to output when stack mode is run.",
    )
    parser.add_argument(
        "--no-matrix-csv",
        action="store_true",
        help="Do not write the derived stack x font matrix CSV.",
    )
    parser.add_argument("--limit-stacks", type=int)
    parser.add_argument("--limit-fonts", type=int)
    parser.add_argument("--font", action="append", dest="fonts", help="Restrict to basename; repeatable.")
    parser.add_argument("--skt-ok", type=int, choices=(0, 1), help="Restrict by existing skt_ok flag.")
    parser.add_argument("--batch-size", type=int, default=10000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode in {"stacks", "both"}:
        stacks_path = args.stacks if args.stacks is not None else DEFAULT_STACKS_CSV
        if not stacks_path.is_file():
            raise SystemExit(
                f"Stack list not found: {stacks_path}\n"
                "Generate it with: python get_stacks_from_corpus.py\n"
                "Or pass an explicit file: --stacks /path/to/stacks.txt"
            )
    else:
        stacks_path = None

    fonts = select_font_rows(
        load_font_rows(args.fonts_csv),
        basenames=set(args.fonts) if args.fonts else None,
        skt_ok=args.skt_ok,
        limit=args.limit_fonts,
    )
    if not fonts:
        raise SystemExit("No font rows selected.")

    output = args.output or (args.out_dir / "stack_support.parquet")
    summary_csv = args.summary_csv or output.with_suffix(".summary.csv")
    matrix_csv = args.matrix_csv or output.with_suffix(".matrix.csv")

    jobs: list[tuple[str, list[str]]] = []
    if args.mode in {"normal", "both"}:
        normal = (
            read_probe_lines(args.normal_probes, limit=args.limit_stacks)
            if args.normal_probes
            else NORMAL_TIBETAN_PROBES[: args.limit_stacks]
        )
        jobs.append(("normal", normal))
    if args.mode in {"latin", "both"}:
        jobs.append(("latin", LATIN_DIGIT_PUNCT_PROBES[: args.limit_stacks]))
    if args.mode in {"stacks", "both"}:
        assert stacks_path is not None
        jobs.append(
            ("stack", read_stacks_path(stacks_path, limit=args.limit_stacks))
        )

    with ParquetRowWriter(output, batch_size=args.batch_size) as writer:
        for test_kind, probes in jobs:
            total = len(fonts) * len(probes)
            print(f"Testing {len(fonts)} font(s) x {len(probes)} {test_kind} probe(s)")
            rows = shape_rows(fonts, probes, test_kind=test_kind)
            for row in tqdm(rows, total=total, unit="row", desc=f"{test_kind} shaping"):
                writer.write(row)

    print("Writing summary CSV...")
    count = write_csv_summary(summarize_support_parquet(output), summary_csv)
    print(f"Wrote {output}")
    print(f"Wrote {summary_csv} ({count} font summaries)")
    if args.mode in {"stacks", "both"} and not args.no_matrix_csv:
        print("Writing stack x font matrix CSV...")
        matrix_count = write_support_matrix_csv(output, matrix_csv, test_kind="stack")
        print(f"Wrote {matrix_csv} ({matrix_count} stack rows)")


if __name__ == "__main__":
    main()
