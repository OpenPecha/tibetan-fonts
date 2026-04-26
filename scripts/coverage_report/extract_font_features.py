#!/usr/bin/env python3
"""Extract static cmap/GSUB/GPOS evidence for Tibetan fonts."""

from __future__ import annotations

import argparse
from pathlib import Path

from coverage_common import (
    DEFAULT_FONTS_CSV,
    DEFAULT_OUT_DIR,
    ParquetRowWriter,
    extract_static_features,
    load_font_rows,
    select_font_rows,
    write_csv_summary,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write static font feature evidence to Parquet.")
    parser.add_argument("--fonts-csv", type=Path, default=DEFAULT_FONTS_CSV)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--output", type=Path, help="Output Parquet file.")
    parser.add_argument("--summary-csv", type=Path, help="Output CSV file.")
    parser.add_argument("--limit-fonts", type=int)
    parser.add_argument("--font", action="append", dest="fonts", help="Restrict to basename; repeatable.")
    parser.add_argument("--skt-ok", type=int, choices=(0, 1), help="Restrict by existing skt_ok flag.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fonts = select_font_rows(
        load_font_rows(args.fonts_csv),
        basenames=set(args.fonts) if args.fonts else None,
        skt_ok=args.skt_ok,
        limit=args.limit_fonts,
    )
    if not fonts:
        raise SystemExit("No font rows selected.")

    output = args.output or (args.out_dir / "font_features.parquet")
    summary_csv = args.summary_csv or output.with_suffix(".csv")
    rows = []
    with ParquetRowWriter(output, batch_size=1000) as writer:
        for font in fonts:
            row = extract_static_features(font)
            rows.append(row)
            writer.write(row)

    write_csv_summary(rows, summary_csv)
    print(f"Wrote {output}")
    print(f"Wrote {summary_csv} ({len(rows)} font rows)")


if __name__ == "__main__":
    main()
