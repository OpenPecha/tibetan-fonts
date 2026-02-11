#!/usr/bin/env python3
"""
Export dataset files from out/ directory to export_corpus/ and export_sources/
based on digital_fonts.filtered.csv
"""

import csv
import shutil
import sys
from pathlib import Path

CSV_FILE = "digital_fonts.filtered.csv"
OUT_DIR = Path("out")
EXPORT_CORPUS_DIR = Path("export_corpus")
EXPORT_SOURCES_DIR = Path("export_sources")


def main():
    # Create export directories
    EXPORT_CORPUS_DIR.mkdir(exist_ok=True)
    EXPORT_SOURCES_DIR.mkdir(exist_ok=True)

    if not OUT_DIR.exists():
        print(f"Error: {OUT_DIR} directory does not exist", file=sys.stderr)
        sys.exit(1)

    if not Path(CSV_FILE).exists():
        print(f"Error: {CSV_FILE} does not exist", file=sys.stderr)
        sys.exit(1)

    copied_png = 0
    copied_txt = 0
    copied_orig_txt = 0
    missing_png = 0
    missing_txt = 0
    missing_orig_txt = 0

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            basename = row.get('basename', '').strip()
            if not basename:
                continue

            # Copy PNG to export_corpus/
            png_src = OUT_DIR / f"{basename}.png"
            png_dst = EXPORT_CORPUS_DIR / f"{basename}.png"
            if png_src.exists():
                shutil.copy2(png_src, png_dst)
                copied_png += 1
            else:
                print(f"Warning: {png_src} not found", file=sys.stderr)
                missing_png += 1

            # Copy .txt to export_corpus/
            txt_src = OUT_DIR / f"{basename}.txt"
            txt_dst = EXPORT_CORPUS_DIR / f"{basename}.txt"
            if txt_src.exists():
                shutil.copy2(txt_src, txt_dst)
                copied_txt += 1
            else:
                print(f"Warning: {txt_src} not found", file=sys.stderr)
                missing_txt += 1

            # Copy .orig.txt to export_sources/
            orig_txt_src = OUT_DIR / f"{basename}.orig.txt"
            orig_txt_dst = EXPORT_SOURCES_DIR / f"{basename}.orig.txt"
            if orig_txt_src.exists():
                shutil.copy2(orig_txt_src, orig_txt_dst)
                copied_orig_txt += 1
            else:
                print(f"Warning: {orig_txt_src} not found", file=sys.stderr)
                missing_orig_txt += 1

    print(f"\nExport complete:")
    print(f"  PNG files: {copied_png} copied, {missing_png} missing")
    print(f"  TXT files: {copied_txt} copied, {missing_txt} missing")
    print(f"  ORIG.TXT files: {copied_orig_txt} copied, {missing_orig_txt} missing")
    print(f"\nExported to:")
    print(f"  {EXPORT_CORPUS_DIR}/ (PNG and TXT files)")
    print(f"  {EXPORT_SOURCES_DIR}/ (ORIG.TXT files)")


if __name__ == "__main__":
    main()

