#!/usr/bin/env python3
"""
Build fonts_export.zip containing:
  font_files/<filename>   – one file per unique font, original filename
  catalog.csv             – merged metadata from catalog/ CSVs + font_size_pt / skt_ok

Merge logic
-----------
  Benchmark catalog   → script_id, font_path, ttc_face_index   (one row per font face)
  Script lists        → script metadata joined on script_id == id
  digital_fonts csv   → font_size_pt, skt_ok joined on font_path
"""

import csv
import sys
import zipfile
from pathlib import Path

# ── paths ──────────────────────────────────────────────────────────────────────
CATALOG_DIR        = Path("catalog")
BENCHMARK_CSV      = CATALOG_DIR / "Benchmark catalog - digital_fonts.csv"
SCRIPTS_CSV        = CATALOG_DIR / "Script lists - Scripts.csv"
SELECTION_DIR      = Path("selection_normalized")
OUTPUT_ZIP         = Path("fonts_for_synthetic_data.zip")

# Pick the most recently modified of digital_fonts.filtered.csv / digital_fonts.csv
_candidates = [Path("digital_fonts.filtered.csv"), Path("digital_fonts.csv")]
FONTS_CSV = max((p for p in _candidates if p.exists()), key=lambda p: p.stat().st_mtime)

# ── load helpers ───────────────────────────────────────────────────────────────

def load_scripts(path: Path) -> dict[str, dict]:
    """Return {id: row} keeping only columns that actually have data."""
    useful = {"id", "name (phonetics, Wylie in parentheses, and English)",
              "3 types", "8 categories", "descenders length ratio",
              "gigu angle for cursives", "popularity on BDRC", "period"}
    result = {}
    with path.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            sid = row.get("id", "").strip()
            if sid:
                result[sid] = {k: v for k, v in row.items() if k in useful}
    return result


def load_fonts_csv(path: Path) -> dict[str, dict]:
    """Return {font_path: {font_size_pt, skt_ok}}."""
    result = {}
    with path.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            fp = row.get("font_path", "").strip()
            if fp:
                result[fp] = {
                    "font_size_pt": row.get("font_size_pt", ""),
                    "skt_ok":       row.get("skt_ok", ""),
                }
    return result


def load_benchmark(path: Path) -> list[dict]:
    """Return list of rows keeping only script_id, font_path, ttc_face_index, ps_name."""
    rows = []
    with path.open(encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            fp      = row.get("font_path", "").strip()
            sid     = row.get("script id", "").strip()
            tidx    = row.get("ttc_face_index", "").strip()
            ps_name = row.get("font ps_name", "").strip()
            if fp and sid:
                rows.append({"script_id": sid, "font_path": fp,
                             "ttc_face_index": tidx, "ps_name": ps_name})
    return rows

# ── main ───────────────────────────────────────────────────────────────────────

def main():
    print(f"Using fonts CSV: {FONTS_CSV}")

    scripts   = load_scripts(SCRIPTS_CSV)
    fonts_map = load_fonts_csv(FONTS_CSV)
    bench     = load_benchmark(BENCHMARK_CSV)

    # Build catalog rows
    catalog_rows = []
    script_cols = [
        "name (phonetics, Wylie in parentheses, and English)",
        "3 types", "8 categories", "descenders length ratio",
        "gigu angle for cursives", "popularity on BDRC", "period",
    ]
    fieldnames = (
        ["font_file", "ps_name", "script_id"]
        + script_cols
        + ["ttc_face_index", "font_size_pt", "skt_ok"]
    )

    missing_scripts = set()
    missing_fonts   = set()

    for row in bench:
        sid       = row["script_id"]
        font_path = row["font_path"]

        script_meta = scripts.get(sid)
        if script_meta is None:
            missing_scripts.add(sid)
            script_meta = {}

        font_meta = fonts_map.get(font_path)
        if font_meta is None:
            missing_fonts.add(font_path)
            font_meta = {"font_size_pt": "", "skt_ok": ""}

        font_file = Path(font_path).name  # e.g. "Aathup.ttf"

        catalog_rows.append({
            "font_file":       font_file,
            "ps_name":         row["ps_name"],
            "script_id":       sid,
            **{c: script_meta.get(c, "") for c in script_cols},
            "ttc_face_index":  row["ttc_face_index"],
            "font_size_pt":    font_meta["font_size_pt"],
            "skt_ok":          font_meta["skt_ok"],
        })

    if missing_scripts:
        print(f"WARNING: {len(missing_scripts)} script id(s) not found in Scripts list: "
              f"{sorted(missing_scripts)}")
    if missing_fonts:
        print(f"WARNING: {len(missing_fonts)} font_path(s) not found in {FONTS_CSV}: "
              f"{sorted(missing_fonts)}")

    # Collect unique font files to bundle
    unique_font_paths = {Path(row["font_path"]) for row in bench}

    # Write zip
    with zipfile.ZipFile(OUTPUT_ZIP, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # font_files/
        missing_files = []
        for rel_path in sorted(unique_font_paths):
            src = SELECTION_DIR / rel_path.relative_to("selection_normalized")
            dest = f"font_files/{rel_path.name}"
            if src.exists():
                zf.write(src, dest)
                print(f"  + {dest}")
            else:
                missing_files.append(str(src))
                print(f"  !! MISSING: {src}")

        # catalog.csv
        import io
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(catalog_rows)
        zf.writestr("catalog.csv", buf.getvalue().encode("utf-8"))

        # README.md
        col_rows = "\n".join([
            "| Column | Description |",
            "|--------|-------------|",
            "| `font_file` | Filename of the font within `font_files/` — matches exactly the filename in the zip |",
            "| `ps_name` | PostScript name of the font face |",
            "| `script_id` | Numeric identifier for the Tibetan script style, matching the Script lists sheet |",
            "| `name (phonetics, ...)` | Full name of the script style in phonetics, Wylie transliteration, and English |",
            "| `3 types` | Broad script category: Uchen, Umé, or Other |",
            "| `8 categories` | Finer-grained script category (e.g. Kham, Drutsa, Petsug…) |",
            "| `descenders length ratio` | Typical ratio of descender length to body height |",
            "| `gigu angle for cursives` | Angle of the gigu vowel marker, relevant for cursive styles |",
            "| `popularity on BDRC` | Estimated frequency of this script style in the BDRC digital image corpus"
            " (0 = extremely rare → 4 = very common). Useful for subsampling fonts representative of real"
            " BDRC scans, or for weighting font frequency during synthetic data generation |",
            "| `period` | Historical period(s) in which this script style was used |",
            "| `ttc_face_index` | For TTC files: 0-based index of the face to load (e.g. with FreeType or"
            " fonttools). Empty for single-face TTF/OTF files |",
            "| `font_size_pt` | Suggested point size so that all fonts render at a visually similar body"
            " height. This is a **relative** value — use it as a scaling factor rather than an absolute"
            " size, e.g. multiply your base size by `font_size_pt / 24` |",
            "| `skt_ok` | `1` if the font correctly renders complex Sanskrit-derived stacks (conjuncts);"
            " `0` if it does not or rendering is unreliable. Note: there may be occasional false positives |",
        ])
        readme = (
            "# Tibetan Digital Fonts For Synthetic Data Benchmark Dataset\n\n"
            "This archive contains a curated collection of Tibetan digital fonts along with\n"
            "a catalog of metadata used for OCR benchmark generation.\n\n"
            "## Contents\n\n"
            "```\n"
            "font_files/   One font file per unique typeface (TTF, OTF, or TTC)\n"
            "catalog.csv   Metadata table — one row per font face (see below)\n"
            "```\n\n"
            "## Font files\n\n"
            "Font files are stored flat in `font_files/` using their original filenames.\n"
            "TTC (TrueType Collection) files may contain multiple faces; the specific face\n"
            "to use is identified by the `ttc_face_index` column in `catalog.csv`.\n\n"
            "## catalog.csv columns\n\n"
            + col_rows + "\n"
        )
        zf.writestr("README.md", readme.encode("utf-8"))

    print(f"\nWrote {OUTPUT_ZIP}")
    print(f"  {len(unique_font_paths)} font file(s)")
    print(f"  {len(catalog_rows)} catalog row(s)")
    if missing_files:
        print(f"  {len(missing_files)} missing source file(s) — see warnings above")


if __name__ == "__main__":
    main()
