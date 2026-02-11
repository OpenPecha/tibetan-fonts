#!/usr/bin/env python3
import csv, pathlib, subprocess, os, sys

CSV_IN = "digital_fonts.filtered.csv"   # or digital_fonts.csv
OUT_DIR = pathlib.Path("preview_skt")
TEST_STR = "ཧཱུ་ཛྲ་དྡྷ་ཅྖ"

# tweak if you want bigger/smaller previews
FONT_PTEM = 60     # point size-ish; hb-view uses ptem for point size
DPI = 250          # raster resolution for png

def render_one(basename, font_path, face_index=""):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_png = OUT_DIR / f"{basename}.png"

    cmd = [
        "hb-view",
        "--font-file", font_path,
        "--output-file", str(out_png),
        "--output-format", "png",
        "--font-ptem", str(FONT_PTEM),
        "--margin", "50",          # give some whitespace around
        "--line-space", "1.2",     # not critical for one line
        "--script", "tibt",
        "--language", "bo",
        TEST_STR
    ]

    if face_index != "":
        cmd += ["--face-index", face_index]

    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
        return True, ""
    except subprocess.CalledProcessError as e:
        return False, e.output[:300]

def main():
    ok = fail = 0
    with open(CSV_IN, encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            base = row["basename"]
            font_path = row["font_path"]
            face_index = row.get("ttc_face_index", "").strip()

            success, msg = render_one(base, font_path, face_index)
            if success:
                ok += 1
            else:
                fail += 1
                print(f"!! hb-view failed for {base}: {msg}")

    print(f"Done. OK={ok} FAIL={fail}. Output in {OUT_DIR}/")

if __name__ == "__main__":
    main()
