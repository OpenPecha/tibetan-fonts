#!/usr/bin/env python3
import pathlib, re

OUT_DIR = pathlib.Path("out")

def split_into_units(orig: str):
    """
    Split orig text into units that correspond to your markers.
    Rule must mirror add_tibetan_breakpoints():
      - boundary after '་' unless next char is '།'
      - boundary after every '།'
    We return a list of strings, each ending at a boundary.
    """
    units = []
    buf = []
    n = len(orig)

    for i, ch in enumerate(orig):
        buf.append(ch)

        next_ch = orig[i+1] if i+1 < n else ""
        is_tsheg_boundary = (ch == "་" and next_ch != "།")
        is_shad_boundary  = (ch == " ")

        if is_tsheg_boundary or is_shad_boundary:
            units.append("".join(buf))
            buf = []

    if buf:
        units.append("".join(buf))

    return units

def rebuild_one(base):
    orig_path = OUT_DIR / f"{base}.orig.txt"
    breaks_path = OUT_DIR / f"{base}.breaks"
    out_txt = OUT_DIR / f"{base}.txt"

    if not orig_path.exists() or not breaks_path.exists():
        return False

    orig = orig_path.read_text(encoding="utf-8")
    units = split_into_units(orig)

    # breaks file is lines of syllable IDs (1-based)
    break_lines = []
    for line in breaks_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        ids = [int(x) for x in line.split()]
        break_lines.append(ids)

    # Build output lines by slicing units using last ID in each line
    out_lines = []
    prev_end = 0
    for ids in break_lines:
        end_id = ids[-1]  # inclusive boundary id
        end_idx = min(end_id, len(units))
        out_lines.append("".join(units[prev_end:end_idx]).strip())
        prev_end = end_idx

    out_txt.write_text("\n".join(out_lines) + "\n", encoding="utf-8")
    return True

def main():
    ok = 0
    for orig_path in OUT_DIR.glob("*.orig.txt"):
        base = orig_path.stem.replace(".orig", "")
        if rebuild_one(base):
            ok += 1
    print(f"Rebuilt page-1 GT for {ok} fonts.")

if __name__ == "__main__":
    main()
