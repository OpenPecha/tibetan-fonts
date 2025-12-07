#!/usr/bin/env python3
import csv, subprocess, pathlib

PROBE = "བོད་ཡིག། སྐྱེས་ཆེན། རྒྱལ་པོ། ༠༡༢༣༤༥༦༧༨༩"

SKT_LIST_PATH = "skt_ok.lst"
FONT_SIZES_ADJUSTED_PATH = "font_sizes_adjusted.csv"

def load_skt_ok_set(path=SKT_LIST_PATH):
    p = pathlib.Path(path)
    if not p.exists():
        return set()
    items = []
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            items.append(s)
    print(items)
    return set(items)

def load_font_sizes_adjusted(path=FONT_SIZES_ADJUSTED_PATH):
    """
    Load font size adjustments from font_sizes_adjusted.csv.
    Returns a dict mapping basename -> proposed_size_pt (as string).
    """
    p = pathlib.Path(path)
    if not p.exists():
        print(f"Warning: {path} not found, font sizes will not be adjusted.")
        return {}
    
    size_map = {}
    try:
        with open(p, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                basename = row.get("basename", "").strip()
                proposed_size = row.get("proposed_size_pt", "").strip()
                if basename and proposed_size:
                    size_map[basename] = proposed_size
        print(f"Loaded {len(size_map)} font size adjustments from {path}.")
    except Exception as e:
        print(f"Warning: Error loading {path}: {e}")
        return {}
    
    return size_map

def hb_shape(font_path, face_index, text):
    """
    Run hb-shape with robust TTC face flag detection.
    """
    index_flags = []
    if face_index != "":
        index_flags = [
            ["--face-index", face_index],
            ["--font-index", face_index],
            ["--ttc-index", face_index],
        ]

    base_cmd = ["hb-shape", "--font-file", font_path, "--script=tibt", "--language=bo"]

    last_err = None
    for flag in (index_flags or [[]]):
        cmd = base_cmd + flag + [text]
        try:
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
            return out
        except subprocess.CalledProcessError as e:
            last_err = e.output
    raise RuntimeError(last_err or "hb-shape failed")

def hb_has_notdef(font_path, face_index=""):
    try:
        out = hb_shape(font_path, face_index, PROBE)
        return (".notdef" in out), out.strip()
    except Exception as e:
        return True, f"hb-shape error: {str(e)[:200]}"

def main(csv_in="digital_fonts.csv", csv_out="digital_fonts.filtered.csv"):
    skt_ok_set = load_skt_ok_set()
    font_sizes_map = load_font_sizes_adjusted()

    kept, dropped = [], []
    with open(csv_in, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            font_path = row["font_path"]
            face_index = row.get("ttc_face_index", "").strip()
            base = row["basename"].strip()

            # Original coverage test
            bad, info = hb_has_notdef(font_path, face_index)
            bad = False

            # New manual Sanskrit OK flag
            row["skt_ok"] = "1" if base in skt_ok_set else "0"
            
            # Update font size from font_sizes_adjusted.csv if available
            if base in font_sizes_map:
                row["font_size_pt"] = font_sizes_map[base]

            if bad:
                row["drop_reason"] = info
                dropped.append(row)
            else:
                kept.append(row)

    if kept:
        with open(csv_out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(kept[0].keys()))
            w.writeheader()
            w.writerows(kept)

    if dropped:
        with open("fonts_dropped.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(dropped[0].keys()))
            w.writeheader()
            w.writerows(dropped)

    print(f"Kept {len(kept)} fonts, dropped {len(dropped)}.")
    if skt_ok_set:
        print(f"Added skt_ok from {SKT_LIST_PATH} ({len(skt_ok_set)} basenames).")
    else:
        print(f"Added skt_ok but list {SKT_LIST_PATH} not found or empty (all skt_ok=0).")

if __name__ == "__main__":
    main()
