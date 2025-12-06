#!/usr/bin/env python3
import os, sys, csv
import re
from fontTools.ttLib import TTFont, TTCollection

FONT_SIZE_PT = 24
DPI = 300

def remove_control_chars(s):
    """Remove control characters from a string."""
    if not s:
        return s
    # Remove all control characters (Unicode category Cc) and DEL (0x7F)
    # This includes: \x00-\x1F and \x7F-\x9F
    return re.sub(r'[\x00-\x1F\x7F-\x9F]', '', s)

def sanitize_basename(s):
    """Sanitize basename to be CLI-friendly: ASCII only, replace non-friendly chars with _."""
    if not s:
        return "Font"
    # Remove all non-ASCII characters
    s = s.encode('ascii', 'ignore').decode('ascii')
    # Replace spaces and non-CLI-friendly characters with underscores
    # Keep only: alphanumeric, underscore, hyphen, dot
    s = re.sub(r'[^a-zA-Z0-9._-]', '_', s)
    # Collapse multiple consecutive underscores into one
    s = re.sub(r'_+', '_', s)
    # Remove leading/trailing underscores and dots
    s = s.strip('_.')
    # Find first letter and remove everything before it
    match = re.search(r'[a-zA-Z]', s)
    if match:
        s = s[match.start():]
    else:
        # No letter found, use default
        return "Font"
    # Capitalize first letter
    if s:
        s = s[0].upper() + s[1:]
    # Ensure it's not empty
    if not s:
        return "Font"
    return s

def get_names(tt):
    names = {}
    for rec in tt['name'].names:
        try:
            s = rec.toUnicode().strip()
        except Exception:
            continue
        if s:
            s = remove_control_chars(s)
            if s:  # Only add if still non-empty after removing control chars
                names.setdefault(rec.nameID, set()).add(s)
    ps = remove_control_chars(next(iter(names.get(6, [])), ""))
    others = sorted(set().union(*[names.get(i,set()) for i in (1,2,4)]))
    return ps, others

def safe_basename(fn, face_index=None):
    base = os.path.splitext(os.path.basename(fn))[0]
    if face_index is None:
        return base
    # stable, filename-safe
    return f"{base}_ttc_{face_index}"

def iter_font_faces(path):
    lower = path.lower()
    if lower.endswith(".ttc"):
        col = TTCollection(path, lazy=True)
        for i in range(len(col.fonts)):
            yield i, TTFont(path, fontNumber=i, lazy=True)
    else:
        yield None, TTFont(path, fontNumber=0, lazy=True)

def main(font_dirs, out_csv="digital_fonts.csv"):
    rows = []
    used_basenames = {}  # Track basenames to ensure uniqueness
    
    for d in font_dirs:
        for root, _, files in os.walk(d):
            for fn in files:
                if not fn.lower().endswith((".ttf",".otf",".ttc")):
                    continue
                path = os.path.join(root, fn)
                try:
                    for face_index, tt in iter_font_faces(path):
                        ps, others = get_names(tt)
                        raw_base = safe_basename(fn, face_index)
                        base = sanitize_basename(raw_base)
                        
                        # Ensure uniqueness
                        original_base = base
                        suffix = 1
                        while base in used_basenames:
                            base = f"{original_base}_{suffix}"
                            suffix += 1
                        used_basenames[base] = True
                        
                        rows.append({
                            "basename": base,
                            "ps_name": ps,
                            "other_names": "|".join(others),
                            "font_path": path,
                            "ttc_face_index": "" if face_index is None else str(face_index),
                            "font_size_pt": FONT_SIZE_PT,
                            "dpi": DPI
                        })
                except Exception:
                    # skip unreadable fonts but keep going
                    continue

    # Sort rows alphabetically by basename (first column)
    rows.sort(key=lambda x: x["basename"].lower())

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "basename","ps_name","other_names","font_path",
                "ttc_face_index","font_size_pt","dpi"
            ]
        )
        w.writeheader()
        w.writerows(rows)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: make_fonts_csv.py FONT_DIR [FONT_DIR2 ...]")
        sys.exit(1)
    main(sys.argv[1:])
