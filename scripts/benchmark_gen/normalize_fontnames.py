#!/usr/bin/env python3
import os, sys, shutil, hashlib, unicodedata, re, csv, pathlib

FONT_EXTS = (".ttf", ".otf", ".ttc")

def slugify_component(name):
    nfkd = unicodedata.normalize("NFKD", name)
    no_marks = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    ascii_only = no_marks.encode("ascii", "ignore").decode("ascii")
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", ascii_only)
    safe = re.sub(r"_+", "_", safe).strip("_")
    return safe or "x"

def slugify_filename(name):
    stem, ext = os.path.splitext(name)
    safe_stem = slugify_component(stem)
    return safe_stem + ext.lower()

def short_hash(s):
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]

def allocate_normalized_dir(rel_parts, used_dirnames):
    """
    Allocate a normalized directory path for an original rel_parts,
    resolving collisions ONLY between different original dirs.
    used_dirnames: dict(parent_norm_path_str -> set(child_names_used))
    Returns list of normalized parts.
    """
    norm_parts = []
    parent_key = ""  # root in normalized space

    for part in rel_parts:
        safe = slugify_component(part)
        used = used_dirnames.setdefault(parent_key, set())

        candidate = safe
        if candidate in used:
            candidate = f"{safe}_{short_hash(parent_key + '/' + part)}"

        used.add(candidate)
        norm_parts.append(candidate)
        parent_key = parent_key + "/" + candidate if parent_key else candidate

    return norm_parts

def normalize_folder(src_root, dst_root, mapping_csv="fontname_mapping.csv"):
    src_root = pathlib.Path(src_root).resolve()
    dst_root = pathlib.Path(dst_root).resolve()
    dst_root.mkdir(parents=True, exist_ok=True)

    used_dirnames = {}     # for collision resolution between *different* dirs
    used_filenames = {}    # per normalized directory
    dir_map = {}           # original_rel_dir(str) -> tuple(norm_parts)

    mappings = []

    for p in src_root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in FONT_EXTS:
            continue

        # Original relative directory (as a stable key)
        rel_dir = p.parent.relative_to(src_root)
        rel_key = str(rel_dir)

        # Allocate normalized directory ONCE per original directory
        if rel_key not in dir_map:
            norm_parts = allocate_normalized_dir(rel_dir.parts, used_dirnames)
            dir_map[rel_key] = tuple(norm_parts)

        norm_dir_parts = dir_map[rel_key]
        norm_dir = dst_root.joinpath(*norm_dir_parts)
        norm_dir.mkdir(parents=True, exist_ok=True)

        # Normalize filename
        safe_name = slugify_filename(p.name)
        used = used_filenames.setdefault(str(norm_dir), set())
        candidate = safe_name

        if candidate in used or (norm_dir / candidate).exists():
            base, ext = os.path.splitext(safe_name)
            candidate = f"{base}_{short_hash(str(p))}{ext}"

        used.add(candidate)

        out_path = norm_dir / candidate
        shutil.copy2(p, out_path)

        mappings.append({
            "original_path": str(p),
            "normalized_path": str(out_path),
            "original_dir": str(p.parent),
            "normalized_dir": str(norm_dir),
            "original_name": p.name,
            "normalized_name": candidate
        })

    map_path = dst_root / mapping_csv
    with open(map_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "original_path","normalized_path",
                "original_dir","normalized_dir",
                "original_name","normalized_name"
            ]
        )
        w.writeheader()
        w.writerows(mappings)

    print(f"Normalized {len(mappings)} fonts into {dst_root}")
    print(f"Mapping written to {map_path}")

def main():
    if len(sys.argv) != 3:
        print("Usage: normalize_fontnames.py SRC_DIR DST_DIR")
        sys.exit(1)
    normalize_folder(sys.argv[1], sys.argv[2])

if __name__ == "__main__":
    main()
