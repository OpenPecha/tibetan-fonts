#!/usr/bin/env python3
"""Render a synthetic benchmark render plan as pecha-format JPEG/TXT pairs."""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import re
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image
from tqdm import tqdm

from synthetic_common import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_RENDER_PLAN,
    SHAD,
    TSHEG,
    tex_escape,
)

PAGE_PREFIX = "༄༅། །"
MW_ID = "MW1BCS001"
W_ID = "W1BCS001"
IE_ID = "IE1BCS001"
I_ID_PREFIX = "I1BCS001"
VE_ID_PREFIX = "VE1BCS001"
VOLUME_VERSION = "v001"
BENCHMARK_VERSION = "202604"
DATASET_ID = "BECSynthetic_01"
GROUP_SIZE = 1000
CATALOG_WRITE_LOCK = threading.Lock()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render planned BoCorpus/font pages.")
    parser.add_argument("render_plan", type=Path, nargs="?", default=DEFAULT_RENDER_PLAN)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--page-height-mm", type=float, default=74.0)
    parser.add_argument("--page-ratio", type=float, default=4.0, help="page width / height")
    parser.add_argument("--margin-x-mm", type=float, default=20.0)
    parser.add_argument("--margin-y-mm", type=float, default=16.0)
    parser.add_argument("--font-scale", type=float, default=1.5)
    parser.add_argument("--jpeg-quality", type=int, default=80)
    parser.add_argument("--image-width-px", type=int, default=2400)
    parser.add_argument("--min-lines-per-image", type=int, default=5)
    parser.add_argument("--max-merge-passes", type=int, default=5)
    parser.add_argument("--page-prefix", default=PAGE_PREFIX)
    parser.add_argument("--no-page-prefix", action="store_true")
    parser.add_argument("--lualatex", default="lualatex")
    parser.add_argument("--pdftoppm", default="pdftoppm")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--jobs", type=int, default=1, help="Number of batches to render in parallel")
    return parser.parse_args()


def volume_number(output_id: int) -> int:
    return ((output_id - 1) // GROUP_SIZE) + 1


def page_number_in_volume(output_id: int) -> int:
    return ((output_id - 1) % GROUP_SIZE) + 1


def i_id_for_output(output_id: int) -> str:
    return f"{I_ID_PREFIX}_{volume_number(output_id):04d}"


def ve_id_for_output(output_id: int) -> str:
    return f"{VE_ID_PREFIX}_{volume_number(output_id):04d}"


def benchmark_image_relpath(output_id: int) -> Path:
    return (
        Path("images")
        / W_ID
        / i_id_for_output(output_id)
        / VOLUME_VERSION
        / f"{page_number_in_volume(output_id):04d}.jpg"
    )


def fmt_dim(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")


def fontspec_options(row: dict[str, object]) -> str:
    font_path = Path(str(row["font_abs_path"]))
    parts = [
        f"Path={font_path.parent.as_posix()}/",
        f"UprightFont={{{font_path.name}}}",
        "Renderer=HarfBuzz",
        "RawFeature={script=tibt}",
    ]
    face_index = str(row.get("ttc_face_index") or "").strip()
    if face_index:
        parts.append(f"FontIndex={face_index}")
    return ",\n  ".join(parts)


def split_marker_units(text: str) -> list[str]:
    units: list[str] = []
    buf: list[str] = []
    for i, ch in enumerate(text):
        buf.append(ch)
        next_ch = text[i + 1] if i + 1 < len(text) else ""
        if ch == TSHEG and next_ch != SHAD:
            units.append("".join(buf))
            buf = []
        elif ch == " ":
            units.append("".join(buf))
            buf = []
    if buf:
        units.append("".join(buf))
    return units


def add_marked_breakpoints(text: str) -> str:
    text = re.sub("་(?!།)", lambda _: "་\\syllmark\\allowbreak{}", text)
    text = text.replace(" ", "\\syllmark\\hspace{0.35em}\\allowbreak{}")
    # Keep generated TeX input lines short. The trailing % prevents inserted
    # source newlines from becoming visible inter-word spaces.
    text = text.replace("\\allowbreak{}", "\\allowbreak{}%\n")
    return text + r"\syllmark{}"


def lua_page_map_code(page_map_path: Path, line_map_path: Path) -> str:
    return rf"""\directlua{{
synthetic_current_chunk = ""
synthetic_page_no = 0
synthetic_page_map_path = {json.dumps(str(page_map_path))}
synthetic_line_map_path = {json.dumps(str(line_map_path))}
local node = node
local hlist_id = node.id("hlist")
local vlist_id = node.id("vlist")
local kern_id = node.id("kern")
local glyph_id = node.id("glyph")

local function child_list(n)
  local child = n.head
  if type(child) == "number" and node.getlist then
    child = node.getlist(n)
  end
  if type(child) == "number" then
    return nil
  end
  return child
end

local function has_glyph(head)
  if not head or type(head) == "number" then
    return false
  end
  for n in node.traverse(head) do
    if n.id == glyph_id then
      return true
    elseif n.id == hlist_id or n.id == vlist_id then
      local child = child_list(n)
      if child and has_glyph(child) then
        return true
      end
    end
  end
  return false
end

local function is_text_hlist(n)
  local child = child_list(n)
  return child and has_glyph(child)
end

local function count_text_lines(head)
  local count = 0
  local function walk(list)
    if not list or type(list) == "number" then
      return
    end
    for n in node.traverse(list) do
      if n.id == hlist_id then
        if is_text_hlist(n) then
          count = count + 1
        end
      elseif n.id == vlist_id then
        local child = child_list(n)
        if child then walk(child) end
      end
    end
  end
  walk(head)
  return count
end

local function collect_markers_in_hlist(h)
  local ids = {{}}
  local child = child_list(h)
  if not child or type(child) == "number" then
    return ids
  end
  for n in node.traverse(child) do
    if n.id == kern_id then
      local v = node.get_attribute(n, tex.attribute.syllattr)
      if v then table.insert(ids, v) end
    end
  end
  return ids
end

local function collect_line_markers(head, lines)
  if not head or type(head) == "number" then
    return
  end
  for n in node.traverse(head) do
    if n.id == hlist_id then
      local ids = collect_markers_in_hlist(n)
      if type(ids) == "table" and next(ids) then
        table.insert(lines, ids)
      end
    elseif n.id == vlist_id then
      local child = child_list(n)
      if child then collect_line_markers(child, lines) end
    end
  end
end

function synthetic_set_chunk(id)
  synthetic_current_chunk = id
  tex.count.syllid = 0
end

local function synthetic_pre_shipout_filter(head)
  synthetic_page_no = synthetic_page_no + 1
  local mode = synthetic_page_no == 1 and "w" or "a"
  local f = io.open(synthetic_page_map_path, mode)
  if f then
    f:write(synthetic_page_no .. "," .. synthetic_current_chunk .. "," .. count_text_lines(head) .. string.char(10))
    f:close()
  end
  local lines = {{}}
  local ok, err = pcall(function() collect_line_markers(head, lines) end)
  if not ok then
    lines = {{}}
  end
  local lf = io.open(synthetic_line_map_path, synthetic_page_no == 1 and "w" or "a")
  if lf then
    for i, ids in ipairs(lines) do
      lf:write(synthetic_page_no .. "," .. synthetic_current_chunk .. "," .. i .. ",")
      if type(ids) == "table" then
        for j, marker_id in ipairs(ids) do
          if j > 1 then lf:write(" ") end
          lf:write(marker_id)
        end
      elseif ids then
        lf:write(ids)
      end
      lf:write(string.char(10))
    end
    lf:close()
  end
  return head
end

if luatexbase and luatexbase.add_to_callback then
  luatexbase.add_to_callback("pre_shipout_filter", synthetic_pre_shipout_filter, "synthetic_page_map")
else
  callback.register("pre_shipout_filter", synthetic_pre_shipout_filter)
end
}}"""


def make_tex(rows: list[dict[str, object]], args: argparse.Namespace, page_map_path: Path) -> str:
    first = rows[0]
    height = args.page_height_mm
    width = height * args.page_ratio
    font_size = float(first["font_size_pt"]) * args.font_scale
    baseline = font_size * 1.25
    pages = []
    for row in rows:
        body = add_marked_breakpoints(tex_escape(str(row["text"])))
        pages.append(
            "\\noindent\n"
            f"\\directlua{{synthetic_set_chunk(\"{row['render_id']}\")}}\n"
            f"{body}\n"
            "\\clearpage\n"
        )
    return rf"""\documentclass[12pt]{{article}}
\pagestyle{{empty}}
\usepackage{{fontspec}}
\usepackage[
  paperwidth={fmt_dim(width)}mm,
  paperheight={fmt_dim(height)}mm,
  left={fmt_dim(args.margin_x_mm)}mm,
  right={fmt_dim(args.margin_x_mm)}mm,
  top={fmt_dim(args.margin_y_mm)}mm,
  bottom={fmt_dim(args.margin_y_mm)}mm
]{{geometry}}
\usepackage{{hyperref}}
\setlength{{\parindent}}{{0pt}}
\setlength{{\parskip}}{{0pt}}
\emergencystretch=1em
\tolerance=9999
\pretolerance=9999
\newcount\syllid
\syllid=0
\newattribute\syllattr
\def\syllmark{{%
  \global\advance\syllid by 1
  \directlua{{
    local n = node.new("kern")
    n.kern = 0
    node.set_attribute(n, tex.attribute.syllattr, tex.count.syllid)
    local nest = tex.nest[tex.nest.ptr]
    if nest and nest.tail then
      node.insert_after(nest.head, nest.tail, n)
      nest.tail = n
    else
      node.write(n)
    end
  }}%
}}
{lua_page_map_code(page_map_path, page_map_path.with_suffix(".lines.csv"))}
\setmainfont{{DUMMY}}[
  {fontspec_options(first)}
]
\fontsize{{{fmt_dim(font_size)}pt}}{{{fmt_dim(baseline)}pt}}\selectfont
\begin{{document}}
{''.join(pages)}
\end{{document}}
"""


def run_command(command: list[str], *, cwd: Path, log_path: Path) -> int:
    with log_path.open("w", encoding="utf-8") as log:
        proc = subprocess.run(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, check=False)
    return proc.returncode


def output_page_candidates(prefix: Path, page_numbers: list[int], page_count: int) -> list[Path]:
    width = max(1, len(str(page_count)))
    candidates = []
    for page in page_numbers:
        names = [
            f"{prefix.name}-{page}.jpg",
            f"{prefix.name}-{page:0{width}d}.jpg",
            f"{prefix.name}-{page:02d}.jpg",
            f"{prefix.name}-{page:03d}.jpg",
        ]
        for name in names:
            path = prefix.with_name(name)
            if path.exists():
                candidates.append(path)
                break
        else:
            candidates.append(prefix.with_name(names[0]))
    return candidates


def parse_page_map(path: Path) -> dict[str, list[dict[str, int]]]:
    pages_by_chunk: dict[str, list[dict[str, int]]] = defaultdict(list)
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f, fieldnames=["physical_page", "render_id", "line_count"]):
            render_id = row["render_id"]
            if not render_id:
                continue
            pages_by_chunk[render_id].append(
                {
                    "physical_page": int(row["physical_page"]),
                    "line_count": int(row["line_count"]),
                }
            )
    return pages_by_chunk


def parse_line_map(path: Path) -> dict[tuple[str, int], list[list[int]]]:
    lines_by_chunk_page: dict[tuple[str, int], list[list[int]]] = defaultdict(list)
    if not path.exists():
        return lines_by_chunk_page
    with path.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(
            f,
            fieldnames=["physical_page", "render_id", "line_index", "marker_ids"],
        ):
            render_id = row["render_id"]
            if not render_id:
                continue
            marker_ids = [
                int(item)
                for item in str(row.get("marker_ids") or "").split()
                if item.strip().isdigit()
            ]
            lines_by_chunk_page[(render_id, int(row["physical_page"]))].append(marker_ids)
    return lines_by_chunk_page


def content_pages(pages: list[dict[str, int]]) -> list[dict[str, int]]:
    return [page for page in pages if page["line_count"] > 0]


def init_source_fields(row: dict[str, object]) -> dict[str, object]:
    if "_source_chunk_ids" in row:
        return row
    row["_source_chunk_ids"] = [str(row["chunk_id"])]
    row["_source_plan_image_ids"] = [str(row["image_id"])]
    row["_source_bocorpus_rows"] = [str(row["bocorpus_row"])]
    row["_source_char_ranges"] = [f"{row['char_start']}-{row['char_end']}"]
    return row


def merge_rows(first: dict[str, object], second: dict[str, object]) -> dict[str, object]:
    first = init_source_fields(dict(first))
    second = init_source_fields(second)
    first["text"] = f"{str(first['text']).strip()} {str(second['text']).strip()}".strip()
    first["char_count"] = len(str(first["text"]))
    first["stack_count"] = int(first.get("stack_count") or 0) + int(second.get("stack_count") or 0)
    first["unique_stack_count"] = max(
        int(first.get("unique_stack_count") or 0),
        int(second.get("unique_stack_count") or 0),
    )
    first["_source_chunk_ids"].extend(second["_source_chunk_ids"])
    first["_source_plan_image_ids"].extend(second["_source_plan_image_ids"])
    first["_source_bocorpus_rows"].extend(second["_source_bocorpus_rows"])
    first["_source_char_ranges"].extend(second["_source_char_ranges"])
    return first


def merge_short_rows(
    rows: list[dict[str, object]],
    pages_by_chunk: dict[str, list[dict[str, int]]],
    *,
    min_lines: int,
) -> tuple[list[dict[str, object]], bool]:
    merged: list[dict[str, object]] = []
    changed = False
    i = 0
    while i < len(rows):
        row = init_source_fields(dict(rows[i]))
        pages = content_pages(pages_by_chunk.get(str(row["render_id"]), []))
        is_short_single_page = (
            i + 1 < len(rows)
            and len(pages) == 1
            and pages[0]["line_count"] <= min_lines
        )
        if is_short_single_page:
            row = merge_rows(row, rows[i + 1])
            changed = True
            i += 2
        else:
            i += 1
        merged.append(row)
    return merged, changed


def prepare_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    prepared = []
    for i, row in enumerate(rows, start=1):
        item = init_source_fields(dict(row))
        item["render_id"] = f"r{i}"
        prepared.append(item)
    return prepared


def assign_render_ids(rows: list[dict[str, object]]) -> None:
    for i, row in enumerate(rows, start=1):
        row["render_id"] = f"r{i}"


def set_render_size_fields(rows: list[dict[str, object]], font_scale: float) -> None:
    for row in rows:
        row["font_scale"] = font_scale
        row["rendered_font_size_pt"] = float(row["font_size_pt"]) * font_scale


def add_prefix_to_alternating_pages(
    rows: list[dict[str, object]],
    *,
    start_output_id: int,
    prefix: str,
    enabled: bool,
) -> list[dict[str, object]]:
    prepared: list[dict[str, object]] = []
    for i, row in enumerate(rows):
        item = dict(row)
        should_prefix = enabled and (start_output_id + i) % 2 == 1
        text = str(item["text"]).strip()
        if should_prefix and "༄" not in text[:5]:
            item["text"] = f"{prefix}{text}"
            item["page_prefix_added"] = 1
        else:
            item["text"] = text
            item["page_prefix_added"] = 0
        prepared.append(item)
    return prepared


def save_grayscale_jpeg(src: Path, dest: Path, quality: int) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(src) as img:
        img.convert("L").save(dest, "JPEG", quality=quality)


def strip_transcription_line_edges(text: str) -> str:
    """Strip leading/trailing spaces from each rendered transcription line."""
    return "\n".join(line.strip() for line in text.splitlines()).strip()


def write_text_and_catalog(
    rows: list[dict[str, object]],
    out_dir: Path,
    catalog_rows: list[dict[str, object]],
    pages_by_chunk: dict[str, list[dict[str, int]]],
    lines_by_chunk_page: dict[tuple[str, int], list[list[int]]],
    next_output_id: int,
) -> int:
    for row in rows:
        output_id = next_output_id
        next_output_id += 1
        image_relpath = benchmark_image_relpath(output_id)
        pages = pages_by_chunk.get(str(row["render_id"]), [])
        text_pages = content_pages(pages)
        first_page = (text_pages or pages or [{"physical_page": "", "line_count": ""}])[0]
        text = strip_transcription_line_edges(
            rebuild_rendered_transcription(row, first_page, lines_by_chunk_page)
        )
        first_page_line_count = first_page["line_count"]
        try:
            first_page_no = int(first_page["physical_page"])
        except (TypeError, ValueError):
            first_page_no = None
        if first_page_no is not None:
            mapped_lines = lines_by_chunk_page.get((str(row["render_id"]), first_page_no), [])
            if mapped_lines:
                first_page_line_count = len(mapped_lines)
        catalog_rows.append(
            {
                "output_sequence": output_id,
                "mw_id": MW_ID,
                "w_id": W_ID,
                "i_id": i_id_for_output(output_id),
                "i_version": VOLUME_VERSION,
                "ie_id": IE_ID,
                "ve_id": ve_id_for_output(output_id),
                "ve_version": VOLUME_VERSION,
                "page_in_volume": page_number_in_volume(output_id),
                "image_file_name": image_relpath.as_posix(),
                "transcription": text,
                "source_plan_image_ids": "|".join(row["_source_plan_image_ids"]),
                "page_prefix_added": row.get("page_prefix_added", 0),
                "font_file": row["font_file"],
                "font_path": row["font_path"],
                "ps_name": row["ps_name"],
                "ttc_face_index": row.get("ttc_face_index") or "",
                "font_size_pt": row["font_size_pt"],
                "font_scale": row.get("font_scale") or "",
                "rendered_font_size_pt": row.get("rendered_font_size_pt") or "",
                "script_id": row["script_id"],
                "script_category": row["script_category"],
                "script_type": row["script_type"],
                "bocorpus_row": "|".join(row["_source_bocorpus_rows"]),
                "char_start": row["char_start"],
                "char_end": row["char_end"],
                "source_char_ranges": "|".join(row["_source_char_ranges"]),
                "chunk_id": "|".join(row["_source_chunk_ids"]),
                "batch_id": row["batch_id"],
                "page_in_batch": row["page_in_batch"],
                "physical_page": first_page["physical_page"],
                "physical_pages_for_chunk": len(text_pages),
                "first_page_line_count": first_page_line_count,
                "stack_count": row["stack_count"],
                "unique_stack_count": row["unique_stack_count"],
            }
        )
    return next_output_id


def cleanup_batch_outputs(prefix: Path) -> None:
    for path in glob.glob(str(prefix) + "-*.jpg"):
        Path(path).unlink()


def rebuild_rendered_transcription(
    row: dict[str, object],
    first_page: dict[str, object],
    lines_by_chunk_page: dict[tuple[str, int], list[list[int]]],
) -> str:
    physical_page = first_page.get("physical_page")
    try:
        page_no = int(physical_page)
    except (TypeError, ValueError):
        return strip_transcription_line_edges(str(row["text"]))

    line_marker_ids = lines_by_chunk_page.get((str(row["render_id"]), page_no), [])
    if not line_marker_ids:
        return strip_transcription_line_edges(str(row["text"]))

    units = split_marker_units(str(row["text"]))
    lines: list[str] = []
    previous_end = 0
    for ids in line_marker_ids:
        if not ids:
            continue
        end = min(max(ids), len(units))
        if end <= previous_end:
            continue
        line = "".join(units[previous_end:end]).strip()
        if line:
            lines.append(line)
        previous_end = end
    return strip_transcription_line_edges("\n".join(lines)) or strip_transcription_line_edges(
        str(row["text"])
    )


def compile_batch(
    rows: list[dict[str, object]],
    args: argparse.Namespace,
    *,
    batch_dir: Path,
    log_dir: Path,
    batch_id: str,
    attempt: int,
) -> tuple[dict[str, list[dict[str, int]]], dict[tuple[str, int], list[list[int]]], Path] | None:
    tex_path = batch_dir / f"{batch_id}.tex"
    pdf_path = batch_dir / f"{batch_id}.pdf"
    page_map_path = batch_dir / f"{batch_id}.pages.csv"
    line_map_path = page_map_path.with_suffix(".lines.csv")
    if page_map_path.exists():
        page_map_path.unlink()
    if line_map_path.exists():
        line_map_path.unlink()
    if pdf_path.exists():
        pdf_path.unlink()
    tex_path.write_text(make_tex(rows, args, page_map_path), encoding="utf-8")
    latex_exit = run_command(
        [
            args.lualatex,
            "-interaction=nonstopmode",
            "-halt-on-error",
            "-file-line-error",
            f"-output-directory={batch_dir}",
            str(tex_path),
        ],
        cwd=Path.cwd(),
        log_path=log_dir / f"{batch_id}.lualatex.{attempt}.log",
    )
    if latex_exit != 0 or not pdf_path.exists() or not page_map_path.exists():
        print(f"WARNING: lualatex failed for {batch_id}; see {log_dir / f'{batch_id}.lualatex.{attempt}.log'}")
        return None
    return parse_page_map(page_map_path), parse_line_map(line_map_path), pdf_path


def render_batch(
    rows: list[dict[str, object]],
    args: argparse.Namespace,
    catalog_rows: list[dict[str, object]],
    next_output_id: int,
) -> tuple[bool, int]:
    batch_id = rows[0]["batch_id"]
    batch_dir = args.out_dir / "batches"
    log_dir = args.out_dir / "logs"
    batch_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    prefix = batch_dir / batch_id

    render_rows = prepare_rows(rows)
    compile_result = None
    pages_by_chunk: dict[str, list[dict[str, int]]] = {}
    lines_by_chunk_page: dict[tuple[str, int], list[list[int]]] = {}
    for attempt in range(1, args.max_merge_passes + 2):
        assign_render_ids(render_rows)
        compile_rows = add_prefix_to_alternating_pages(
            render_rows,
            start_output_id=next_output_id,
            prefix=args.page_prefix,
            enabled=not args.no_page_prefix,
        )
        compile_result = compile_batch(
            compile_rows,
            args,
            batch_dir=batch_dir,
            log_dir=log_dir,
            batch_id=batch_id,
            attempt=attempt,
        )
        if compile_result is None:
            return False, next_output_id
        pages_by_chunk, lines_by_chunk_page, _pdf_path = compile_result
        render_rows = compile_rows
        set_render_size_fields(render_rows, args.font_scale)
        merged_rows, changed = merge_short_rows(
            render_rows,
            pages_by_chunk,
            min_lines=args.min_lines_per_image,
        )
        if changed and attempt <= args.max_merge_passes:
            render_rows = merged_rows
            continue
        break
    if compile_result is None:
        return False, next_output_id

    pages_by_chunk, lines_by_chunk_page, pdf_path = compile_result
    physical_pages = sorted(
        {page["physical_page"] for pages in pages_by_chunk.values() for page in pages}
    )
    cleanup_batch_outputs(prefix)
    ppm_exit = run_command(
        [
            args.pdftoppm,
            "-f",
            "1",
            "-l",
            str(max(physical_pages)),
            "-jpeg",
            "-jpegopt",
            f"quality={args.jpeg_quality}",
            "-gray",
            "-scale-to-x",
            str(args.image_width_px),
            "-scale-to-y",
            "-1",
            str(pdf_path),
            str(prefix),
        ],
        cwd=Path.cwd(),
        log_path=log_dir / f"{batch_id}.pdftoppm.log",
    )
    if ppm_exit != 0:
        print(f"WARNING: pdftoppm failed for {batch_id}; see {log_dir / f'{batch_id}.pdftoppm.log'}")
        return False, next_output_id

    first_pages = [
        (content_pages(pages_by_chunk[str(row["render_id"])]) or pages_by_chunk[str(row["render_id"])])[0][
            "physical_page"
        ]
        for row in render_rows
    ]
    rendered_pages = output_page_candidates(prefix, first_pages, max(physical_pages))
    if any(not path.exists() for path in rendered_pages):
        missing = [str(path) for path in rendered_pages if not path.exists()]
        print(f"WARNING: missing rendered page(s) for {batch_id}: {missing[:3]}")
        return False, next_output_id
    output_id = next_output_id
    for src in rendered_pages:
        dest = args.out_dir / benchmark_image_relpath(output_id)
        save_grayscale_jpeg(src, dest, args.jpeg_quality)
        output_id += 1
    next_output_id = write_text_and_catalog(
        render_rows,
        args.out_dir,
        catalog_rows,
        pages_by_chunk,
        lines_by_chunk_page,
        next_output_id,
    )
    cleanup_batch_outputs(prefix)
    return True, next_output_id


def assign_batches(rows: list[dict[str, object]], batch_size: int) -> list[list[dict[str, object]]]:
    by_font: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_font[(str(row["basename"]), str(row.get("ttc_face_index") or ""))].append(row)
    batches: list[list[dict[str, object]]] = []
    for (basename, face_index), group in sorted(by_font.items()):
        group.sort(key=lambda row: int(row["image_id"]))
        for batch_i in range(math.ceil(len(group) / batch_size)):
            chunk = group[batch_i * batch_size : (batch_i + 1) * batch_size]
            batch_id = f"{basename}_{face_index or 'face'}_{batch_i:06d}".replace("/", "_")
            for page_i, row in enumerate(chunk, start=1):
                row["batch_id"] = batch_id
                row["page_in_batch"] = page_i
            batches.append(chunk)
    return batches


def catalog_sort_key(row: dict[str, object]) -> int:
    value = row.get("output_sequence")
    try:
        return int(str(value))
    except (TypeError, ValueError):
        stem = Path(str(row.get("image_file_name") or "0")).stem
        try:
            return int(stem)
        except ValueError:
            return 0


def write_catalog(path: Path, rows: list[dict[str, object]]) -> None:
    with CATALOG_WRITE_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(path.name + ".tmp")
        if not rows:
            tmp_path.write_text("", encoding="utf-8")
            tmp_path.replace(path)
            return
        rows.sort(key=catalog_sort_key)
        fieldnames: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    fieldnames.append(key)
                    seen.add(key)
        with tmp_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        tmp_path.replace(path)


def catalog_fragments_dir(out_dir: Path) -> Path:
    return out_dir / "checkpoints" / "catalog_batches"


def safe_fragment_name(batch_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", batch_id)


def write_catalog_fragment(out_dir: Path, batch_id: str, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    fragment_path = catalog_fragments_dir(out_dir) / f"{safe_fragment_name(batch_id)}.csv"
    write_catalog(fragment_path, rows)


def read_catalog_fragments(out_dir: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    fragment_dir = catalog_fragments_dir(out_dir)
    if not fragment_dir.exists():
        return rows
    for fragment_path in sorted(fragment_dir.glob("*.csv")):
        with fragment_path.open(encoding="utf-8", newline="") as f:
            rows.extend(csv.DictReader(f))
    return rows


def dedupe_catalog_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_sequence: dict[int, dict[str, object]] = {}
    for row in rows:
        key = catalog_sort_key(row)
        if key:
            by_sequence[key] = row
    return [by_sequence[key] for key in sorted(by_sequence)]


def completed_plan_ids(rows: list[dict[str, object]]) -> set[str]:
    done: set[str] = set()
    for row in rows:
        image_path = Path(str(row.get("image_file_name") or ""))
        if image_path:
            for item in str(row.get("source_plan_image_ids") or "").split("|"):
                item = item.strip()
                if item:
                    done.add(item)
    return done


def next_output_id_from_catalog(rows: list[dict[str, object]]) -> int:
    if not rows:
        return 1
    return max(catalog_sort_key(row) for row in rows) + 1


def filter_completed_plan_rows(
    rows: list[dict[str, object]],
    existing_catalog_rows: list[dict[str, object]],
    out_dir: Path,
) -> list[dict[str, object]]:
    done = set()
    for row in existing_catalog_rows:
        image = out_dir / str(row.get("image_file_name") or "")
        if image.exists():
            done.update(item for item in str(row.get("source_plan_image_ids") or "").split("|") if item)
    if not done:
        return rows
    return [row for row in rows if str(row["image_id"]) not in done]


def most_common_join(values: list[str]) -> str:
    counts: dict[str, int] = defaultdict(int)
    for value in values:
        if value:
            counts[value] += 1
    return "|".join(value for value, _count in sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def normalized_script(value: str) -> str:
    text = value.strip().lower()
    if "ume" in text or "transitional" in text:
        return "ume"
    if "uchen" in text:
        return "uchen"
    return "uchen"


def write_benchmark_metadata(out_dir: Path, catalog_rows: list[dict[str, object]]) -> None:
    if not catalog_rows:
        return
    rows = sorted(catalog_rows, key=catalog_sort_key)
    align_root = out_dir / "alignments" / BENCHMARK_VERSION
    dataset_root = align_root / DATASET_ID
    dataset_root.mkdir(parents=True, exist_ok=True)

    groups: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        groups[(str(row["i_id"]), str(row["ve_id"]))].append(row)

    datasets_path = align_root / "datasets.csv"
    with datasets_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["dataset_id", "nb_img_volumes", "nb_img_files", "technology", "script", "modes"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "dataset_id": DATASET_ID,
                "nb_img_volumes": len({row["i_id"] for row in rows}),
                "nb_img_files": len(rows),
                "technology": "synthetic",
                "script": most_common_join([normalized_script(str(row.get("script_type") or "")) for row in rows]),
                "modes": "ptt",
            }
        )

    with (dataset_root / "catalog_alignments.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["i_id", "ve_id", "alignment_type", "nb_pages", "mode"])
        writer.writeheader()
        for (i_id, ve_id), group_rows in sorted(groups.items()):
            writer.writerow(
                {
                    "i_id": i_id,
                    "ve_id": ve_id,
                    "alignment_type": "manual",
                    "nb_pages": len(group_rows),
                    "mode": "ptt",
                }
            )

    with (dataset_root / "catalog_volumes.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "mw_id",
                "mw_title",
                "coll_id",
                "vol_id",
                "vol_version",
                "source",
                "line_breaks",
                "technology",
                "script",
                "access",
            ],
        )
        writer.writeheader()
        for (i_id, ve_id), group_rows in sorted(groups.items()):
            script = most_common_join([normalized_script(str(row.get("script_type") or "")) for row in group_rows])
            for coll_id, vol_id in ((W_ID, i_id), (IE_ID, ve_id)):
                writer.writerow(
                    {
                        "mw_id": MW_ID,
                        "mw_title": "Synthetic BoCorpus Tibetan OCR benchmark",
                        "coll_id": coll_id,
                        "vol_id": vol_id,
                        "vol_version": VOLUME_VERSION,
                        "source": "BDRC",
                        "line_breaks": "Y",
                        "technology": "synthetic",
                        "script": script,
                        "access": "open",
                    }
                )

    for (i_id, ve_id), group_rows in sorted(groups.items()):
        parquet_rows = [
            {
                "img_file_name": Path(str(row["image_file_name"])).name,
                "transcription": row["transcription"],
                "dist_ocr": None,
                "pagination": int(row["page_in_volume"]),
            }
            for row in sorted(group_rows, key=catalog_sort_key)
        ]
        pq.write_table(
            pa.Table.from_pylist(parquet_rows),
            dataset_root / f"{i_id}-{ve_id}_ptt.parquet",
            compression="zstd",
        )

    (dataset_root / "README.md").write_text(
        "# Synthetic BoCorpus Tibetan OCR benchmark\n\n"
        "Local benchmark-format export generated by scripts/synthetic_benchmark/render_batches.py.\n"
        "Images are under images/ and page-to-text alignments are stored as parquet "
        "files in this directory.\n",
        encoding="utf-8",
    )


def render_batch_worker(
    batch_index: int,
    batch: list[dict[str, object]],
    args: argparse.Namespace,
    start_output_id: int,
) -> tuple[int, str, bool, list[dict[str, object]], Path]:
    batch_id = str(batch[0]["batch_id"])
    worker_out = args.out_dir / "workers" / f"batch_{batch_index:06d}"
    if worker_out.exists() and args.force:
        shutil.rmtree(worker_out)
    worker_out.mkdir(parents=True, exist_ok=True)
    worker_args = SimpleNamespace(**vars(args))
    worker_args.out_dir = worker_out
    catalog_rows: list[dict[str, object]] = []
    ok, _ = render_batch(batch, worker_args, catalog_rows, start_output_id)
    return batch_index, batch_id, ok, catalog_rows, worker_out


def move_worker_output(
    result: tuple[int, str, bool, list[dict[str, object]], Path],
    final_out_dir: Path,
) -> tuple[list[dict[str, object]], int]:
    catalog_rows: list[dict[str, object]] = []
    ok_batches = 0
    _batch_index, _batch_id, ok, rows, worker_out = result
    if not ok:
        return catalog_rows, ok_batches
    ok_batches += 1
    for row in sorted(rows, key=catalog_sort_key):
        old_image = worker_out / row["image_file_name"]
        final_image = final_out_dir / row["image_file_name"]
        final_image.parent.mkdir(parents=True, exist_ok=True)
        if old_image.exists():
            shutil.move(str(old_image), final_image)
        catalog_rows.append(dict(row))
    return catalog_rows, ok_batches


def render_batches_parallel(
    batches: list[list[dict[str, object]]],
    args: argparse.Namespace,
    *,
    start_output_id: int,
    catalog_rows: list[dict[str, object]],
) -> tuple[list[dict[str, object]], int]:
    new_catalog_rows: list[dict[str, object]] = []
    checkpoint_catalog_rows = list(catalog_rows)
    ok_batches = 0
    start_ids: list[int] = []
    next_start = start_output_id
    for batch in batches:
        start_ids.append(next_start)
        next_start += len(batch)
    executor = ThreadPoolExecutor(max_workers=args.jobs)
    progress = tqdm(total=len(batches), desc="Render batches", unit="batch")
    future_map = {
        executor.submit(render_batch_worker, i, batch, args, start_ids[i]): i
        for i, batch in enumerate(batches)
    }
    pending_results: dict[int, tuple[int, str, bool, list[dict[str, object]], Path]] = {}
    next_commit_index = 0
    try:
        for future in as_completed(future_map):
            batch_index = future_map[future]
            pending_results[batch_index] = future.result()
            progress.update(1)
            while next_commit_index in pending_results:
                result = pending_results.pop(next_commit_index)
                rows, ok = move_worker_output(
                    result,
                    args.out_dir,
                )
                if rows:
                    new_catalog_rows.extend(rows)
                    checkpoint_catalog_rows.extend(rows)
                    write_catalog_fragment(args.out_dir, result[1], rows)
                ok_batches += ok
                next_commit_index += 1
    except KeyboardInterrupt:
        for future in future_map:
            future.cancel()
        checkpointed_plan_ids = completed_plan_ids(checkpoint_catalog_rows)
        print(
            f"\nInterrupted; checkpoint fragments contain {len(checkpoint_catalog_rows)} "
            f"output image row(s), covering {len(checkpointed_plan_ids)} plan row(s)."
        )
        raise
    finally:
        progress.close()
        executor.shutdown(wait=False, cancel_futures=True)
    return new_catalog_rows, ok_batches


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    if args.force:
        checkpoint_dir = catalog_fragments_dir(args.out_dir)
        if checkpoint_dir.exists():
            shutil.rmtree(checkpoint_dir)
        existing_catalog_rows = []
    else:
        existing_catalog_rows = dedupe_catalog_rows(read_catalog_fragments(args.out_dir))
        if existing_catalog_rows:
            print(
                f"Loaded {len(existing_catalog_rows)} checkpointed output image row(s), "
                f"covering {len(completed_plan_ids(existing_catalog_rows))} plan row(s)."
            )
    rows = pq.read_table(args.render_plan).to_pylist()
    rows.sort(key=lambda row: int(row["image_id"]))
    if args.limit is not None:
        rows = rows[: args.limit]
    if existing_catalog_rows:
        before = len(rows)
        rows = filter_completed_plan_rows(rows, existing_catalog_rows, args.out_dir)
        skipped = before - len(rows)
        if skipped:
            print(f"Resuming: skipped {skipped} already rendered plan row(s)")
    batches = assign_batches(rows, args.batch_size)
    next_output_id = next_output_id_from_catalog(existing_catalog_rows)
    if args.jobs > 1:
        new_catalog_rows, ok_batches = render_batches_parallel(
            batches,
            args,
            start_output_id=next_output_id,
            catalog_rows=existing_catalog_rows,
        )
    else:
        new_catalog_rows = []
        ok_batches = 0
        for batch_index, batch in enumerate(tqdm(batches, desc="Render batches", unit="batch")):
            batch_start_id = next_output_id
            ok, next_output_id = render_batch(batch, args, new_catalog_rows, next_output_id)
            if ok:
                ok_batches += 1
                fragment_rows = [
                    row for row in new_catalog_rows if catalog_sort_key(row) >= batch_start_id
                ]
                write_catalog_fragment(args.out_dir, str(batch[0]["batch_id"]), fragment_rows)
    catalog_rows = dedupe_catalog_rows(existing_catalog_rows + new_catalog_rows)
    write_benchmark_metadata(args.out_dir, catalog_rows)
    print(f"Rendered {len(new_catalog_rows)} new image(s) in {ok_batches}/{len(batches)} successful batch(es)")
    print(f"Benchmark metadata now has {len(catalog_rows)} image(s)")


if __name__ == "__main__":
    main()

