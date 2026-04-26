#!/usr/bin/env python3
"""Render sampled stack/font cases for manual false-positive/negative review."""

from __future__ import annotations

import argparse
import csv
import hashlib
from pathlib import Path

import pandas as pd
from PIL import Image, ImageDraw

from coverage_common import (
    DEFAULT_FONTS_CSV,
    DEFAULT_OUT_DIR,
    hb_view_render,
    load_font_rows,
    slugify,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a sampled audit sheet from support Parquet.")
    parser.add_argument("support_parquet", type=Path)
    parser.add_argument("--fonts-csv", type=Path, default=DEFAULT_FONTS_CSV)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR / "audit")
    parser.add_argument(
        "--kind",
        choices=(
            "mixed",
            "false-positive",
            "complex-pass",
            "placement-warning",
            "false-negative",
            "normal-fail",
            "disagreement",
        ),
        default="mixed",
    )
    parser.add_argument("--sample-size", type=int, default=80)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--hb-view", default="hb-view")
    parser.add_argument("--render-margin", type=int, default=80)
    parser.add_argument("--columns", type=int, default=4)
    return parser.parse_args()


def ensure_classification_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "placement_warning_count" not in df.columns:
        df["placement_warning_count"] = 0
        df["placement_warnings"] = ""
        print(
            "WARNING: support parquet has no placement_warning_count column; "
            "rebuild it to apply the latest geometry heuristics."
        )
    if "placement_warnings" not in df.columns:
        df["placement_warnings"] = ""
    df["final_ok"] = (df["ok"] == True) & (df["placement_warning_count"].fillna(0) == 0)  # noqa: E712
    return df


def filter_rows(df: pd.DataFrame, kind: str) -> pd.DataFrame:
    if kind == "complex-pass":
        return df[
            (df["test_kind"] == "stack")
            & (df["final_ok"] == True)  # noqa: E712 - pandas boolean mask
            & (df["subjoined_count"] >= 2)
            & (df["vowel_diacritic_count"] >= 1)
        ]
    if kind == "placement-warning":
        return df[(df["placement_warning_count"] > 0)]
    if kind == "false-positive":
        return df[
            (df["test_kind"] == "stack")
            & (df["final_ok"] == True)  # noqa: E712 - pandas boolean mask
            & (
                (df["skt_ok"] == 0)
                | (df["complexity"] >= 3)
                | (df["has_vowel_diacritic"] == True)  # noqa: E712
            )
        ]
    if kind == "false-negative":
        return df[(df["final_ok"] == False) & (df["skt_ok"] == 1)]  # noqa: E712
    if kind == "normal-fail":
        return df[(df["test_kind"] == "normal") & (df["final_ok"] == False)]  # noqa: E712
    if kind == "disagreement":
        return df[
            (df["test_kind"] == "stack")
            & (df["final_ok"].astype(int) != df["skt_ok"].fillna(-1))
        ]
    return df


def sample_rows(df: pd.DataFrame, sample_size: int, seed: int) -> pd.DataFrame:
    if len(df) <= sample_size:
        return df
    # Prefer suspicious cases while still spreading across fonts.
    ranked = df.assign(
        audit_rank=(
            df["final_ok"].astype(int) * 2
            + df["has_vowel_diacritic"].astype(int)
            + df["complexity"].clip(upper=6)
        )
    ).sort_values(["audit_rank", "basename"], ascending=[False, True])
    top = ranked.head(sample_size // 2)
    rest = ranked.drop(top.index).sample(sample_size - len(top), random_state=seed)
    return pd.concat([top, rest]).sort_values(["basename", "stack"])


def render_rows(rows: pd.DataFrame, font_map: dict[str, object], args: argparse.Namespace) -> list[dict[str, str]]:
    image_dir = args.out_dir / "png"
    image_dir.mkdir(parents=True, exist_ok=True)
    manifest = []
    for i, row in enumerate(rows.itertuples(index=False), start=1):
        font = font_map.get(row.basename)
        stack_hash = hashlib.sha1(row.stack.encode("utf-8")).hexdigest()[:10]
        filename = f"{i:04d}_{slugify(row.basename)}_{stack_hash}.png"
        out_png = image_dir / filename
        ok, error = (False, "font row not found")
        if font is not None:
            ok, error = hb_view_render(
                font,
                row.stack,
                out_png,
                hb_view_bin=args.hb_view,
                margin=args.render_margin,
            )
        final_ok = bool(row.final_ok)
        manifest.append(
            {
                "image": str(out_png.relative_to(args.out_dir)),
                "render_ok": str(ok),
                "render_error": error,
                "basename": row.basename,
                "skt_ok": str(row.skt_ok),
                "test_kind": row.test_kind,
                "stack": row.stack,
                "codepoints": row.codepoints,
                "auto_ok": str(row.ok),
                "final_ok": str(final_ok),
                "reason": row.reason,
                "complexity": str(row.complexity),
                "subjoined_count": str(row.subjoined_count),
                "vowel_diacritic_count": str(row.vowel_diacritic_count),
                "placement_warnings": row.placement_warnings,
                "glyph_names": row.glyph_names,
            }
        )
    return manifest


def make_contact_sheet(manifest: list[dict[str, str]], out_dir: Path, *, columns: int) -> Path | None:
    if not manifest:
        return None
    cell_w, cell_h = 460, 380
    image_h = 260
    text_y = image_h + 20
    rows = (len(manifest) + columns - 1) // columns
    sheet = Image.new("RGB", (columns * cell_w, rows * cell_h), "white")
    draw = ImageDraw.Draw(sheet)
    for i, item in enumerate(manifest):
        x = (i % columns) * cell_w
        y = (i // columns) * cell_h
        png_path = out_dir / item["image"]
        if png_path.exists():
            with Image.open(png_path) as img:
                img = img.convert("RGB")
                img.thumbnail((cell_w - 20, image_h))
                sheet.paste(img, (x + 10, y + 10))
        final_ok = item["final_ok"] == "True"
        label_fill = "black" if final_ok else "red"
        if not final_ok:
            draw.rectangle((x + 2, y + 2, x + cell_w - 3, y + cell_h - 3), outline="red", width=3)
        label = (
            f"{i + 1}. {item['basename']} skt={item['skt_ok']} auto={item['auto_ok']} final={item['final_ok']}\n"
            f"{item['stack']} | sub={item['subjoined_count']} vowel={item['vowel_diacritic_count']}\n"
            f"{item['placement_warnings'] or item['codepoints']}"
        )
        draw.multiline_text((x + 10, y + text_y), label, fill=label_fill, spacing=4)
    out_path = out_dir / "contact_sheet.jpg"
    sheet.save(out_path, quality=90)
    return out_path


def write_manifest(manifest: list[dict[str, str]], path: Path) -> None:
    if not manifest:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(manifest[0].keys()))
        writer.writeheader()
        writer.writerows(manifest)


def main() -> None:
    args = parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(args.support_parquet)
    df = ensure_classification_columns(df)
    df = filter_rows(df, args.kind)
    if df.empty:
        raise SystemExit(f"No rows matched audit kind: {args.kind}")
    df = sample_rows(df, args.sample_size, args.seed)

    font_map = {row.basename: row for row in load_font_rows(args.fonts_csv)}
    manifest = render_rows(df, font_map, args)
    manifest_path = args.out_dir / "manifest.csv"
    write_manifest(manifest, manifest_path)
    contact_sheet = make_contact_sheet(manifest, args.out_dir, columns=args.columns)
    print(f"Wrote {manifest_path} ({len(manifest)} rows)")
    if contact_sheet:
        print(f"Wrote {contact_sheet}")


if __name__ == "__main__":
    main()
