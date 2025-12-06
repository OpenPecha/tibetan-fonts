#!/usr/bin/env bash
set -uo pipefail

FONTS_CSV="${1:-digital_fonts.filtered.csv}"   # allow override
OUTDIR="out"
LOGDIR="$OUTDIR/latex_logs"
SUMMARY="$OUTDIR/render_summary.csv"

mkdir -p "$OUTDIR" "$LOGDIR"

echo "basename,status,latex_exit,notes" > "$SUMMARY"

render_one () {
  local basename="$1"
  local dpi="$2"

  local tex_path="$OUTDIR/${basename}.tex"
  local pdf_path="$OUTDIR/${basename}.pdf"
  local png_path="$OUTDIR/${basename}.png"
  local log_path="$LOGDIR/${basename}.log"
  local err_path="$LOGDIR/${basename}.err"

  if [[ ! -f "$tex_path" ]]; then
    echo "!! Missing TeX for $basename ($tex_path). Skipping."
    echo "$basename,skip,NA,missing_tex" >> "$SUMMARY"
    return 0
  fi

  echo "LaTeX $basename"

  lualatex \
    -interaction=nonstopmode \
    -halt-on-error \
    -file-line-error \
    -output-directory="$OUTDIR" \
    "$tex_path" \
    >"$log_path" 2>"$err_path"
  local latex_exit=$?

  if [[ $latex_exit -ne 0 || ! -f "$pdf_path" ]]; then
    echo "!! LaTeX FAILED for $basename (exit=$latex_exit)"
    echo "   log: $log_path"
    echo "   --- tail log ---"
    tail -n 20 "$log_path" | sed 's/^/   /'
    echo "   ---------------"
    echo "$basename,fail,$latex_exit,latex_error" >> "$SUMMARY"
    return 0
  fi

  echo "PNG  $basename"

  if ! pdftoppm -f 1 -l 1 -png -r "$dpi" \
        "$pdf_path" "$OUTDIR/${basename}" \
        > /dev/null 2>&1; then
    echo "!! pdftoppm FAILED for $basename"
    echo "$basename,fail,$latex_exit,pdftoppm_error" >> "$SUMMARY"
    return 0
  fi

  if [[ -f "$OUTDIR/${basename}-1.png" ]]; then
    mv "$OUTDIR/${basename}-1.png" "$png_path"
  else
    echo "!! pdftoppm produced no PNG for $basename"
    echo "$basename,fail,$latex_exit,no_png" >> "$SUMMARY"
    return 0
  fi

  echo "$basename,ok,$latex_exit," >> "$SUMMARY"
  return 0
}

# Correct CSV parsing: feed CSV as stdin *to python code passed via -c*
python3 -c '
import csv, sys
r = csv.DictReader(sys.stdin)
for row in r:
    b = row.get("basename","").strip()
    dpi = row.get("dpi","300").strip()
    if b:
        print(b + "\t" + dpi)
' < "$FONTS_CSV" | while IFS=$'\t' read -r basename dpi; do
  render_one "$basename" "$dpi"
done

echo
echo "Done. Summary: $SUMMARY"
echo "Logs: $LOGDIR/"
