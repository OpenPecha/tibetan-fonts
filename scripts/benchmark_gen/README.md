# Tibetan Font Rendering Benchmark (OCR)

This repo automates creation of a Tibetan OCR benchmark sub-set by rendering the same randomized Tibetan text with many fonts on Linux.

For each font you get:

* `out/XXX.txt` — ground-truth text (line by line)
* `out/XXX.png` — **first page only** rasterized from the PDF

and the original files:
* `out/XXX.orig.txt` — ground-truth text (with no line breaks, used as input for rendering)
* `out/XXX.tex` — LaTeX source used for rendering
* `out/XXX.pdf` — multi-page PDF rendered with LuaLaTeX + HarfBuzz


A global `digital_fonts.csv` lists all fonts + metadata.

## Requirements

Linux packages:

```bash
sudo apt-get install -y \
  python3 python3-pip \
  texlive-luatex texlive-xetex texlive-fonts-recommended \
  poppler-utils imagemagick \
  harfbuzz-tools
```

Python deps:

```bash
pip install fonttools pandas
```

## Folder layout

```
fonts/          (your .ttf/.otf/.ttc trees; can be multiple dirs)
texts/          (corpus .txt files in UTF-8)
out/            (generated outputs)
```

## Usage

1. Normalize font file names:

```bash
python3 normalize_fontnames.py /path/to/fonts /path/to/fonts_normalized
```

2. Build font inventory (TTC faces expanded):

```bash
python3 make_fonts_csv.py /path/to/fonts_normalized
```

3. Generate randomized text + TeX per font
   (skips fonts that already have both `.txt` and `.tex` in `out/`):

```bash
python3 generate_texts.py
```

4. Optional: filter fonts that produce `.notdef` with HarfBuzz:

```bash
python3 check_notdef.py
# produces digital_fonts.filtered.csv
```

5. Render PDFs and first-page PNGs:

```bash
bash render_all.sh
```

6. Extract text from PDFs to get the line breaks and make sure it is the same as the original one

```bash
python3 extract_and_qc.py
```

## Regenerating text for a font

Delete the existing files and re-run step 2:

```bash
rm out/XXX.orig.txt out/XXX.tex
python3 generate_texts.py
```

## Notes

* Text samples are built from random “shad units” (།…།) pulled from `texts/`, plus a mandatory hardcoded snippet for coverage.
* PNGs always come from page 1, but PDFs may contain multiple pages.
* TTC fonts are handled face-by-face via `FontIndex` in LaTeX and `--font-index` for HarfBuzz checks.
