# Tibetan Font Coverage Report

Tools for measuring which Tibetan font faces can render ordinary Tibetan and complex Sanskrit-style stacks. The scripts are designed for iterative calibration: keep raw shaping evidence in Parquet, review suspicious samples visually, then tune heuristics in later rounds.

Run with the project venv:

```bash
/home/eroux/pvenvs/1/bin/python scripts/coverage_report/build_support_dataset.py --help
```

## Main Outputs

- `stack_support.parquet`: one row per `(font face, probe)` with raw HarfBuzz evidence.
- `stack_support.summary.csv`: one row per `(font face, test kind)` with pass/fail counts, `supported_percent`, and `supported_stacks_percent` for the input stack list.
- `stack_support.matrix.csv`: one row per stack and one column per font, with `1` for supported and `0` for unsupported.
- `font_features.parquet`: static `cmap`, `GSUB`, and `GPOS` evidence per font face.
- `audit/`: rendered PNG samples, a manifest, and a contact sheet for manual false-positive/false-negative review.

The default `bocorpus_stacks.csv` path is `scripts/coverage_report/`. Parquet and font evidence still default to `scripts/coverage_report/out/`.

## Build Dynamic Support Data

Default stack probes come from BoCorpus-derived frequencies: run `get_stacks_from_corpus.py` once to write `scripts/coverage_report/bocorpus_stacks.csv` (columns `stack`, `nb_occurences`, and `hunspell_bo`: the latter is 1 if the stack appears when segmenting at least one syllable form allowed by [hunspell-bo](https://github.com/eroux/hunspell-bo/) `bo.aff`/`bo.dic`, using the same botok normalization and stack tokenization as the corpus pass; see `hunspell_bo_stacks.py`). Then `build_support_dataset.py` uses that path by default for `--mode stacks` or `--mode both`.

The stack file is treated as already normalized in NFD. The loader preserves that text and only skips obvious metadata/non-Tibetan lines. For `.csv` output from `get_stacks_from_corpus.py`, the `stack` column is read (see `read_stacks_path` in `coverage_common.py`).

```bash
/home/eroux/pvenvs/1/bin/python scripts/coverage_report/get_stacks_from_corpus.py
/home/eroux/pvenvs/1/bin/python scripts/coverage_report/build_support_dataset.py --mode both
```

`--mode both` runs ordinary Tibetan probes, Latin digit/punctuation probes, and BoCorpus stack probes. Use `--mode latin` when you only want to check ASCII digits and parentheses. The Latin probe set includes each character individually plus `0123456789` and `(0123456789)`.

To use a plain newline-delimited stack list instead:

```bash
/home/eroux/pvenvs/1/bin/python scripts/coverage_report/build_support_dataset.py \
  --stacks /path/to/tib-stacks_v2.txt \
  --mode both
```

Useful smaller runs:

```bash
# Check normal Tibetan coverage for fonts currently marked skt_ok=0.
/home/eroux/pvenvs/1/bin/python scripts/coverage_report/build_support_dataset.py \
  --mode normal \
  --skt-ok 0 \
  --output scripts/coverage_report/out/normal_skt0.parquet

# Check Latin digits and parentheses only.
/home/eroux/pvenvs/1/bin/python scripts/coverage_report/build_support_dataset.py \
  --mode latin \
  --output scripts/coverage_report/out/latin_digits_punct.parquet

# Test a small stack sample on a few fonts while tuning heuristics.
/home/eroux/pvenvs/1/bin/python scripts/coverage_report/build_support_dataset.py \
  --stacks /path/to/tib-stacks_v2.txt \
  --mode both \
  --limit-fonts 5 \
  --limit-stacks 200
```

The builder uses `uharfbuzz` with `direction=ltr`; Tibetan probes use `script=tibt`, `language=bo`, while non-Tibetan probes such as Latin digits use `script=latn`, `language=en`. Each row records glyph IDs, glyph names, clusters, advances, offsets, bounding box, ink area, HarfBuzz version, existing `skt_ok`, and stack metrics.

When the stack input is `bocorpus_stacks.csv`, the `hunspell_bo` column is stored as `is_standard_tibetan`. If `is_standard_tibetan=0` and a font has `skt_ok=0`, the row is marked invalid without shaping:

```text
ok=False
reason=non_standard_tibetan_for_skt0
support_class=non_standard_for_skt0
```

The main Parquet file is deliberately long-form, not a wide table:

```text
basename | stack | test_kind | ok | reason | glyph_names | offsets | ...
```

That preserves the evidence needed for debugging false positives and false negatives. For simple lookup or spreadsheet use, the builder also writes `stack_support.matrix.csv`:

```text
stack | codepoints | Aathup | BabelStoneTibetan | ...
ཀྲིཾ | U+0F40 U+0FB2 U+0F72 U+0F7E | 1 | 1 | ...
```

## Extract Static Font Features

Static evidence is explanatory, not decisive. It helps identify fonts that advertise Tibetan layout support and mark positioning, especially for vowels and diacritics that are usually handled by `GPOS` rather than precomposed glyphs.

```bash
/home/eroux/pvenvs/1/bin/python scripts/coverage_report/extract_font_features.py
```

Important columns:

- `tibetan_cmap_count`, `tibetan_cmap_coverage`
- `has_gsub`, `gsub_scripts`, `gsub_features`
- `has_gpos`, `gpos_scripts`, `gpos_features`
- `has_mark_positioning`, `has_mark_to_mark`

## Render Audit Sheets

Use audit sheets to look for false positives and false negatives. This is expected to take a few rounds.

```bash
/home/eroux/pvenvs/1/bin/python scripts/coverage_report/render_audit_sheet.py \
  scripts/coverage_report/out/stack_support.parquet \
  --kind complex-pass \
  --sample-size 80
```

By default, audit sampling is random each run and balanced round-robin across fonts, so repeated runs should expose different supported stacks. Pass `--seed 13` or another integer when you need a reproducible sheet.

If the audit script warns that `placement_warning_count` is missing, rebuild the support Parquet first. Audit filtering uses the stored classification in the Parquet file, so older files will not include newer heuristics.

Audit kinds:

- `complex-pass`: automated passes from the stack list with at least two subjoined letters and at least one vowel/diacritic. These are good potential false positives to inspect manually.
- `placement-warning`: rows flagged by placement heuristics, for example top marks colliding or drifting horizontally, a below-mark placed too far horizontally from its base, a bottom-vowel glyph floating around the middle of a tall stack, repeated top diacritics piling up, a subscript swallowed by or detached from the previous stack layer, or many subscript components drawn in the same vertical layer.
- `false-positive`: automated passes that are suspicious because they include hard stacks, vowels/diacritics, or `skt_ok=0` fonts.
- `false-negative`: automated failures in fonts already marked `skt_ok=1`.
- `normal-fail`: ordinary Tibetan probes that failed, useful for checking whether `skt_ok=0` fonts are still safe for normal text.
- `disagreement`: stack rows where the automated result disagrees with the existing broad `skt_ok` flag.
- `mixed`: any sampled rows.

Review the contact sheet and update heuristics or manual override data in a later round. Keep raw Parquet rows stable where possible so derived summaries can be regenerated and compared.

## Current Heuristics

Hard failures:

- HarfBuzz shape error
- `.notdef` glyph
- dotted-circle glyph
- empty shape
- zero ink for a non-empty glyph sequence
- floating bottom-vowel geometry in tall stacks
- horizontal below-mark misalignment, where U+0F35 or U+0F37 is drawn too far outside the base stack's horizontal span
- top mark overlap, where a top mark collides with a top vowel/mark already present in the preceding composite glyph
- top mark horizontal misalignment, where a trailing top-mark cluster is drawn mostly to the side of the base stack
- top diacritic collision, where repeated top marks are drawn with identical geometry
- subscript horizontal misalignment, where a subscript layer is drawn mostly to the side instead of under the previous stack layer
- subscript containment, where the final subscript is drawn entirely inside the previous stack component instead of becoming a lower layer
- subscript overlap, where adjacent subscript layers overlap too much to read as separate vertical layers
- subscript insufficient descent, where the next subscript starts too high relative to the previous subscript layer
- subscript layer collision, where several subscript components occupy the same vertical band instead of descending through the stack

Pass classes:

- `normal_tibetan_ok`: ordinary Tibetan probe passed.
- `latin_digit_punct_ok`: Latin digit/parenthesis probe passed.
- `complex_stack_ok`: stack-list probe passed.
- `fail`: shaped, but one of the hard failures was detected.
- `font_error`: the font face could not be loaded.
- `shape_error`: HarfBuzz failed on the probe.

The floating bottom-vowel check is a first-pass geometry heuristic: when a stack has a bottom vowel such as U+0F71 or U+0F74, late mark glyphs are expected to sit near the lower part of the stack. If their ink stays around the middle or too far inside the preceding stack glyph, the row is flagged with `floating_bottom_vowel`.

The horizontal below-mark check flags cases where U+0F35 or U+0F37 is emitted as a separate glyph but lies mostly outside the horizontal span of the base stack. The row is flagged with `mark_horizontal_misalignment`.

The top mark overlap check flags cases such as U+0F7A plus U+0F7E where a separately emitted top mark overlaps the preceding composite glyph instead of sitting above it, including cases where a top vowel is swallowed by an earlier body glyph. The row is flagged with `top_mark_overlap`.

The top mark horizontal misalignment check flags trailing top-mark clusters that are mostly detached to the left or right of the base stack. The row is flagged with `top_mark_horizontal_misalignment`.

The top diacritic collision check flags repeated top marks, such as multiple U+0F83 signs, when the resulting glyph boxes are identical and therefore pile up in the same position. The row is flagged with `top_diacritic_collision`.

The subscript horizontal misalignment check catches cases where a separate subscript layer, even in a single-subscript stack, is mostly detached to the left or right of the previous stack component. The row is flagged with `subscript_horizontal_misalignment`.

The subscript containment check targets shallower stacks where a final subscript glyph is emitted separately but sits entirely inside the previous stack component. The row is flagged with `subscript_containment`.

The subscript overlap check catches adjacent subscript layers where the later layer descends, but still overlaps too much with the previous layer to read as separate stacked forms. The row is flagged with `subscript_overlap`.

The subscript insufficient descent check catches adjacent subscript layers where the later layer begins nearly aligned with, or above, the previous layer instead of starting lower. The row is flagged with `subscript_insufficient_descent`.

The subscript layer collision check targets very tall stacks with at least four subscript components. If the visible subscript glyphs all start at nearly the same y-position, the row is flagged with `subscript_layer_collision`; this catches renderings where the subscripts overlap into gibberish rather than forming descending layers.

These are first-pass labels. They intentionally keep enough raw evidence to discover additional patterns, especially misplaced bottom vowels, detached marks, and fonts whose glyph sequence looks plausible but renders incorrectly.
