"""
Valid Tibetan *stacks* (botok tokenize_in_stacks) that appear in at least one
syllable form licensed by [hunspell-bo](https://github.com/eroux/hunspell-bo/) bo.aff + bo.dic.

Hunspell matches whole syllables, not individual stacks, so we expand stems using the
.aff SFX rules (via spylls' parsed data), keep every string σ with ``lookup(σ)``, then
collect stacks from each σ using the same normalization + tokenize_in_stacks as the
corpus step.

The ``ignore_c_s_morphology`` path skips expanding SFX classes C and S (grammatical /
inflectional suffixes) when walk-building candidate strings. New stack shapes from those
chains are often already covered; use ``--full-sfx`` in get_stacks_from_corpus to include
C and S expansion (slower, larger set).
"""

from __future__ import annotations

import sys
from collections import deque
from pathlib import Path
from typing import Iterable, Set

from botok import normalize_unicode, tokenize_in_stacks
from botok.utils.lenient_normalization import normalize_graphical
from spylls.hunspell import Dictionary

from coverage_common import in_tibetan_block


def _apply_suffix_forward(text: str, rule) -> str | None:
    """Add one SFX to the end of *text* if conditions match (forward of spylls' desuffix)."""
    if rule.strip:
        if not text.endswith(rule.strip):
            return None
        base = text[: -len(rule.strip)]
    else:
        base = text
    if not rule.cond_regexp.search(text):
        return None
    return base + rule.add


def _expand_strings(
    d: Dictionary,
    *,
    ignore_c_s: bool,
    max_states: int = 2_000_000,
) -> set[str]:
    """
    BFS on (string, output flags) using bo.aff SFX A/B/C/S, seeded from bo.dic stems.
    Collect every string s with d.lookup(s) (respects NEEDAFFIX, REP, IGNORE, etc.).
    """
    aff = d.aff
    out_words: set[str] = set()
    seen: set[tuple[str, frozenset[str]]] = set()
    q: deque[tuple[str, frozenset[str]]] = deque()
    n_states = 0

    for w in d.dic.words:
        st0 = (w.stem, frozenset(w.flags))
        if st0 not in seen:
            seen.add(st0)
            q.append((w.stem, frozenset(w.flags)))
            n_states += 1

    def consider(text: str) -> None:
        if d.lookup(text):
            out_words.add(text)

    while q and n_states < max_states:
        text, flgs = q.popleft()
        consider(text)
        for cls, rules in aff.SFX.items():
            if ignore_c_s and cls in ("C", "S"):
                continue
            if cls not in flgs:
                continue
            for rule in rules:
                nxt = _apply_suffix_forward(text, rule)
                if nxt is None:
                    continue
                nf = frozenset(rule.flags)
                st1 = (nxt, nf)
                if st1 in seen:
                    continue
                seen.add(st1)
                q.append((nxt, nf))
                n_states += 1
                if n_states >= max_states:
                    break
    return out_words


def _syllable_to_stacks(syllable: str) -> set[str]:
    t = normalize_unicode(syllable)
    t = normalize_graphical(t)
    return {
        x
        for x in tokenize_in_stacks(t)
        if x and all(in_tibetan_block(c) for c in x)
    }


def build_valid_stack_set(
    bo_dir: str | Path,
    *,
    ignore_c_s_morphology: bool = True,
    show_progress: bool = True,
) -> frozenset[str]:
    """
    Return the set of all stacks (pure Tibetan substrings in stack-tokenization) that
    occur in at least one valid hunspell-bo syllable.
    """
    base = Path(bo_dir) / "bo"
    d = Dictionary.from_files(str(base))
    if show_progress:
        print(
            f"  Expanding hunspell-bo stems (SFX C/S expansion: "
            f"{'off' if ignore_c_s_morphology else 'on'})…",
            file=sys.stderr,
        )
    valid_strings = _expand_strings(d, ignore_c_s=ignore_c_s_morphology)
    if show_progress:
        print(
            f"  Valid syllable forms (lookup true): {len(valid_strings):,}",
            file=sys.stderr,
        )
    stacks: Set[str] = set()
    it: Iterable[str] = valid_strings
    if show_progress:
        from tqdm import tqdm

        it = tqdm(
            valid_strings,
            desc="  Stacks from syllables",
            unit="syl",
            file=sys.stderr,
        )
    for s in it:
        stacks |= _syllable_to_stacks(s)
    return frozenset(stacks)
