#!/usr/bin/env python3
import csv, os, random, pathlib, sys, re

TEXT_DIR = "texts"
OUT_DIR = "out"
TEX_TEMPLATE = "render.tex"

# Hardcoded mandatory string (exactly as provided)
MANDATORY = (
"ཀུན་མཁྱེན་གྲུབ་དངོས་གཅིག་མཆོག་རྗེ་ཉིད་སྟོན། "
"།ཐུགས་དག་རྣམ་དཔྱོད་འཕྲུལ་དབང་སྨྲ་རྩོམ་མཚྭར། "
"།འཚོ་མཛད་ཝེར་ཞིའི་ཟླ་འོད་ཡེ་རིཊ་ལེཌ༑ ༈ "
"།གཤེན་སྲས་ལྷ་རྗེའི་" "ཨོཾ་ཨཱཿ ཧཱུཾ་ལས་འཁྲུངས།། "
"༠༡༢༣༤༥༦༧༨༩"
)

SHAD = "།"
TSHEG = "་"
MIN_UNITS = 60      # number of shad-units to grab (approx "shortish sentences")
MAX_UNITS = 90
MIN_CHARS = 4500    # ensures multi-page likely
MAX_CHARS = 6000
MIN_UNIT_LENGTH = 20  # minimum character length for units

# Unicode characters to filter when skt_ok is 0
SKT_FILTER_CHARS = [
    '\u0F71',  # ཱ
    '\u0F7B',  # ཻ
    '\u0F7D',  # ཽ
    '\u0F83',  # ྃ
    '\u0F75',  # ཱུ
    '\u0FB0',  # ྰ
    "ཛྲ",
    "དྡྷ",
    "ཅྖ"
]

def contains_skt_chars(text):
    """Check if text contains any SKT filter characters."""
    return any(char in text for char in SKT_FILTER_CHARS)

def load_corpus_units(text_dir, filter_skt=False):
    units = []
    page_pattern = re.compile(r'^--\s*page\s+\d+\s*--\s*$', re.IGNORECASE)
    
    for p in pathlib.Path(text_dir).glob("*.txt"):
        txt = p.read_text(encoding="utf-8")
        txt = txt.replace("\r\n","\n").replace("\r","\n")
        txt = re.sub("༄༅། ?།?", "", txt)
        
        # Filter out page markers and remove header markers from line beginnings
        lines = []
        for line in txt.split("\n"):
            # Skip lines matching "-- page X --" pattern
            if page_pattern.match(line.strip()):
                continue
            lines.append(line)
        
        txt = "\n".join(lines)
        
        # split into shad units: after SHAD up to next SHAD
        parts = txt.split(SHAD)
        # rebuild units with ending SHAD
        for i in range(1, len(parts)):  # start after first shad boundary
            u = parts[i].strip()
            if not u:
                continue
            unit = u + SHAD
            
            # Filter: ignore units less than MIN_UNIT_LENGTH characters
            if len(unit) < MIN_UNIT_LENGTH:
                continue
            
            # Filter: ignore units with multiple consecutive tshegs (་་་་་་)
            if TSHEG * 2 in unit:  # Check for 2+ consecutive tshegs
                continue
            
            # Filter: reject units containing any Latin letter (a-z, A-Z)
            if re.search(r'[a-zA-Z]', unit):
                continue
            
            # Filter: if filter_skt is True, reject units with SKT characters
            if filter_skt and contains_skt_chars(unit):
                continue
            
            # keep units that look "sentence-ish"
            if len(unit) <= 220:
                units.append(unit)
    return units

def build_random_text(units, filter_skt=False):
    n_units = random.randint(MIN_UNITS, MAX_UNITS)
    chosen = [random.choice(units) for _ in range(n_units)]
    
    # Clean and prepare mandatory segment
    mandatory_clean = MANDATORY.replace("\n", " ").replace("\r", " ").strip()
    
    # If filter_skt is True, skip MANDATORY if it contains SKT characters
    if not (filter_skt and contains_skt_chars(mandatory_clean)):
        # Shuffle mandatory segment into the mix instead of always putting it first
        chosen.append(mandatory_clean)
    
    random.shuffle(chosen)
    
    body = " ".join(chosen).strip()

    # enforce length bounds by adding more units if too short
    while len(body) < MIN_CHARS:
        body += " " + random.choice(units)
    body = body[:MAX_CHARS]

    # Remove all line breaks from body
    body = body.replace("\n", " ").replace("\r", " ")
    
    # Normalize spaces: remove spaces after tshegs (་)
    body = re.sub(TSHEG + r'\s+', TSHEG, body)
    
    # Collapse multiple spaces
    body = re.sub(r'\s+', ' ', body).strip()

    if not body.startswith("༄"):
        body = "༄༅། །" + body
    
    return body

def add_tibetan_breakpoints(s: str) -> str:
    # 1) tsheg: allow break ONLY if not followed by shad
    #    (negative lookahead for U+0F0D)
    s = re.sub("་(?!།)", "་\\\\allowbreak{}", s)

    # 2) shad: add visible sentence gap + allow break
    s = s.replace("། ", "།\\hspace{0.35em}\\allowbreak{}")

    return s

def escape_tex(s):
    # minimal TeX escaping for ASCII specials; Tibetan unaffected
    return (s.replace('\\', '\\textbackslash{}')
             .replace('%','\\%').replace('&','\\&').replace('#','\\#')
             .replace('_','\\_').replace('{','\\{').replace('}','\\}'))

def add_tibetan_breakpoints(s: str) -> str:
    # tsheg: allow break + marker unless followed by shad
    s = re.sub("་(?!།)", r"་\\syllmark\\allowbreak{}", s)
    # shad: marker + visible gap + break
    s = s.replace(" ", "\\syllmark\\hspace{0.35em}\\allowbreak{}")
    return s

def make_tex(template_str, base, font_path, font_size_pt, body, ttc_face_index=""):
    body_escaped = escape_tex(body)
    body_escaped = add_tibetan_breakpoints(body_escaped)

    font_dir = os.path.dirname(font_path)
    if font_dir and not font_dir.endswith("/"):
        font_dir += "/"
    font_fileonly = os.path.basename(font_path)

    tex = template_str
    tex = tex.replace("FONTDIR", font_dir)
    tex = tex.replace("FONTFILEONLY", font_fileonly)
    tex = tex.replace("FONTSIZE", str(font_size_pt))
    tex = tex.replace("TEXTBODY", body_escaped)
    tex = tex.replace("GTOUTFILE", f"out/{base}.txt")
    tex = tex.replace("GTBREAKFILE", f"out/{base}.breaks")

    # Robust TTC face insertion: append FontIndex inside first option block
    if ttc_face_index != "":
        tex = tex.replace("RawFeature={script=tibt}",
                          f"RawFeature={{script=tibt}},FontIndex={ttc_face_index}", 1)
    return tex

def main(csv_in="digital_fonts.filtered.csv"):
    os.makedirs(OUT_DIR, exist_ok=True)
    template = pathlib.Path(TEX_TEMPLATE).read_text(encoding="utf-8")

    with open(csv_in, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        
        # Process each row
        for row in rows:
            base = row["basename"]
            font_path = row["font_path"]
            face_index = row.get("ttc_face_index","")
            font_size_pt = int(row["font_size_pt"])
            
            # Read skt_ok column and determine if we should filter SKT characters
            skt_ok = row.get("skt_ok", "1")
            filter_skt = (skt_ok == "0")
            
            out_txt = pathlib.Path(OUT_DIR) / f"{base}.orig.txt"
            out_tex = pathlib.Path(OUT_DIR) / f"{base}.tex"

            # rule 1: if both exist, skip
            if out_txt.exists() and out_tex.exists():
                continue

            # rule 2: if .txt exists but .tex doesn't, read .txt and generate .tex
            if out_txt.exists() and not out_tex.exists():
                body = out_txt.read_text(encoding="utf-8").strip()
                tex = make_tex(template, base, font_path, font_size_pt, body, face_index)
                out_tex.write_text(tex, encoding="utf-8")
                continue

            # rule 3: otherwise, generate both files
            # Load corpus units with SKT filtering if needed
            units = load_corpus_units(TEXT_DIR, filter_skt=filter_skt)
            if not units:
                print(f"No usable shad units found in texts/ for {base} (filter_skt={filter_skt})", file=sys.stderr)
                continue

            body = build_random_text(units, filter_skt=filter_skt)
            out_txt.write_text(body + "\n", encoding="utf-8")

            tex = make_tex(template, font_path, font_size_pt, body, face_index)
            out_tex.write_text(tex, encoding="utf-8")

if __name__ == "__main__":
    main()
