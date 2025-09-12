#!/bin/bash

# 1. Check if an input file is provided
if [ $# -eq 0 ]; then
    echo "Error: Please provide a Metafont source file as an argument."
    exit 1
fi

# 2. Get the input file and construct the output filename
SOURCE_MF="$1"
OUTPUT_TTF="${SOURCE_MF%.*}.ttf" 

# 3. Generate the font in Metafont format (.gf)
mf "\mode=ljfour; input $SOURCE_MF"

# 4. Convert .gf to .tfm (TeX font metric)
gftopk ${SOURCE_MF%.*}.gf ${SOURCE_MF%.*}.tfm

# 5. Convert .gf to .pk (packed bitmap font) using a specific resolution (adjust as needed)
gftype -p 300 ${SOURCE_MF%.*}.gf ${SOURCE_MF%.*}.300pk

# 6. Convert .pk to .pt1 (Type 1 font) using a specific resolution (adjust as needed)
pktogf -p 300 ${SOURCE_MF%.*}.300pk ${SOURCE_MF%.*}.pt1

# 7. Convert .pt1 to .ttf (TrueType font)
pt1tot1 ${SOURCE_MF%.*}.pt1 $OUTPUT_TTF

