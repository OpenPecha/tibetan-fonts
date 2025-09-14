#!/bin/sh

mydir=`dirname "${0}"`
#    --input "$mydir"/GP.Druk_PageSetup.txt \

# ------- FontLab VI automatically inserts uniXXXX names where it can match glyph names
echo "Fusing groomed glyph names"
ftxdumperfuser -t post -d Drukv14-groomed.post.xml "$mydir"/DrukFonV14.ttf

# ------- Up the version to 14 and rename the font more consistently
echo "Fusing cleaned name strings"
ftxdumperfuser -t name -d Druk14.name.xml "$mydir"/DrukFonV14.ttf

