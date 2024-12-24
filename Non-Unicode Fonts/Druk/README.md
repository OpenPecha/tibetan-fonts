# The Druk Font

Notes on the Druk Font Sources

Uploaded to BDRC by Peter Lofting, 2024-12-23
These sources are made available under the MIT open source license.

The Druk font v1.3 (written v13) was created by Peter Lofting between 1989 and 1991. It was a pre-unicode type-1 postscript font with an 8-bit encoding.

It was used in Thimphu as part of the Druk Macintosh for over 21 years for producing the Dzongkha edition of the Kuensel National newspaper from 1991 to 2012 or later. 
 
I heard from the Kuensel editors that each week’s edition was stored on separate floppies. That’s a lot of weekly dzongkha data that may be recoverable.  

At some point after 2012, the newspaper font changed. Probably when they retired their old Macintoshes and moved to new hardware and software. Chris Fynn probably ported them to a new setup in Libre Office or similar as he was living there for many of those years.

Kuensel now has a website with english and dzongkha editions
   https://kuenselonline.com
   https://kuenselonline.com/category/dzongkha/

In 2020 I regenerated the font with the original encoding for imaging legacy data as V13 and then re-encoded it for Unicode as V14 and updated glyph names to use the dot suffix naming convention for better parsability.

The Druk Font Technical Manual was one of three documentation manuals of the 1991 design for the original project. There was a User, Technical and Design Dependency manual.

The manuals list code points in 8-bit decimal integers. See the scraped spreadsheet. 

I used the V14 font to start re-generating the technical manual in SVG - see Druk-aachung-Dependencies-test-3. This uses an open source tool from https://lindenbergsoftware.com/en/tools/index.html called glyphplotter that can accurately document all the geometry and combining dependencies of any font. 

Note that neither v13 nor v14 fonts have shaping tables. The original Macintosh System 6.1 version Tibetan language kit was articulated by itl5 resource code that contained the shaping rules. The v14 is for re-generating the manual documentation.  A future version will have full shaping.

---oOo---

