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

### From correspondence with Peter Lofting:

> Pierre Robillard and Steve Hartwell created Punakha and Thimphu bitmap fonts in 1988. I created the Druk font in 1989-91.
> 
> The Druk Macintosh was commissioned by the Government of Bhutan, which is why it was never circulated. The system was never updated from the original Mac OS 6.1 version.
> 
> Steve Hartwell and I never assigned our authorship rights, but at the same time we had no interest in setting up as software distributors. The main value of the project was in proving the effectiveness of the script encoding model, which went on to become the Unicode Tibetan Script block.
> 
> \[For the design\] I was given Bhutanese calligraphy samples by the Bhutanese Department of Information and they reviewed and refined the typeface at every stage. Will eventually scan and release some project documents. 

> The Royal Civil Service Commission was teaching typing on the Macintosh at that stage. I think the layout was derived from one of the mechanical typewriters they trained on before that.
>
> Through conversations, my understanding was that the Druk Macintosh keyboard layout was influenced by the layout of a keyboard that the Civil Service were using.  Pre-existing keyboard layouts are relevant for muscle memory, so trying to keep the same arrangement for existing typists was a consideration.

### From conversation with Prof. Imaeda

The Bhutanese government would only allow Bhutanese-specific names (not general Tibetan names) for the fonts we were working on, hence "Druk". When we developed the Otani System for Macintosh 7, we renamed the font "Kailasa". The idea was that fonts were usually named after places.

### From DEVELOPMENT OF CURSIVE BHUTANESE WRITING by KHENPO PHUNTSOK TASHI

https://fid4sa-repository.ub.uni-heidelberg.de/2611/1/12_SpdrPglt.pdf

> In 1989, Kuensel adopted Uchen based on Lopon Pema Tshewang's handwriting and developed it into computer font. 