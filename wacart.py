#!/usr/bin/env python
# encoding: utf-8

import codecs
import re
import sys 
reload (sys) 
sys.setdefaultencoding ('utf-8') # lets us pipe the output straight to a file and not puke on utf/ascii conversion

ID_COL = 36
ARTIST_COL = 56
DEBUG_ARTISTS=False

tabfile = sys.argv[1]
fin = open( tabfile, "rU") # codecs open doesn't respect \r newlines
fout = codecs.open( "/tmp/out.tab", "wt", "utf-8" )

# ok name regex
singlename_1 = re.compile(r"^[^\s]+,?(\s+)[^\s]+$")
singlename_2 = re.compile(r"^[^\s]+,?(\s+)([^\s]+\s+[^\s]+)$")
singlename_3 = re.compile(r"^([^\s]+\s+[^\s]+),?(\s+)[^\s]+$")
couple_shortcut_1 = re.compile(r"^([^\s]+),\s+([^\s]+)\sand\s([^\s]+)$")
splitname_1 = re.compile(r"^([^(]+)(;\s|\sand\s)(.+)$")
splitname_2 = re.compile(r"^([^\s]+(\s+[^\s]+)+)(,\s)([^\s]+(\s+[^\s]+)+)$")

def explode_artists(artist, artists = None):
    if not artists:
        artists = []
    artist = artist.strip()
    artist = artist.strip(',')
    if singlename_1.match(artist) or singlename_2.match(artist) or singlename_3.match(artist):
        if DEBUG_ARTISTS:
            print "this is a singlename: {}".format(artist)
        artists.append(artist)
    m = couple_shortcut_1.match(artist)
    if m:
        artist = "{}, {} and {} {}".format(m.group(1),m.group(2),m.group(3),m.group(2))
    m = splitname_1.match(artist)
    g1 = g2 = ''
    if m:
        g1 = m.group(1)
        g2 = m.group(3)
    else:
        m = splitname_2.match(artist)
        if m:
            g1 = m.group(1)
            g2 = m.group(4)
    if g1 and g2:
        if DEBUG_ARTISTS:
            print "this is a splitname: {}: {} ||||| {}".format(artist, g1, g2)
        artists.extend(explode_artists(g1))
        artists.extend(explode_artists(g2))
    if not artists:
        # something weird (single name, parens, etc)
        artists.append(artist)
    return artists

if DEBUG_ARTISTS:
    print explode_artists('Clegg & Guttmann (Michael Clegg and Martin Guttmann) in collaboration with Franz West')
    print explode_artists('Scheier, Edwin and Mary')
    print explode_artists('Charlip, Remy; Ray Johnson, Robert Rauschenberg, and Vera Williams')
    print explode_artists('Vieira da Silva, Maria Helena')
    fin = []
    
for line in fin:
    line = unicode( line, "mac_roman" )
    cols = map(lambda x: x.split('\x1d'), line.split('\t'))
    if len(cols[ARTIST_COL]) > 1:
        print "OMG {}".format(cols[ARTIST_COL])
    original_artist_string = cols[ARTIST_COL][0]
    cols[ARTIST_COL] = explode_artists(cols[ARTIST_COL][0])
    #print "{} ===== {}".format(original_artist_string, ' : '.join(cols[ARTIST_COL]))
    numrows = max(map(lambda x: len(x), cols))
    for i in range(numrows):
        out = []
        for j in range(len(cols)):
            out.append((cols[j][i] if len(cols[j]) > i else '')+('\n' if (j == (len(cols)-1) and i>0) else ''))
        fout.write('\t'.join(out))
