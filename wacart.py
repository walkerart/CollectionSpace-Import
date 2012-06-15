#!/usr/bin/env python
# encoding: utf-8

import codecs
import fuzzy
import Levenshtein
import re
import sys 
reload (sys) 
sys.setdefaultencoding ('utf-8') # lets us pipe the output straight to a file and not puke on utf/ascii conversion
import psycopg2
import psycopg2.extras
import psycopg2.extensions
import pickle

ACC_COL = 35
ID_COL = 38
TITLE_COL = 41
ARTIST_COL = 56
BIRTHDATE_COL = 59
BIRTHPLACE_COL = 60
DEATHDATE_COL = 61
DEATHPLACE_COL = 62
NATIONALITY_COL = 67
LASTNAME_COL=69
DEBUG_ARTISTS=False
DEBUG_ULAN=True
PROMPT_ULAN=True

tabfile = sys.argv[1]
fin = open( tabfile, "rU") # codecs open doesn't respect \r newlines
fout = codecs.open( "/tmp/out1.tab", "wt", "utf-8" )

# ok name regex
singlename_1 = re.compile(r"^[^\s]+,?(\s+)[^\s]+$")
singlename_2 = re.compile(r"^[^\s]+,?(\s+)([^\s]+\s+[^\s]+)$")
singlename_3 = re.compile(r"^([^\s]+\s+[^\s]+),?(\s+)[^\s]+$")
couple_shortcut_1 = re.compile(r"^([^\s]+),\s+([^\s]+)\sand\s([^\s]+)$")
splitname_1 = re.compile(r"^([^(]+)(;\s|\sand\s)(.+)$")
splitname_2 = re.compile(r"^([^\s]+(\s+[^\s]+)+)(,\s)([^\s]+(\s+[^\s]+)+)$")

# first/last regex
lastfirst = re.compile(r"^([^,]+),\s+([^,]+)$")
firstlast = re.compile(r"^([^\s]+)\s+([^,]+)$")

dmeta = fuzzy.DMetaphone()

#establish a connection with db
try:
    conn = psycopg2.connect("dbname='%s' user='%s'" % ('ulan','nschroeder'))
    #print conn.encoding
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
except Exception as e:
    print "Database connection error"
    print e
    exit()


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
        artist = "{}, {} and {} {}".format(m.group(1),m.group(2),m.group(3),m.group(1))
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

firstlast_re = re.compile(r"([^,]+),\s+(.+)")
ulan_hit = 0
ulan_miss = 0
done_header = False

def map_to_ulan(data,object_data):
    global ulan_hit
    global ulan_miss
    global done_header
    name = data[ARTIST_COL]
    # ULAN ID, preferred label, nationality, role, birth date & death date
    ret = ['','','','','','']
    
    if not name or 'anonymous' in name.lower() or 'uknown' in name.lower():
        return ret # empty row padding another column, skip
    name = name.replace(',  ',', ') # easy fix: extra space
    
    wac_id = 0
    #return ret # header line or something
    #cur.execute("select pickled_data from ulan_cache where wac_id=%s and artist_name=%s",(wac_id,name))
    cur.execute("select pickled_data from ulan_cache where artist_name=%s",(name,))
    row = cur.fetchone()
    skip = False
    if row and row['pickled_data'] == 'skip':
        skip = True # already skipped, skip again.
    if row and row['pickled_data'] != 'skip':
        return pickle.loads(row['pickled_data'])
    
    
#    m = firstlast_re.match(name)
#    if m:
#        name = "{} {}".format(m.group(2),m.group(1))
    dmname = dmeta(name)
    cur.execute(""" select ulan_id, ulan_name, dmname1, dmname2,
                           case when upper(ulan_name) = upper(%s) then 3
                                when dmname1=%s and dmname2=%s then 2
                                when dmname2=%s and dmname1<>%s then 1
                        else 0 end
                        as score
                    from ulan_names where
                        (upper(ulan_name) = upper(%s)) OR
                        (dmname1=%s and dmname2=%s) OR
                        (dmname2=%s and dmname1<>%s)
                    order by score desc;
    """,(name, dmname[0], dmname[1],dmname[0], dmname[1], name, dmname[0], dmname[1],dmname[0], dmname[1]));
    
    rows = cur.fetchall()
    smallest_distance = 100 
    best_guess = ''
    best_data = []
    found = False
    ulan_person = []
    for data in rows:
        if data['score'] == 3:
            if found:
                if data['ulan_id'] != ulan_person['ulan_id']:
                    # hmm. duplicate name. We should probably ask about this?
                    cur.execute("select * from ulan_person where ulan_id=%s",(data['ulan_id'],))
                    tmp_person = cur.fetchone()
                    if tmp_person['birthdate'] == object_data[BIRTHDATE_COL][0]:
                        print "DUPLICATE NAME AND BIRTHDATE on match: {}".format(name)
                        print ulan_person
                        print tmp_person
            else:
                # awesome. I don't think we can do any better than an exact preferred name match
                best_data = data
                cur.execute("select p.*, r.ulan_role, n.nationality from ulan_person p, ulan_role r, ulan_nationality n where p.ulan_id=%s and r.ulan_id=p.rolePreferred and n.ulan_id=p.nationalityPreferred",(best_data['ulan_id'],))
                ulan_person = cur.fetchone()
                if ulan_person['birthdate'] == object_data[BIRTHDATE_COL][0]:
                    # uber confident
                    found = True
                if not object_data[BIRTHDATE_COL][0]:
                    #print "exact match found but no existing birthdate to compare: {}".format(name)
                    data['score'] = 2
        if data['score'] < 3 and not found:
            #print "{}:{}".format(name,data['ulan_name'])
            distance = Levenshtein.distance(str(name),str(data['ulan_name']))
            if distance < smallest_distance:
                smallest_distance = distance
                best_guess = data['ulan_name']
                best_data = data
    is_hit = False
    if best_data:
        if not ulan_person:
            cur.execute("select p.*, r.ulan_role, n.nationality from ulan_person p, ulan_role r, ulan_nationality n where p.ulan_id=%s and r.ulan_id=p.rolePreferred and n.ulan_id=p.nationalityPreferred",(best_data['ulan_id'],))
            ulan_person = cur.fetchone()
        if best_data['score'] < 3:
            # it's a guess, let's see if it's any good
            if smallest_distance < 3 and object_data[BIRTHDATE_COL][0] == ulan_person['birthdate']: # nothing over 3 worth considering, but birthdates match
            #if smallest_distance < 3: # nothing over 3 worth considering, and only if some metadata matches
                is_hit = True
                #print "For name '{}', we found this name: {}".format(name,best_guess)
                #print "The birthdates match ({}), so we're confidently using '{}'".format(ulan_person['birthdate'], ulan_person['labelpreferred'])
                #print "Born: {}\nNationality: {}\nRole: {}".format(ulan_person['birthdate'],ulan_person['nationality'],ulan_person['ulan_role'])
                #print "=========================================="
        else:
            is_hit = True # 3 = exact match
        if not skip and smallest_distance < 2 and not object_data[BIRTHDATE_COL][0]: # nothing over 3 worth considering, and only if some metadata matches
                #print "++ For name '{}', we found this name: {}".format(name,best_guess)
                print "=========================================="
                print "++ For object {}, '{}'\n++ WAC name '{}'".format(object_data[ACC_COL][0].strip(),object_data[TITLE_COL][0].strip(),name)
                print "++ ULAN match is '{}'".format(ulan_person['labelpreferred'])
                print "++ ULAN id '{}'".format(ulan_person['ulan_id'])
                print "++ Born: {}\n++ Nationality: {}\n++ Role: {}".format(ulan_person['birthdate'],ulan_person['nationality'],ulan_person['ulan_role'])
                print "=========================================="
                if PROMPT_ULAN:
                    val = raw_input("Is this the right person (y/n) or skip? (s) ")
                if not PROMPT_ULAN or 'y' in val.lower():
                    print "keeping {}".format(ulan_person['labelpreferred'])
                    is_hit = True
                if PROMPT_ULAN and 's' in val.lower():
                    print "skipping {}".format(ulan_person['labelpreferred'])
                    skip = True
                print ""
    if not is_hit:
        #print "miss: {}".format(name)
        ulan_miss += 1
    else:
        #print "hit: {}".format(name)
        ulan_hit += 1
        # ULAN ID, preferred label, nationality, role, birth date & death date
        ret = [str(ulan_person['ulan_id']),ulan_person['labelpreferred'],ulan_person['nationality'],ulan_person['ulan_role'],ulan_person['birthdate'],ulan_person['deathdate'] if ulan_person['deathdate'] else '']
    pickled_data = pickle.dumps(ret)
    if skip:
        pickled_data = 'skip'
    cur.execute("insert into ulan_cache (wac_id,artist_name,pickled_data) values (%s,%s,%s)",(wac_id,name,pickled_data))
    conn.commit()
    return ret

if 'reset_cache' in sys.argv:
    cur.execute("delete from ulan_cache where 1=1");

if DEBUG_ARTISTS:
    print explode_artists('Clegg & Guttmann (Michael Clegg and Martin Guttmann) in collaboration with Franz West')
    print explode_artists('Scheier, Edwin and Mary')
    print explode_artists('Charlip, Remy; Ray Johnson, Robert Rauschenberg, and Vera Williams')
    print explode_artists('Vieira da Silva, Maria Helena')
    fin = []
    
for line in fin:
    line = unicode( line, "mac_roman" )
    cols = map(lambda x: x.split('\x1d'), line.split('\t'))
    orig_cols = cols
    if len(cols[ARTIST_COL]) > 1:
        # this never fires. good news: we don't use that "group separator" in the artist field
        print "OMG {}".format(cols[ARTIST_COL])
    original_artist_string = cols[ARTIST_COL][0]
    cols[ARTIST_COL] = explode_artists(cols[ARTIST_COL][0])
    if DEBUG_ARTISTS:
        print "{} ===== {}".format(original_artist_string, ' : '.join(cols[ARTIST_COL]))
    numrows = max(map(lambda x: len(x), cols))
    for i in range(numrows):
        out = []
        for j in range(len(cols)):
            out.append((cols[j][i] if len(cols[j]) > i else '')+('\n' if (j == (len(cols)-1) and i>0) else ''))
        ulan = map_to_ulan(out,orig_cols)
        out[-1:] = [out[-1:][0].strip()]
        out.append(original_artist_string if i == 0 else '')
        out.extend(ulan)
        
        # figure out first/last names
        prefname = ulan[1] if ulan[1] else out[ARTIST_COL]
        if done_header and prefname:
            found_first = False
            m = lastfirst.match(prefname)
            if m:
                out[LASTNAME_COL] = m.group(1)
                out.append(m.group(2)) # firstname
                found_first = True
            m = firstlast.match(prefname) if not m else None
            if m:
                out[LASTNAME_COL] = m.group(2)
                out.append(m.group(1)) # firstname
                found_first = True
                # re-arrange artist name to go last, first
                out[ARTIST_COL] = "{}, {}".format(m.group(2),m.group(1))
            if not found_first:
                out.append('') # no first name
            #print u"{} : {} : {}".format(out[-1:][0],out[LASTNAME_COL],prefname)
        elif done_header and not prefname:
            out.append('') # keep column count the same
            
        if not done_header:
            done_header = True
            add_columns = ['WAC display name', 'ULAN ID', 'preferred label', 'ulan_nationality', 'role', 'birth date', 'death date', 'First Name']
            out = out[:-(len(add_columns)-1)]
            out.extend(add_columns)
        
        out[-1:] = [out[-1:][0]+'\n']
        fout.write('\t'.join(out))

print "Hit rate: {}/{} = {}".format(ulan_hit,(ulan_hit+ulan_miss),(ulan_hit/(ulan_hit+ulan_miss)))
