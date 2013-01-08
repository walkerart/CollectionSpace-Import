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
AUTHOR_COL = 57
BIRTHDATE_COL = 59
BIRTHAUTHOR_COL = 60
DEATHDATE_COL = 61
DEATHAUTHOR_COL = 62
GENDER_COL = 21
NATIONALITY_COL = 66
LASTNAME_COL=69

DEBUG_ARTISTS=True
DEBUG_ULAN=True
PROMPT_ULAN=True

ARTIST = True # are we using the artist? False means author.

tabfile = sys.argv[1]
fin = open( tabfile, "rU") # codecs open doesn't respect \r newlines
fout = codecs.open( "/tmp/out1.tab", "wt", "utf-8" )

# ok name regex
singlename_1 = re.compile(r"^[^\s]+,?(\s+)[^\s]+$")
singlename_2 = re.compile(r"^[^\s]+,?(\s+)([^\s]+\s+[^\s]+)$")
singlename_3 = re.compile(r"^([^\s]+\s+[^\s]+),?(\s+)[^\s]+$")
singlename_4 = re.compile(r"^[^\s]+\s+([jJsS]r)\.,?(\s+)([^\s]+\s+[^\s]+)$")
couple_shortcut_1 = re.compile(r"^([^\s]+),\s+([^\s]+)\sand\s([^\s]+)$")
splitname_1 = re.compile(r"^([^(]+)(:\s|;\s|\sand\s)(.+)$")
splitname_2 = re.compile(r"^([^\s]+(\s+[^\s]+)+)(?<![jJsS]r\.)(,\s)([^\s]+(\s+[^\s]+)+)$")

# first/last regex
lastfirst = re.compile(r"^([^,]+),\s+([^,]+)$")
firstlast = re.compile(r"^([^\s]+)\s+([^,]+)$")

# not an artist if it has weird characters in it!
non_artist = re.compile(r".*[:+;\(\[\/\"].*")

# special cases regex
gilbertgeorge = re.compile(r"^Gilbert (&|and) George$")
split_bar = re.compile(r" [|] .+$")
cleggguttmann = "Clegg & Guttmann (Michael Clegg and Martin Guttmann) in collaboration with Franz West"

# some year fields have too much data for us
fixyear = re.compile(r"\d{1,2}\/\d{1,2}\/(\d{4})")

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


comma_normalize_re = re.compile(r",(\s+)?")

def explode_artists(artist, artists = None):
    if DEBUG_ARTISTS:
        print "Starting with '{}'".format(artist)

    # normalize commas on first call: always one space after, no more no less
    artist = comma_normalize_re.sub(", ", artist)

    artist = split_bar.sub('',artist)

    # explicit special cases
    if artist == "Mieko (Chieko) Shiomi":
        return ['Chieko Shiomi']
    if artist == "Ed Ruscha":
        return ['Edward Ruscha']
    if artist == cleggguttmann:
        return ['Michael Clegg','Martin Guttmann','Franz West']
    if artist == 'Peterson, Christian A., Anderson, Simon Christian A. Peterson and Simon Anderson':
        return ['Peterson, Christian A.', 'Simon Anderson']
    if artist == 'Bengston, Goode, Graham, Moses, Price, Ruscha':
        return ['Billy Al Bengston','Joe Goode','Robert Graham','Ed Moses','Kenneth Price','Edward Ruscha']
    if gilbertgeorge.match(artist):
        return ['Gilbert Proesch','George Passmore']
    if not artists:
        artists = []
    artist = artist.strip()
    artist = artist.strip(',')
    if singlename_1.match(artist) or singlename_2.match(artist) or singlename_3.match(artist) or singlename_4.match(artist):
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
    # remove non-artists: 
    artists = [artist for artist in artists if not non_artist.match(artist)]
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
    
    if not name or 'anonymous' in name.lower() or 'unknown' in name.lower():
        return ret # empty row padding another column, skip
    
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
    print "\n\n"
    print explode_artists('Scheier, Edwin and Mary')
    print "\n\n"
    print explode_artists('Charlip, Remy; Ray Johnson, Robert Rauschenberg, and Vera Williams')
    print "\n\n"
    print explode_artists('Vieira da Silva, Maria Helena')
    print "\n\n"
    print explode_artists('Kennedy Jr., Amos Paul')
    print "\n\n"
    print explode_artists('Luchese Jr., Joseph P.')
    print "\n\n"
    print explode_artists('Gilbert & George')
    print "\n\n"
    print explode_artists('Gilbert and George')
    print "\n\n"
    print explode_artists('LeWitt,Sol')
    print "\n\n"
    print explode_artists('Anderson, Graham: Sarah Rara Anderson, Marijke Appelman, Michael G. Bauer, Michelle Blade')
    print "\n\n"
    print explode_artists('Peterson, Christian A., Anderson, Simon Christian A. Peterson and Simon Anderson')
    print "\n\n"
    print explode_artists('Ball, Lillian, Jones, Kristen, + 64 other artists')
    print "\n\n"
    print explode_artists('Bengston, Goode, Graham, Moses, Price, Ruscha')
    print "\n\n"
    print explode_artists('Lawler, Louise: Sherrie Levine, Richard Prince, Cindy Sherman, Laurie Simmons, James Welling')
    print "\n\n"
    print explode_artists('Nordfeldt, Bror Julius Olsson (B.J.O.)')
    print "\n\n"
    print explode_artists('Arakawa (and secondary: Madeline Gins)')
    print "\n\n"
    print explode_artists('George Brecht, George Maciunas, Dick Higgins, Joe Jones, Takako Saito, Alison Knowles, Takehisa Kosugi, Shigeko Kubota, György Ligeti, Jackson Mac Low, Ben Patterson, Tomas Schmit, Mieko (Chieko) Shiomi, Ben Vautier, Robert Watts, Emmett Williams, La Monte Young, Nam June Paik, Sohei Hashimoto, Brion Gysin | Brecht, George; Maciunas, George; Higgins, Dick; Jones, Joe; Saito, Takako; Knowles, Alison; Kosugi, Takehisa; Kubota, Shigeko; Ligeti, György; Mac Low, Jackson; Patterson, Ben; Schmit, Tomas; Shiomi, Mieko (Chieko); Vautier, Ben; Watts, Robert; Williams, Emmett; Young, La Monte; Paik, Nam June; Hashimoto, Sohei; Gysin, Brion')
    print "\n\n"
    print explode_artists('Larry [??] Rivers, Test Name')
    print "\n\n"
    print explode_artists('Ed Ruscha, Test Name')
    print "\n\n"




    
    fin = []

def explode_vt_or_slash(col):
    orig = col
    if len(col) > 1:
        return col # already split
    col = col[0]
    col = col.split('\x0b')# vertical tab
    if len(col) > 1:
        #print "split {} on vt! {}".format(orig,col)
        return col # already split
    col = col[0]
    col = col.split(',')# comma
    if len(col) > 1:
        col = map(lambda x: x.strip(), col)
        #print "split {} on ,! {}".format(orig,col)
        return col # already split
    col = col[0]
    col = fixyear.sub(r"\1",col) # this will only mess with years, and we only have to do it before we match slashes below
    col = col.split('/' )# slash
    #if len(col) > 1:
    #    print "split {} on slash! {}".format(orig,col)
    return col # whatever we've got here is fine, either split or not
    
for line in fin:
    line = unicode( line, "mac_roman" )
    cols = map(lambda x: x.split('\x1d'), line.split('\t'))
    i = 0
    for col in cols:
        print "{} {}".format(i,col)
        i+=1
    fin = []
    orig_cols = cols
    if len(cols[ARTIST_COL]) > 1:
        # this never fires. good news: we don't use that "group separator" in the artist field
        print "OMG {}".format(cols[ARTIST_COL])
    original_artist_string = cols[ARTIST_COL][0]
    cols[ARTIST_COL] = explode_artists(cols[ARTIST_COL][0])
    #print "{} {} {}\n".format(cols[BIRTHDATE_COL],cols[DEATHDATE_COL],cols[NATIONALITY_COL])
    cols[GENDER_COL] = explode_vt_or_slash(cols[GENDER_COL])
    cols[BIRTHDATE_COL] = explode_vt_or_slash(cols[BIRTHDATE_COL])
    cols[DEATHDATE_COL] = explode_vt_or_slash(cols[DEATHDATE_COL])
    cols[NATIONALITY_COL] = explode_vt_or_slash(cols[NATIONALITY_COL])
    if DEBUG_ARTISTS:
        print "{} ===== {}".format(original_artist_string, ' : '.join(cols[ARTIST_COL]))
    numrows = max(map(lambda x: len(x), cols))
    for i in range(numrows):
        out = []
        for j in range(len(cols)):
            out.append((cols[j][i] if len(cols[j]) > i else '')+('\n' if (j == (len(cols)-1) and i>0) else ''))
        ulan = map_to_ulan(out,orig_cols)
        out[-1:] = [out[-1:][0].strip()] # .tab file has trailing CR on last field
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


#0 [u'Condition']
#1 [u'Condition Date']
#2 [u'IAIA Subject']
#3 [u'Running Time']
#4 [u'Width']
#5 [u'Depth']
#6 [u'Height']
#7 [u'Weight']
#8 [u'DimDescription']
#9 [u'Dimensions']
#10 [u'Edition']
#11 [u'Cast No.']
#12 [u'Signature']
#13 [u'Workshop Number']
#14 [u'Signed/location']
#15 [u'Printer\xf8s Marks']
#16 [u'Foundry Marking']
#17 [u'Inscription/location']
#18 [u'Medium']
#19 [u'Support']
#20 [u'Description ']
#21 [u'Sex']
#22 [u'Genre']
#23 [u'IAIA Styly']
#24 [u'Unique frame']
#25 [u'Frame']
#26 [u'Number of Pages']
#27 [u'Vol./No.']
#28 [u'Binding']
#29 [u'Slipcase']
#30 [u'Master']
#31 [u'Submaster']
#32 [u'Portfolio']
#33 [u'Media']
#34 [u'Related material location']
#35 [u'Accession Number']
#36 [u'Old Accession No.']
#37 [u'LC#']
#38 [u'Object ID']
#39 [u'Classification']
#40 [u'Status']
#41 [u'Title']
#42 [u'Credit Line']
#43 [u'Initial Value']
#44 [u'Initial Price']
#45 [u'Current Value']
#46 [u'Valuation Date']
#47 [u'Valuation source']
#48 [u'Source']
#49 [u'Date']
#50 [u'Ct. Raisonne Ref. #']
#51 [u'Fabricator']
#52 [u'Foundry']
#53 [u'Printer']
#54 [u'Publisher']
#55 [u'Editor']
#56 [u'Artist']
#57 [u'Author']
#58 [u'Author birth year']
#59 [u'Born']
#60 [u'Author Death year']
#61 [u'Died']
#62 [u'Author gender']
#63 [u'MN Artist']
#64 [u'Ethnicity']
#65 [u'Author Nationality']
#66 [u'Nationality']
#67 [u'Author place of birth']
#68 [u'Place of Birth']
#69 [u'Last Name']
#70 [u'Reproduction Rights held by:\n']

