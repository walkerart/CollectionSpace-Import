#!/usr/bin/env python
# encoding: utf-8
"""
Temp script to convert a .tab doc to a .sql doc so we can use it more places.

"""
#### Python imports

import sys
import codecs
import psycopg2
import psycopg2.extras
import psycopg2.extensions
reload (sys) 
sys.setdefaultencoding ('utf-8') # lets us pipe the output straight to a file and not pike on utf/ascii conversion

tabfile = sys.argv[1]
fin = open( tabfile, "rU") # codecs open doesn't respect \r newlines
fout = codecs.open( "/tmp/out1.sql", "wt", "utf-8" )


#establish a connection with db
try:
    conn = psycopg2.connect("dbname='%s' user='%s' host='%s'" % ('cnr','nschroeder','colbert.walkerart.org'))
    #print conn.encoding
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
except Exception as e:
    print "Database connection error"
    print e
    exit()

ACC_COL = 35
OBJECTID_COL = 38
TITLE_COL = 41
FIRSTNAME_COL=78
LASTNAME_COL=69

sql = u''
wac_obj_placeholders = ''
size = 0
limit = 10000000
count = 0
objectid = ''
full_col_list = []
wac_object_col_list = []
related_tables = {'wac_condition':[
                               'Condition',
                               'ConditionDate'
                               ],
                  'wac_artist':[
                            'Artist',
                            'Sex',
                            'birthdate',
                            'deathdate',
                            'Born',
                            'Died',
                            'FirstName',
                            'LastName',
                            'PlaceofBirth',
                            'preferredlabel',
                            'role',
                            'ULANID',
                            'Nationality',
                            'ulan_ulan_nationality',
                            ],
                  'wac_subject':[
                            'IAIASubject',
                            ],
                  'wac_valuation':[
                               'ValuationDate',
                               'Valuationsource',
                               'CurrentValue'
                               ],
                  'wac_dimensions':[
                               'Width',
                               'Height',
                               'Depth',
                               'Weight',
                               'DimDescription',
                               'Dimensions',
                               ],
                  }
create_tables = {}
create_data = {}

for line in fin:
    count+=1
    if count < limit:
        #line = unicode( line, "utf-8" )
        cols = line.split('\t')
        cols[-1:] = [cols[-1:][0].strip()]
        sys.stderr.write("line {}\n".format(str(count)))
        if count == 1:
            col_list = []
            # first row is the field names, spit it out
            sql = u""
            
            for col in cols:
                col = col.replace(' ','').replace('.','').replace(':','').replace('/','').replace('#','').replace("'",'')
                col = col.replace('nationality','ulan_nationality') # bug in wacart I can't correct till Joe's done
                if col == 'Date':
                    col = 'date_wac'
                col = col.replace('PrinterøsMarks','PrintersMarks') # bug in wacart I can't correct till Joe's done
                full_col_list.append("{}".format(col))
                wac_object = True
                for key, val in related_tables.items():
                    if col in val:
                        wac_object = False
                        if key not in create_tables:
                            create_tables[key] = ['wac_object_id']
                            if key == 'wac_artist':
                                create_tables[key].append('web_id')
                        create_tables[key].append(col)
                        #sys.stderr.write("SKIP %s\n" % (col))
                        # store these in order, I reckon, so we can create the table and store the pieces
                if wac_object:
                    if 'wac_object' not in create_tables:
                        create_tables['wac_object'] = ['id','web_id']
                    create_tables['wac_object'].append(col)
                    
                    wac_object_col_list.append("{}".format(col))
                    
                    
            
            # generate the sql
            for tablename,tablecols in create_tables.items():
                sql += u"""DROP TABLE if exists {};
CREATE TABLE {} (\n""".format(tablename,tablename)
                for tablecol in tablecols:
                    sql += u"    {} text,\n".format(tablecol)
                sql = sql[:-2] # remove trailing comma
                sql += u"""
);\n"""
#            size = len(col_list)
            
            
        else:
            # populate the hash to write out at the end
            if cols[OBJECTID_COL] or cols[ACC_COL] or cols[TITLE_COL]:
                objectid = cols[OBJECTID_COL]
                if not objectid:
                    # use the accession number
                    objectid = cols[ACC_COL]
                if not objectid:
                    # um. What the heck kind of object IS this?               
                    objectid = cols[TITLE_COL].strip().replace(' ','')
                    sys.stderr.write("=========== WTF{}\n".format(objectid))
            #sys.stderr.write("{}\n".format(objectid))
            data_cols = {}
            for i in range(len(cols)):
                for key, val in create_tables.items():
                    if full_col_list[i] in val:
                        #sys.stderr.write("{} goes in {}\n".format(full_col_list[i], key))
                        if key not in data_cols:
                            data_cols[key] = [objectid]
                            if key == 'wac_object':
                                wacid=''
                                if cols[OBJECTID_COL]:
                                    cur.execute("select id from object where cms_id=%s",(cols[OBJECTID_COL],))
                                    row = cur.fetchone()
                                    if row and row['id']:
                                        wacid = str(row['id'])
                                data_cols[key].append(wacid)
                            if key == 'wac_artist':
                                wacid=''
                                if cols[LASTNAME_COL]:
                                    #sys.stderr.write("{} {}\n".format(cols[FIRSTNAME_COL],cols[LASTNAME_COL]))
                                    cur.execute("select agent_id from names where upper(first_name) like upper(%s) and upper(index_name) like (%s)",(cols[FIRSTNAME_COL],cols[LASTNAME_COL]))
                                    row = cur.fetchone()
                                    if row and row['agent_id']:
                                        wacid = str(row['agent_id'])
                                data_cols[key].append(wacid)
                        data_cols[key].append(cols[i])
            
            for key, val in data_cols.items():
                has_data = False
                for testcol in val[1:]:
                    if testcol:
                        has_data = True
                if has_data:
                    if key not in create_data:
                        create_data[key] = ''
                    create_data[key] += u"\t".join(val)+"\n"
                        
for key, val in create_tables.items():
    sql += u"COPY {} ({}) FROM stdin;\n".format(key,','.join(val))
    sql += create_data[key].replace('\x0b','\\r') #replace vertical tabs with carriage return marker
    sql += "\\.\n\n"
if sql:
    fout.write(sql)
