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
    conn = psycopg2.connect("dbname='%s' user='%s'" % ('ulan','nschroeder'))
    #print conn.encoding
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
except Exception as e:
    print "Database connection error"
    print e
    exit()

ACC_COL = 35

sql = u''
sql_cols = ''
sql_placeholders = ''
size = 0
limit = 1000000
count = 0
working_cols = []

for line in fin:
    count+=1
    if count < limit:
        #line = unicode( line, "utf-8" )
        cols = line.split('\t')
        cols[-1:] = [cols[-1:][0].strip()]
        if count == 1:
            col_list = []
            # first row is the field names, spit it out
            sql = u"""DROP TABLE if exists wac_object;
    CREATE TABLE wac_object (
    """
            for col in cols:
                col = col.replace(' ','').replace('.','').replace(':','').replace('/','').replace('#','').replace("'",'')
                col = col.replace('nationality','ulan_nationality') # bug in wacart I can't correct till Joe's done
                col = col.replace('Date','date_wac    ') # bug in wacart I can't correct till Joe's done
                col_list.append("{}".format(col))
                sql += u"    {} text,\n".format(col)
            sql = sql[:-2] # remove trailing comma
            sql += u"""
    );
    """
            size = len(col_list)
            sql_cols = ','.join(col_list)
            sql_placeholders = '\t'.join(['%s' for fake in col_list])
            
            sql += u"COPY wac_object ({}) FROM stdin;\n".format(sql_cols)
        else:
            if len(cols) < size:
                cols.insert(size-8,'') # bug in wacart I can't correct till Joe's done
            if cols[ACC_COL] and count > 1:
                out_cols = []
                for c in working_cols:
                    out_cols.append("|||".join(c))
                if len(out_cols) > 0:
                    sql += u"\t".join(out_cols)+"\n"
                working_cols = [[c] for c in cols]
            if not cols[ACC_COL]:
                # append to existing data and make it a list
                for i in range(len(cols)):
                    if cols[i]:
                        working_cols[i].append(cols[i])
                #print working_cols
                
                #working_cols = []
#            str = "{}\n".format(sql_placeholders)
#            #print cur.mogrify(str,cols)
#            sql += cur.mogrify(str,cols)
    if sql:
        fout.write(sql)
    sql = u''
        
sql += "\\.\n\n"
#print sql
fout.write(sql)