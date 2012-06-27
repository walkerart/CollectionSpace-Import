#!/usr/bin/env jython
# encoding: utf-8
"""
First draft of import script to dump Walker objects into big honking xml import files.

"""
from __future__ import with_statement

#### Java imports
from java.io import StringWriter
from java.util import List
from java.math import BigInteger
from java.math import BigDecimal

from java.util.regex import Pattern
from java.util.regex import Matcher

from com.ziclix.python.sql import zxJDBC

from org.w3c.dom import Document
from org.w3c.dom import Element
from org.w3c.dom import Node

from org.collectionspace.services.collectionobject import CollectionobjectsCommon

#### Python imports

from utils import *
from collectionobject import *
from personauthority import *
import sys 
reload (sys) 
sys.setdefaultencoding ('utf-8') # lets us pipe the output straight to a file and not pike on utf/ascii conversion

# build the global document and root element:
try:
    dbf = DocumentBuilderFactory.newInstance()
    dbf.setNamespaceAware(True)
    builder = dbf.newDocumentBuilder()
    DOMImplementation = builder.getDOMImplementation()
    doc = DOMImplementation.createDocument(None, "imports", None)
    root = doc.getDocumentElement()
except Exception, e:
    print "Fatal error creating document. Bailing."
    print e

#establish a connection with import db
jdbc_url = "jdbc:postgresql://colbert.walkerart.org/cnr"
username = "nschroeder"
password = ""
driver = "org.postgresql.Driver"

global HOSTNAME
if len(sys.argv) > 1 and sys.argv[1].index('-'):
    limit,offset = sys.argv[1].split('-')
reset_cache = False
if 'reset_cache' in sys.argv:
    reset_cache = True

with zxJDBC.connect(jdbc_url, username, password, driver) as conn:
    with conn:
        with conn.cursor() as cur:
            
            # these need to be cleared between runs if the CS database changed:
            personauthorityinfo = getPersonauthorityInfo()
            sql = "SELECT csid from cs where hostname=? and other_table=?"
            cur.execute(sql,(HOSTNAME,"personauthoritycsid"))
            row = cur.fetchone()
            csidmiss = True
            if reset_cache or (not row or row[0] != personauthorityinfo['csid']):
                sql = "DELETE from cs where hostname=?"
                cur.execute(sql,(HOSTNAME,))
                sql = "INSERT into cs (other_table, other_id, csid, hostname) values (?,?,?,?)"
                cur.execute(sql,("personauthoritycsid",'',personauthorityinfo['csid'],HOSTNAME))
            sql = "select * \
                FROM wac_object \
                ORDER BY ObjectID ASC \
                LIMIT %s \
                OFFSET %s \
                ";
            sql = sql % (limit, offset)
            cur.execute(sql);
            
            rows = cur.fetchall()
            description = cur.description
            for row in rows:
                sys.stderr.write('.')
                data = namedColumns(row,description)
                #sys.stderr.write(str(data))
                cms_id = data['objectid']+data['accessionnumber']+data['title']
                #sys.stderr.write(cms_id)
                # build the collectionobject
                collectionobject = CollectionobjectsCommon()
                collectionobject.setObjectNumber(data['accessionnumber'])
                
                addProductionDatesToObject(collectionobject,data,cur)
                addTitleToObject(collectionobject,data,cur)
                addDimensionsToObject(collectionobject,data,cur)
                addCreatorsToObject(collectionobject,data,cur,doc)
                
                #persist the csid
                sql = "SELECT refname,csid from cs where other_table=? and other_id=?"
                cur.execute(sql,('object',str(cms_id)))
                csidrow = cur.fetchone()
                csid = getCSID()
                if csidrow:
                    csid = csidrow[1]
                else:
                    # put it in the cs table for the next time we see this object:
                    sql = "INSERT into cs (other_table, other_id, csid, refname, hostname) values (?,?,?,?,?)"
                    cur.execute(sql,('object',str(cms_id),csid,'',HOSTNAME))
                
                # collectionobject extensions
                extensions = []
                
                # then the main object, with extensions in the same import wrapper
                addObjectToDom(**{'object':collectionobject,
                      'doc':doc,
                      'type':'CollectionObject',
                      'service':'CollectionObjects',
                      'ns_name':'collectionobjects_common',
                      'ns_uri':'http://collectionspace.org/services/collectionobject',
                      'csid':csid,
                      'extensions':extensions})
            
            
            writeDom(doc)

