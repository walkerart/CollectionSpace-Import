#!/usr/bin/env jython
# encoding: utf-8
"""
Test the jar integration

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

with zxJDBC.connect(jdbc_url, username, password, driver) as conn:
    with conn:
        with conn.cursor() as cur:
            
            # these need to be cleared between runs if the CS database changed:
            personauthorityinfo = getPersonauthorityInfo()
            sql = "SELECT csid from cs where hostname=? and other_table=?"
            cur.execute(sql,(HOSTNAME,"personauthoritycsid"))
            row = cur.fetchone()
            csidmiss = True
            if not row or row[0] != personauthorityinfo['csid']:
                sql = "DELETE from cs where hostname=?"
                cur.execute(sql,(HOSTNAME,))
                sql = "INSERT into cs (other_table, other_id, csid, hostname) values (?,?,?,?)"
                cur.execute(sql,("personauthoritycsid",'',personauthorityinfo['csid'],HOSTNAME))
            sql = "select cms_id, \
                  amico_id, creator_text_forward, \
                  creator_text_inverted, creation_date_text, \
                  creation_start_year, creation_end_year, \
                  creation_place, materials_techniques_text, \
                  measurement_text, inscriptions_marks, \
                  work_state, edition, \
                  printer, publisher, \
                  physical_description, accession_number, \
                  old_accession_number, call_no, \
                  credit_line, copyright_permission, \
                  copyright, copyright_link_id, \
                  da_link_id, technology_used, \
                  department, title.title \
                FROM object, title \
                WHERE title.object_id = object.id \
                  AND title.title_type = 'P' \
                ORDER BY object.id ASC \
                LIMIT 10 \
                ";
                
            cur.execute(sql);
            
            rows = cur.fetchall()
            description = cur.description
            for row in rows:
                sys.stderr.write('.')
                data = namedColumns(row,description)
                #print data
                cms_id = data['cms_id']
                
                # build the collectionobject
                collectionobject = CollectionobjectsCommon()
                collectionobject.setObjectNumber(data['accession_number'])
                
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
                    # put it in the cs table for the next time we see this person:
                    sql = "INSERT into cs (other_table, other_id, csid, refname, hostname) values (?,?,?,?,?)"
                    cur.execute(sql,('object',str(cms_id),csid,'',HOSTNAME))
                collectionobject.setCsid(csid)
                
                # put it in the xml we're building
                addCollectionObjectToDom(collectionobject,doc)
            
            
            writeDom(doc)

