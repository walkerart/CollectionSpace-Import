#!/usr/bin/env jython
# encoding: utf-8
"""
Test the jar integration
"""

#### Java imports
from java.io import StringWriter
from java.util import List
from java.math import BigInteger
from java.math import BigDecimal

from java.util.regex import Pattern
from java.util.regex import Matcher

from org.w3c.dom import Document
from org.w3c.dom import Element
from org.w3c.dom import Node

from org.collectionspace.services.person import PersonsCommon
from org.collectionspace.services.person import NationalityList
from org.collectionspace.services.person.local.walkerart import PersonsWalkerart

### python imports
from utils import *
import urllib2
from xml.dom import minidom
import sys


personauthorityinfo = {}
def getPersonauthorityInfo():
    global personauthorityinfo
    if personauthorityinfo:
        return personauthorityinfo
    # use beautifulsoup to get the default person authority info
    global CSPACE_URL
    url = CSPACE_URL+"cspace-services/personauthorities/"
    try:
        dom = minidom.parse(urllib2.urlopen(url))
        
        # bit of fiddling to find the Default Person Authority
        for dname in  dom.getElementsByTagName('displayName'):
            if dname.firstChild.data == 'Default Person Authority':
                csid = dname.parentNode.getElementsByTagName('csid')[0].firstChild.data
                short_identifier = dom.getElementsByTagName('shortIdentifier')[0].firstChild.data
                personauthorityinfo = {'csid':csid,'short_identifier':short_identifier}
                return personauthorityinfo
    except Exception, e:
        sys.stderr.writeln(e)
    sys.stderr.writeln("is the cspace server running??")

def addPersonObjectToDom(person,doc):
    try:
        context = JAXBContext.newInstance(person.getClass())
        marshaller = context.createMarshaller()
        root = doc.getDocumentElement()
            
        import_element = doc.createElement("import")
        import_element.setAttribute("seq", next_seq())
        import_element.setAttribute("CSID", person.getCsid())
        person.setCsid(None)
        import_element.setAttribute("service", "Persons")
        import_element.setAttribute("type", "Person")
        root.appendChild(import_element)
        
        marshaller.marshal(person, import_element)
        schema_element = import_element.getFirstChild()
        schema_element.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:persons_common", "http://collectionspace.org/services/person");
        doc.renameNode(schema_element, "", "schema") # rename root element to "schema", also remove default xmlns
        #cleanup
        schema_element.removeAttribute("xmlns:ns2")
        schema_element.removeAttribute("xmlns")
        schema_element.setAttribute("name","persons_common")
        
#        # add the namespace and prefix to everything
        renameNamespaceRecursive(doc,schema_element,"http://collectionspace.org/services/person","persons_common")

    except Exception, e:
        sys.stderr.writeln(e)


def refnameForPerson(agent_data,cms_data,doc,cur,usename):
    other_id = usename if usename else agent_data['id']
    # look for existing refname
    sql = "SELECT refname,csid from cs where other_table=? and other_id=?"
    cur.execute(sql,('agent',str(other_id)))
    row = cur.fetchone()
    csid = getCSID()
    csidmiss = True
    if row:
        csid = row[1]
        csidmiss = False
    
    if usename:
        # fake person, just a name...
        rows = [usename]
    else:
        # again. Fucking bullshit since we seem to only have one name for anyone.
        sql =  "SELECT first_name, index_name, prefix, suffix, preferred_name \
            FROM names WHERE agent_id = ?"
        cur.execute(sql,(agent_data['id'],) );
        rows = cur.fetchall()
    
    if not usename:
        description = cur.description
    person = PersonsCommon()
    for row in rows:
        # we don't actually loop. There's only ever one. (I found out too late...)
        if not usename:
            data = namedColumns(row,description)
            fwdName = cms_data['creator_text_forward']
            if (data['index_name']):
              # ah, got something, let's use that instead:
              fwdName = data['first_name'] + (" " if data['first_name'] else "") + data['index_name']
            person.setDisplayName(fwdName)
            person.setSalutation(data['prefix'])
            person.setForeName(data['first_name'])
            person.setSurName(data['index_name'])
            person.setNameAdditions(data['suffix'])
            if agent_data['gender']:
                if agent_data['gender'].upper() == 'M':
                    person.setGender("male")
                if agent_data['gender'].upper() == 'F':
                    person.setGender("female")
            person.setBirthPlace(agent_data['birth_place'])
            person.setBirthDate(str(agent_data['start_date']))
            person.setDeathPlace(agent_data['death_place'])
            person.setDeathDate(str(agent_data['end_date']))
            nationalitylist = NationalityList()
            nationalitylist.getNationality().add(agent_data['nationality'])
            person.setNationalities(nationalitylist)
        else:
            # again, just a name
            person.setDisplayName(usename)
            note = "CS Import Script: creator inferred from original string: '%s'" %(cms_data['creator_text_forward'])
            print note
            person.setNameNote(note)
        personauthorityinfo = getPersonauthorityInfo()
        personauthorityname = personauthorityinfo['short_identifier']
        shortid = person.getDisplayName().replace(' ','-')+'-'+csid
        # build and set the refname
        refname = "urn:cspace:walkerart.org:personauthorities:name("+personauthorityname+"):item:"
        refname += "name("+shortid+")'"+person.getDisplayName().replace("'","\'")+"'"
        person.setRefName(refname)
        person.setShortIdentifier(shortid)
        person.setInAuthority(personauthorityinfo['csid'])
        person.setTermStatus('accepted')
        
        person_walker = PersonsWalkerart()
        person_walker.setEmployer("test employer!")

        if csidmiss:
            # new person, add to dom and cs table
            # first the schema extensions
            extensions = []
            extensions.append(addObjectToDom(**{'object':person_walker,
                  'doc':doc,
                  'type':'Person',
                  'service':'Persons',
                  'ns_name':'persons_walkerart',
                  'ns_uri':'http://collectionspace.org/services/person/local/walkerart',
                  'return_schema':True}))
            
            # then the main person object, with extensions in the same import wrapper
            addObjectToDom(**{'object':person,
                  'doc':doc,
                  'type':'Person',
                  'service':'Persons',
                  'ns_name':'persons_common',
                  'ns_uri':'http://collectionspace.org/services/person',
                  'csid':csid,
                  'extensions':extensions})
            # put it in the cs table for the next time we see this person:
            sql = "INSERT into cs (other_table, other_id, csid, refname, hostname) values (?,?,?,?,?)"
            global HOSTNAME
            cur.execute(sql,('agent',str(agent_data['id']),csid,refname,HOSTNAME))
        
        return refname