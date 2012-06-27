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
from org.collectionspace.services.person import PersonTermGroupList
from org.collectionspace.services.person import PersonTermGroup
from org.collectionspace.services.person import StructuredDateGroup
# schema extension
#from org.collectionspace.services.person.local.walkerart import PersonsWalkerart

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
            if dname.firstChild.data == 'Local Persons':
                csid = dname.parentNode.getElementsByTagName('csid')[0].firstChild.data
                short_identifier = dname.parentNode.getElementsByTagName('shortIdentifier')[0].firstChild.data
                personauthorityinfo = {'csid':csid,'short_identifier':short_identifier}
                return personauthorityinfo
    except Exception, e:
        sys.stderr.write(str(e))
    sys.stderr.write("\nis the cspace server running??\n")
    sys.exit()

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


def refnameForPerson(agent_data,cms_data,doc,cur):
    other_id = agent_data['firstname']+agent_data['lastname']+agent_data['web_id']+agent_data['ulanid']
    # look for existing refname
    sql = "SELECT refname,csid from cs where other_table=? and other_id=?"
    cur.execute(sql,('agent',str(other_id)))
    row = cur.fetchone()
    csid = getCSID()
    csidmiss = True
    if row:
        csid = row[1]
        csidmiss = False
        return row[0] # this isn't how the script used to be, but... what's the point of the cache? return it.
    
    person = PersonsCommon()
    person_term_group_list = PersonTermGroupList()
    person_term_groups = person_term_group_list.getPersonTermGroup()
    person_term_group = PersonTermGroup()
    
    fwdName = agent_data['preferredlabel']
    if not fwdName:
      fwdName = agent_data['firstname'] + (" " if agent_data['firstname'] else "") + agent_data['lastname']
    person_term_group.setTermDisplayName(fwdName)
    person_term_group.setTermStatus('accepted')
    # we don't have this info...
    #person.setSalutation(data['prefix'])
    person_term_group.setForeName(agent_data['firstname'])
    person_term_group.setSurName(agent_data['lastname'])
    # we don't have this info...
    #person.setNameAdditions(data['suffix'])
    person_term_groups.add(person_term_group)
    person.setPersonTermGroupList(person_term_group_list)
    
    if agent_data['sex']:
        if agent_data['sex'].upper() == 'M':
            person.setGender("male")
        if agent_data['sex'].upper() == 'F':
            person.setGender("female")
    person.setBirthPlace(agent_data['placeofbirth'])
    
    
    m = get_class("org.collectionspace.services.person.StructuredDateGroup")
    
    bday = agent_data['birthdate'] if agent_data['birthdate'] else agent_data['born']
    person.setBirthDateGroup(setStructuredDateYear(m(),bday))
    
    dday = agent_data['deathdate'] if agent_data['deathdate'] else agent_data['died']
    person.setDeathDateGroup(setStructuredDateYear(m(),dday))
    
    # we don't have this info...
    #person.setDeathPlace(agent_data['placeofdeath'])
    nationalitylist = NationalityList()
    nationalitylist.getNationality().add(agent_data['ulan_ulan_nationality'] if agent_data['ulan_ulan_nationality'] else agent_data['nationality'])
    person.setNationalities(nationalitylist)
        
    personauthorityinfo = getPersonauthorityInfo()
    personauthorityname = personauthorityinfo['short_identifier']
    shortid = fwdName.replace(' ','-')+'-'+csid
    # build and set the refname
    refname = "urn:cspace:walkerart.org:personauthorities:name("+personauthorityname+"):item:"
    refname += "name("+shortid+")'"+fwdName.replace("'","\'")+"'"
    person.setRefName(refname)
    person.setShortIdentifier(shortid)
    person.setInAuthority(personauthorityinfo['csid'])
    
#    person_walker = PersonsWalkerart()
#    person_walker.setEmployer("test employer!")
    
    note = ''
    if agent_data['web_id']:
        # store the old id in the notes field
        note += "WEB ID:%s" % (agent_data['web_id']) + "\n"
    if agent_data['ulanid']:
        # store the old id in the notes field
        note += "ULAN_ID:%s" % (agent_data['ulanid']) + "\n"
    #sys.stderr.write(note+"\n")
    person.setNameNote(note)

    if csidmiss:
        # new person, add to dom and cs table
        # first the schema extensions
        extensions = []
#        extensions.append(addObjectToDom(**{'object':person_walker,
#              'doc':doc,
#              'type':'Person',
#              'service':'Persons',
#              'ns_name':'persons_walkerart',
#              'ns_uri':'http://collectionspace.org/services/person/local/walkerart',
#              'return_schema':True}))
        
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
        cur.execute(sql,('agent',other_id,csid,refname,HOSTNAME))
        
    return refname