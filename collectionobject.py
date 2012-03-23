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

from org.collectionspace.services.collectionobject import DimensionSubGroupList
from org.collectionspace.services.collectionobject import DimensionSubGroup
from org.collectionspace.services.collectionobject import MeasuredPartGroupList
from org.collectionspace.services.collectionobject import MeasuredPartGroup
from org.collectionspace.services.collectionobject import TitleGroupList
from org.collectionspace.services.collectionobject import TitleGroup
from org.collectionspace.services.collectionobject import ObjectProductionPersonGroupList
from org.collectionspace.services.collectionobject import ObjectProductionPersonGroup
from org.collectionspace.services.collectionobject import ObjectProductionDateGroupList
from org.collectionspace.services.collectionobject import StructuredDateGroup

### python imports
from utils import *
from personauthority import refnameForPerson

######################################################### XML population

def addCollectionObjectToDom(collectionobject,doc):
    try:
        context = JAXBContext.newInstance(collectionobject.getClass())
        marshaller = context.createMarshaller()
        root = doc.getDocumentElement()
            
        import_element = doc.createElement("import")
        import_element.setAttribute("seq", next_seq())
        import_element.setAttribute("CSID", collectionobject.getCsid())
        collectionobject.setCsid(None)
        import_element.setAttribute("service", "CollectionObjects")
        import_element.setAttribute("type", "CollectionObject")
        root.appendChild(import_element)
        
        marshaller.marshal(collectionobject, import_element)
        schema_element = import_element.getFirstChild()
        schema_element.setAttributeNS("http://www.w3.org/2000/xmlns/", "xmlns:collectionobjects_common", "http://collectionspace.org/services/collectionobject");
        doc.renameNode(schema_element, "", "schema") # rename root element to "schema", also remove default xmlns
        #cleanup
        schema_element.removeAttribute("xmlns:ns2")
        schema_element.removeAttribute("xmlns")
        schema_element.setAttribute("name","collectionobjects_common")
        
#        # add the namespace and prefix to everything
        renameNamespaceRecursive(doc,schema_element,"http://collectionspace.org/services/collectionobject","collectionobjects_common")

    except Exception, e:
        print e
        
def addDimensionsToObject(collectionobject,cms_data,cur):
    sql = "SELECT m.measurement_type, m.unit, m.value \
    FROM measurement m WHERE m.object_id=? order by m.id desc"
    
    cur.execute(sql, (cms_data['cms_id'],) )
    
    rows = cur.fetchall()
    if len(rows) > 0:
        measured_part_group_list = MeasuredPartGroupList()
        measured_part_groups = measured_part_group_list.getMeasuredPartGroup() 
        measured_part_group = MeasuredPartGroup()
        measured_part_group.setDimensionSummary(cms_data['measurement_text'])
        dimension_sub_group_list = DimensionSubGroupList()
        dimension_sub_groups = dimension_sub_group_list.getDimensionSubGroup()
        
        description = cur.description
        for row in rows:
            data = namedColumns(row,description)
            dimension_sub_group = DimensionSubGroup()
            mtype = data['measurement_type'].upper()
            measure_type = "width" if (mtype == 'W') else "height" if (mtype == 'H') else 'D'
            dimension_sub_group.setDimension(measure_type)
            dimension_sub_group.setMeasurementUnit("inches");
            dimension_sub_group.setMeasurementMethod("ruler")
            dimension_sub_group.setValue(BigDecimal(str(data['value'])))
            dimension_sub_groups.add(dimension_sub_group)
        measured_part_groups.add(measured_part_group)
        measured_part_group.setDimensionSubGroupList(dimension_sub_group_list)
        collectionobject.setMeasuredPartGroupList(measured_part_group_list)
    
def addProductionDatesToObject(collectionobject,cms_data,cur):
    # Production dates
    structured_date_group = StructuredDateGroup()
    structured_date_group.setScalarValuesComputed(False)
    if cms_data['creation_end_year']:
        structured_date_group.setDateLatestYear(BigInteger(str(cms_data['creation_end_year'])))
    structured_date_group.setDateEarliestSingleEra("urn:cspace:walkerart.org:vocabularies:name(dateera):item:name(ce)'CE'")
    structured_date_group.setDateDisplayDate(cms_data['creation_date_text'])
    structured_date_group.setDateEarliestSingleCertainty("urn:cspace:walkerart.org:vocabularies:name(datecertainty):item:name(after)'After'")
    structured_date_group.setDateLatestEra("urn:cspace:walkerart.org:vocabularies:name(dateera):item:name(ce)'CE'")
    structured_date_group.setDateLatestCertainty("urn:cspace:walkerart.org:vocabularies:name(datecertainty):item:name(before)'Before'")
    if cms_data['creation_start_year']:
        structured_date_group.setDateEarliestSingleYear(BigInteger(str(cms_data['creation_start_year'])))
    
    prod_date_group_list = ObjectProductionDateGroupList()
    dategroups = prod_date_group_list.getObjectProductionDateGroup()
    dategroups.add(structured_date_group)
    collectionobject.setObjectProductionDateGroupList(prod_date_group_list)

def addTitleToObject(collectionobject,cms_data,cur):
    # Title
    # this is overkill since it appears our database has only one title and it's always preferred.
    sql = "select t.title, t.title_type from title t where t.object_id = ?"
    
    cur.execute(sql, (cms_data['cms_id'],) )
    
    rows = cur.fetchall()
    if len(rows) > 0:
        titlegrouplist = TitleGroupList()
        titlegroups = titlegrouplist.getTitleGroup()
        
        description = cur.description
        for row in rows:
            data = namedColumns(row,description)
            titlegroup = TitleGroup()
            titlegroup.setTitle(cms_data['title'])
            titlegroups.add(titlegroup)
        collectionobject.setTitleGroupList(titlegrouplist)
    
def addCreatorsToObject(collectionobject,cms_data,cur,doc):
    # Creators are an authority, so we associate via refname.
    # check if it exists and use it, otherwise create person record.
    sql = "SELECT a.id, a.gender, a.nationality, a.ethnicity, a.date_text, \
    a.start_date, a.end_date, a.birth_place, a.death_place, a.active_date_place, \
    oa.role \
    FROM agent a, object_agent oa WHERE a.id = oa.agent_id and oa.object_id=?"
    
    cur.execute(sql,(cms_data['cms_id'],) );
    
    rows = cur.fetchall()
    if len(rows) > 0:
        persongrouplist = ObjectProductionPersonGroupList();
        persongroups = persongrouplist.getObjectProductionPersonGroup();
        
    # figure out if we need to "magically" extract names from a pile of shit
    usenames = cms_data['creator_text_forward'].split(',')
    do_multiname_hack = False
    orig_agent_size = len(rows)
    while len(usenames) > len(rows):
        do_multiname_hack = True # name extraction is a go, set a flag so we know
        rows.append(row[0]) #
            
    description = cur.description
    loopcount = 0
    nameindex = 0
    for row in rows:
        agent_data = namedColumns(row,description)
        loopcount += 1
        usename = None
        if loopcount>orig_agent_size and do_multiname_hack:
            usename = strip(usenames[nameIndex])
        
        # go build it
        refname=refnameForPerson(agent_data,cms_data,doc,cur,usename)
        
        persongroup = ObjectProductionPersonGroup()
        persongroup.setObjectProductionPersonRole(agent_data['role'])
        persongroup.setObjectProductionPerson(refname)
        persongroups.add(persongroup)
        collectionobject.setObjectProductionPersonGroupList(persongrouplist)
    
    