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
import re
import sys
from personauthority import refnameForPerson

######################################################### XML population

fraction_1 = re.compile(r"^(\d+)[ |-](\d+)/(\d+)$")
fraction_2 = re.compile(r"^(\d+)/(\d+)$")
fraction_3 = re.compile(r"^[\d\.]+$")
def defractionize(fraction_ref):
    # turn a measurement string into an appropriate decimal.
    fraction_ref = fraction_ref.replace('"','').replace('--','-').strip();
    #$$fraction_ref =~ s/(\S) $/$1/; -- I think this is a strip?
    out = ''
    m = fraction_1.match(fraction_ref)
    if m:
        out = float(m.group(1)) + float(float(m.group(2))/float(m.group(3)))
    if not out:
        m = fraction_2.match(fraction_ref)
        if m:
            out = float(m.group(1))/float(m.group(2))
    if not out:
        m = fraction_3.match(fraction_ref)
        if m:
            out = int(fraction_ref)
    if not out and fraction_ref:
        sys.stderr.write("Discarding measurement '%s'\n" % (fraction_ref))
    return out

           
def addDimensionsToObject(collectionobject,cms_data,cur):
    sql = "select * FROM wac_dimensions where wac_object_id=?";
    cur.execute(sql,(cms_data['id'],));
    
    rows = cur.fetchall()
    description = cur.description
    if len(rows) > 0:
        measured_part_group_list = MeasuredPartGroupList()
        measured_part_groups = measured_part_group_list.getMeasuredPartGroup() 
        
    for row in rows:
        data = namedColumns(row,description)
        measured_part_group = MeasuredPartGroup()
        measured_part_group.setDimensionSummary(data['dimensions'])
        measured_part_group.setMeasuredPart(data['dimdescription'])
        dimension_sub_group_list = DimensionSubGroupList()
        dimension_sub_groups = dimension_sub_group_list.getDimensionSubGroup()
        
        for measure_type in ['width','height','depth','weight']:
            if data[measure_type]:
                dimension_sub_group = DimensionSubGroup()
                dimension_sub_group.setDimension(measure_type)
                if measure_type == 'weight':
                    dimension_sub_group.setMeasurementUnit("pounds")
                    dimension_sub_group.setMeasurementMethod("scale")
                else:
                    dimension_sub_group.setMeasurementUnit("inches")
                    dimension_sub_group.setMeasurementMethod("ruler")
                val = defractionize(str(data[measure_type]))
                if val:
                    dimension_sub_group.setValue(BigDecimal(val))
                    dimension_sub_groups.add(dimension_sub_group)
        measured_part_groups.add(measured_part_group)
        measured_part_group.setDimensionSubGroupList(dimension_sub_group_list)
        collectionobject.setMeasuredPartGroupList(measured_part_group_list)
    
date_1 = re.compile(r"^\D*(\d{4})\D*$")
date_2 = re.compile(r"^\D*(\d{4})\D+(\d{4}).*$")
date_3 = re.compile(r"^\D*(\d{4}).(\d{2}).*$")
date_3a = re.compile(r"(\d{2})")
def addProductionDatesToObject(collectionobject,cms_data,cur):
    # Production dates
    start_date = ''
    end_date   = ''
    date = cms_data['date_wac'].replace('unknown','')
    m = date_1.match(date)
    if m:
        start_date = m.group(1)
        end_date = m.group(1)
        
    if not start_date:
        m = date_2.match(date)
        if m:
            start_date = m.group(1)
            end_date = m.group(2)
    
    if not start_date:
        m = date_3.match(date)
        if m:
            start_date = m.group(1)
            end_date = m.group(2)
            m2 = date_3a.match(start_date)
            end_date = m2.group(1) + end_date;
    
    structured_date_group = StructuredDateGroup()
    structured_date_group.setScalarValuesComputed(False)
    if end_date:
        structured_date_group.setDateLatestYear(BigInteger(str(end_date)))
    structured_date_group.setDateEarliestSingleEra("urn:cspace:walkerart.org:vocabularies:name(dateera):item:name(ce)'CE'")
    structured_date_group.setDateDisplayDate(cms_data['date_wac'])
    structured_date_group.setDateEarliestSingleCertainty("urn:cspace:walkerart.org:vocabularies:name(datecertainty):item:name(after)'After'")
    structured_date_group.setDateLatestEra("urn:cspace:walkerart.org:vocabularies:name(dateera):item:name(ce)'CE'")
    structured_date_group.setDateLatestCertainty("urn:cspace:walkerart.org:vocabularies:name(datecertainty):item:name(before)'Before'")
    if start_date:
        structured_date_group.setDateEarliestSingleYear(BigInteger(str(start_date)))
    
    prod_date_group_list = ObjectProductionDateGroupList()
    dategroups = prod_date_group_list.getObjectProductionDateGroup()
    dategroups.add(structured_date_group)
    collectionobject.setObjectProductionDateGroupList(prod_date_group_list)

def addTitleToObject(collectionobject,cms_data,cur):
    # Title
    titlegrouplist = TitleGroupList()
    titlegroups = titlegrouplist.getTitleGroup()
    titlegroup = TitleGroup()
    titlegroup.setTitle(cms_data['title'])
    titlegroups.add(titlegroup)
    collectionobject.setTitleGroupList(titlegrouplist)
    
def addCreatorsToObject(collectionobject,cms_data,cur,doc):
    # Creators are an authority, so we associate via refname.
    # check if it exists and use it, otherwise create person record.
    
    sql = "select * FROM wac_artist where wac_object_id=?";
    cur.execute(sql,(cms_data['id'],));
    
    rows = cur.fetchall()
    description = cur.description
    if len(rows) > 0:
        persongrouplist = ObjectProductionPersonGroupList();
        persongroups = persongrouplist.getObjectProductionPersonGroup();
        
    for row in rows:
        data = namedColumns(row,description)
            
        # go build it
        refname=refnameForPerson(data,cms_data,doc,cur)
        
        persongroup = ObjectProductionPersonGroup()
        persongroup.setObjectProductionPersonRole(data['role'])
        persongroup.setObjectProductionPerson(refname)
        persongroups.add(persongroup)
        collectionobject.setObjectProductionPersonGroupList(persongrouplist)
    
    