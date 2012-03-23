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
from java.util import UUID

from org.w3c.dom import Document
from org.w3c.dom import Element
from org.w3c.dom import Node

from javax.print import Doc
from javax.xml.bind import JAXBContext
from javax.xml.bind import JAXBException
from javax.xml.bind import Marshaller
from javax.xml.parsers import DocumentBuilder
from javax.xml.parsers import DocumentBuilderFactory
from javax.xml.transform import Transformer
from javax.xml.transform import TransformerFactory
from javax.xml.transform import OutputKeys
from javax.xml.transform.dom import DOMResult
from javax.xml.transform.dom import DOMSource
from javax.xml.transform.stream import StreamResult

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

#### Python imports
import urllib2

#################### GLOBALS
CSPACE_URL = "http://localhost:8180/"
password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
password_mgr.add_password(None, CSPACE_URL, 'admin@walkerart.org', 'Administrator')
handler = urllib2.HTTPBasicAuthHandler(password_mgr)
opener = urllib2.build_opener(handler)
urllib2.install_opener(opener)

def renameNamespaceRecursive(doc, node, namespace, prefix):
    #if (node.getNodeType() == Node.ELEMENT_NODE and node.getNodeName() != "schema"):
    if (node.getNodeType() == Node.ELEMENT_NODE):
        if node.getNodeName() != "schema":
            doc.renameNode(node, namespace, node.getNodeName())
            node.setPrefix(prefix)
        #node.removeAttribute("xmlns:"+namespace)

    childnodelist = node.getChildNodes()
    for i in range(childnodelist.getLength()):
        renameNamespaceRecursive(doc, childnodelist.item(i), namespace, prefix)

def writeDom(doc):
    try:
        # set up a transformer
        transfac = TransformerFactory.newInstance()
        trans = transfac.newTransformer()
        trans.setOutputProperty(OutputKeys.ENCODING, "utf-8")
        trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no")
        trans.setOutputProperty(OutputKeys.INDENT, "yes")
        trans.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2")
        
        sw1 = StringWriter()
        result = StreamResult(sw1)
        source = DOMSource(doc)
        trans.transform(source, result)
        xmlString = sw1.toString()
        print xmlString
    except Exception, e:
        print e

def namedColumns(row,description):
    newrow = {}
    for i in range(len(row)):
        newrow[description[i][0]] = row[i]
    return newrow

seq = 0
def next_seq():
    global seq
    seq += 1
    return str(seq)

def getCSID():
    return str(UUID.randomUUID())
