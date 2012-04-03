#!/usr/bin/env python
# encoding: utf-8

import sys 
reload (sys) 
sys.setdefaultencoding ('utf-8') # lets us pipe the output straight to a file and not puke on utf/ascii conversion

import xml.etree.cElementTree as etree
import fuzzy
import psycopg2
import psycopg2.extras
import psycopg2.extensions

ulan_base = '/home/nschroeder/Documents/walker/getty_vocabs/gettyconvert-1.0.0/'
ulan_rdfs = ['ULAN-painter.rdf','ULAN-architect.rdf','ULAN-sculptor.rdf','ULAN-printmaker.rdf','ULAN-draftsman.rdf','ULAN-photographer.rdf','ULAN-engraver.rdf','ULAN-illustrator.rdf','ULAN-architectural_firm.rdf','ULAN-designer.rdf','ULAN-Other.rdf']

def get_attribute_or_element(person,fullname,do_list=False):
    "returns a list if requested"
    # some properties can occur as either attributes OR elements for some reason...
    values = []
    value = person.get(fullname)
    if value:
        if do_list:
            values.append(value)
        else:
            # done!
            return value
    for element in person.findall(fullname):
        value = element.text
        if not value:
            value = element.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            if value:
                value = value.replace('http://e-culture.multimedian.nl/ns/getty/ulan#','')
        if value:
            if do_list:
                values.append(value)
            else:
                # done!
                return value
    return values


#establish a connection with db
try:
    conn = psycopg2.connect("dbname='%s' user='%s'" % ('ulan','nschroeder'))
    #print conn.encoding
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
except Exception as e:
    print "Database connection error"
    print e
    exit()

cur.execute("delete from ulan_names where 1=1");
cur.execute("delete from ulan_person where 1=1");
cur.execute("delete from ulan_role where 1=1");
cur.execute("delete from ulan_nationality where 1=1");

VPNS = '{http://e-culture.multimedian.nl/ns/getty/vp#}'
ULANNS = '{http://e-culture.multimedian.nl/ns/getty/ulan#}'
RDFNS = '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}'
RDFSNS = '{http://www.w3.org/2000/01/rdf-schema#}'


seen_ids = []
with open(ulan_base+"ULAN-Roles.rdf") as xml_file:
    root = etree.parse(xml_file)
    for item in root.iter(ULANNS+'Role'):
        ulan_id = get_attribute_or_element(item,RDFNS+'about').replace('http://e-culture.multimedian.nl/ns/getty/ulan#','')
        if ulan_id not in seen_ids:
            seen_ids.append(ulan_id)
            label = get_attribute_or_element(item,RDFSNS+'label')
            print ulan_id
            print label
            cur.execute("insert into ulan_role (ulan_id, ulan_role) values (%s, %s)",(ulan_id, label));
conn.commit()

seen_ids = []
with open(ulan_base+"ULAN-Nationalities.rdf") as xml_file:
    root = etree.parse(xml_file)
    for item in root.iter(ULANNS+'Nationality'):
        ulan_id = get_attribute_or_element(item,RDFNS+'about').replace('http://e-culture.multimedian.nl/ns/getty/ulan#','')
        if ulan_id not in seen_ids:
            seen_ids.append(ulan_id)
            label = get_attribute_or_element(item,RDFSNS+'label')
            print ulan_id
            print label
            cur.execute("insert into ulan_nationality (ulan_id, nationality) values (%s, %s)",(ulan_id, label));
conn.commit()

dmeta = fuzzy.DMetaphone()
seen_ids = []
for rdf_file in ulan_rdfs:
    with open(ulan_base+rdf_file) as xml_file:
        root = etree.parse(xml_file)
        for person in root.iter(ULANNS+'Person'):
            ulan_id = get_attribute_or_element(person,VPNS+'id')
            if ulan_id not in seen_ids:
                seen_ids.append(ulan_id)
                birthDate = get_attribute_or_element(person,ULANNS+'birthDate')
                deathDate = get_attribute_or_element(person,ULANNS+'deathDate')
                deathDate = deathDate if (deathDate and int(deathDate) < 2012) else None # wtf dates in the future??!?!
                birthPlace = get_attribute_or_element(person,ULANNS+'birthPlace')
                deathPlace = get_attribute_or_element(person,ULANNS+'deathPlace')
                biographyPreferred = get_attribute_or_element(person,ULANNS+'biographyPreferred')
                labelNonPreferred = get_attribute_or_element(person,VPNS+'labelNonPreferred',True)
                labelPreferred = get_attribute_or_element(person,VPNS+'labelPreferred')
                nationalityPreferred = get_attribute_or_element(person,ULANNS+'nationalityPreferred')
                rolePreferred = get_attribute_or_element(person,ULANNS+'rolePreferred')
                gender = get_attribute_or_element(person,ULANNS+'gender')
                gender = 'm' if 'Male' in gender else gender
                gender = 'f' if 'Female' in gender else gender
                cur.execute("insert into ulan_person (ulan_id, labelPreferred, gender, birthDate, deathDate, birthPlace, deathPlace, biographyPreferred, nationalityPreferred, rolePreferred) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(ulan_id, labelPreferred, gender, birthDate, deathDate, birthPlace, deathPlace, biographyPreferred, nationalityPreferred, rolePreferred));
    #            print ulan_id
    #            print birthDate
    #            print deathDate
    #            print birthPlace
    #            print deathPlace
    #            print biographyPreferred
    #            print labelPreferred
    #            print labelNonPreferred
    #            print gender
    #            print nationalityPreferred
    #            print rolePreferred
    #            print "=============="
                allnames = labelNonPreferred
                allnames.append(labelPreferred)
                for name in allnames:
                    dmname = dmeta(name)
                    print "{} {} {}".format(name,dmname[0], dmname[1])
                    cur.execute("insert into ulan_names (ulan_id, ulan_name, dmname1, dmname2) values (%s, %s, %s, %s)",(ulan_id,name,dmname[0],dmname[1]));
    conn.commit()
