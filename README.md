# CollectionSpace Import

This project uses the CS JAXB jars to generate valid XML for imports.

It requires jython so the jars can be used. (I'm using 2.5.2)

I created a virtual environment to isolate the packages, but as it turns out so far I haven't needed to add any. If I do they'll be in requirements.txt and should be installed via `pip install -r requirements.txt`

There is certainly a better way to do it, but for now I run this for set the classpath: `export CLASSPATH=./lib/commons-lang-2.5.jar:./lib/org.collectionspace.services.collectionobject.jaxb-2.0-SNAPSHOT.jar:./lib/postgresql-9.1-901.jdbc4.jar:./lib/jaxb2-basics-runtime-0.6.2.jar:./lib/org.collectionspace.services.person.jaxb-2.0-SNAPSHOT.jar`

If you are looking to adapt this for your own work, there are a few keys to getting the XML into the right namespaces (CS seems very particular about this).
The code in addObjectToDom and renameNamespaceRecursive will show you the method I'm using, as well as the technique for chaining schemas when custom extensions are involved.

__Note__

This is not polished code (yet). It's not commented well (yet). It's not feature-complete (probably ever).

## Setup
1. install jython
2. install virtualenv
3. jython /usr/local/lib/python2.6/dist-packages/virtualenv.py --no-site-packages ../virtenv
4. source ../virtenv/bin/activate (run in every new shell)
5. load CLASSPATH from above
6. cd into services project
7. mvn install -DskipTests
8. cp ./services/JaxRsServiceProvider/target/cspace-services.war
/usr/local/share/apache-tomcat-6.0.33/webapps/
9. start cspace

## Usage
1. `./import.py {limit}-{offset} [reset-cache] > cs_import.xml`
2. `curl http://localhost:8180/cspace-services/imports?type=xml -i -u admin@walkerart.org:Administrator \
-F "file=@cs_import.xml;type=application/xml"`

## Implementation notes
This code uses a database table to cache CSIDs so multiple runs should work and not generate duplicate CSIDs for the same object. This also allows us to put the authorities in the same XML import file, as long as they precede their first reference. (For instance, we cache the person refname and re-use for any other objects sharing the person.)

It's also using hostname to allow multiple import scripts to run and not conflict. Finally, the import routine can either manually clear the cache by passing in an argument, or it will detect when the default authority CSIDs have changed and blow the cache away (e.g. when resetting the CS db between botched tests).

The table is generated in Postgres like this:
```
DROP TABLE if exists cs;
CREATE TABLE cs (
    hostname varchar(255) NOT NULL,
    other_id varchar(255) NOT NULL,
    other_table varchar(255),
    refname text,
    csid text
);
```
