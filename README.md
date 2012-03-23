CollectionSpace Import
======================

This project uses the CS JAXB jars to generate valid XML for imports.
It requires jython so the jars can be used. (I'm using 2.5.2)
I created a virtual environment to isolate the packages, but as it turns out so far I haven't needed to add any. If I do they'll be in requirements.txt and should be installed via
    pip install -r requirements.txt

There is certainly a better way to do it, but for now I run this for set the classpath:
    export CLASSPATH=./lib/commons-lang-2.5.jar:./lib/org.collectionspace.services.collectionobject.jaxb-2.0-SNAPSHOT.jar:./lib/postgresql-9.1-901.jdbc4.jar:./lib/jaxb2-basics-runtime-0.6.2.jar:./lib/org.collectionspace.services.person.jaxb-2.0-SNAPSHOT.jar

If you are looking to adapt this for your own work, there are a few keys to getting the XML into the right namespaces (CS seems very particular about this).
The code in addPersonObjectToDom and renameNamespaceRecursive will show you the method I'm using (and addXXXToDom is ripe for abstracting to a superclass).

__Note__
This is not polished code (yet). It's not commented well (yet). It's not feature-complete (probably ever).
