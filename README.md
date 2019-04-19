QlikGlue
=====
QlikGlue is essentially a flexible Kafka consumer that can read data published to Kafka 
topics -- whether by Attunity Replicate or another Kafka producer. QlikGlue parses the messages
that are received and passes them downstream for formatting and eventual "publishing" to
a number of different targets.


## Building this project
First, get this repository into your local environment:

        git clone https://github.com/attunity/qlikglue

Simply type ``make`` from the command line. Under the covers, ``make``
will be calling Maven, but everyone understands ``make``.


Assumptions: gmake, Maven, and OpenJDK 8 (or Oracle Java SE 8) are all installed and 
configured. Note that this project is built and tested with OpenJDK 8.
