#!/bin/sh

make clean
make server
make client

cd ../lockmgr
make

rmiregistry -J-classpath -J.. 3676 &

cd ../test.part2
export CLASSPATH=".:gnujaxp.jar"
javac RunTests.java
java -ea -DrmiPort=4444 RunTests MASTER.xml

pidof rmiregistry
