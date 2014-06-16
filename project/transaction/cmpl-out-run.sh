#!/bin/sh

killall make

make runregistry & 
sleep 2
make runtm & 
sleep 2
make runrmflights & 
sleep 2
make runrmrooms &
sleep 2
make runrmcars &
sleep 2
make runrmcustomers &
sleep 2	
make runwc &
sleep 2

cd ../test.part2
cp -f Client.java ../transaction

cd ../lockmgr
make

cd ../transaction
make clean
make server
make client

cd ../test.part2
rm results/*
rm data/*

export CLASSPATH=".:gnujaxp.jar"


javac RunTests.java
java  -DrmiPort=4444 RunTests MASTER.xml

pidof rmiregistry
