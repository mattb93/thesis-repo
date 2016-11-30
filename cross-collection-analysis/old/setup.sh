#!/bin/bash

# This script pulls down the requested collection files, organizes them into a
# data folder, runs a python scrip on each of them to convert them from avro
# into plaintext, and then stores them back into hbase for processing

collectionNumbers=(z_41 z_45 z_121 z_122 z_128 z_145 z_157 z_443)
collectionSource="/collections/tweets/"
localDestination="data/"
hBaseDestination="processedCollections/"

# Pull the files down from hbase
for i in ${collectionNumbers[@]}; do
    echo "hadoop fs -get $collectionSource$i $localDestination$i"
    hadoop fs -get $collectionSource$i $localDestination$i
done

# Useavro-tools to convert fromavro to json
for i in ${collectionNumbers[@]}; do
    echo "avro-tools tojson ${localDestination}${i}/part-m-00000.avro > ${localDestination}${i}/${i}.json"
    avro-tools tojson "${localDestination}${i}/part-m-00000.avro" > "${localDestination}${i}/${i}.json"
done

# Run the python script that pulls the necessary data out of the json file and dumps it into a text file
for i in ${collectionNumbers[@]}; do
    echo "python jsontotxt.py $i"
    python jsontotxt.py $i
done

# Put the preprocessed files back in HBase for processing with spark
for i in ${collectionNumbers[@]}; do
    echo "hadoop fs -put ${localDestination}${i}/${i}-textOnly-raw ${hBaseDestination}${i}-textOnly-raw"
    hadoop fs -put "${localDestination}${i}/${i}-textOnly-raw" "${hBaseDestination}${i}-textOnly-raw"
done
