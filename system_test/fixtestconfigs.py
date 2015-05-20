#!/usr/bin/python
import sys
import json
import os
import fnmatch
import collections
import fileinput
import re

# brokers    = ["c6501.ambari.apache.org",
#               "c6502.ambari.apache.org",
#               "c6503.ambari.apache.org"]
#
# zookeepers = ["c6501.ambari.apache.org"]
#
# producers =  ["c6501.ambari.apache.org",
#               "c6502.ambari.apache.org"]
#
# consumers =  ["c6501.ambari.apache.org"]
#
# javaHome = "/usr/jdk64/jdk1.8.0_40"
# kafkaHome = "/usr/hdp/current/kafka-broker"
#
# zkPort = "2181"

# Settings loaded from cluster.properties file
brokers    = []
zookeepers = []
producers =  []
consumers =  []
javaHome = ""
kafkaHome = ""
zkPort = ""


def find_files(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


def fix_cluster_config_files(directory):
    for f in find_files("cluster_config.json", directory):
        print "Processing " + f
        inFile = open(f, "r")
        data = json.load(inFile, object_pairs_hook=collections.OrderedDict)
        inFile.close()

        brokerIndx = 0
        producerIndx = 0
        consumerIndx = 0
        zkIndx = 0

        for entity in data["cluster_config"]:
            if entity["role"] == "broker" and len(brokers)>0:
                entity["hostname"] = brokers[brokerIndx]
                brokerIndx = brokerIndx+1 if brokerIndx+1<len(brokers) else 0
            elif entity["role"] == "zookeeper" and len(zookeepers)>0:
                entity["hostname"] = zookeepers[zkIndx]
                zkIndx = zkIndx+1 if zkIndx+1<len(zookeepers) else 0
            elif entity["role"] == "producer_performance" and len(producers)>0:
                entity["hostname"] = producers[producerIndx]
                producerIndx = producerIndx+1 if producerIndx+1<len(producers) else 0
            elif entity["role"] == "console_consumer" and len(consumers)>0:
                entity["hostname"] = consumers[consumerIndx]
                consumerIndx = consumerIndx+1 if consumerIndx+1<len(consumers) else 0

            if "java_home" in entity and javaHome!="":
                entity["java_home"] = javaHome
            if "kafka_home" in entity and kafkaHome!="":
                entity["kafka_home"] = kafkaHome

        outFile = open(f, "w+")
        outFile.write( json.dumps(data, indent=4, separators=(',', ': ')) )
        outFile.close()


def fix_json_properties_files(directory):
    for f in find_files("testcase_*_properties.json", directory):
        print "Processing " + f
        inFile = open(f, "r")
        data = json.load(inFile, object_pairs_hook=collections.OrderedDict)
        inFile.close()
        changed = False
        for entity in data["entities"]:
            if "zookeeper" in entity:
                entity["zookeeper"] = zookeepers[0] + ":" + zkPort
                changed = True

        if changed:
            outFile = open(f, "w+")
            outFile.write( json.dumps(data, indent=4, separators=(',', ': ')) )
            outFile.close()

def fix_other_properties_file(directory):
    if len(zookeepers) == 0:
        return
    for f in find_files("*.properties", directory):
        print "Processing " + f
        print os.popen("perl -i -pe 's/zookeeper.connect=localhost:.*/zookeeper.connect=" + zookeepers[0] + ":" + zkPort + "/' " + f).read()
        print os.popen("perl -i -pe 's/zk.connect=localhost:.*/zk.connect=" + zookeepers[0] + ":" + zkPort + "/' " + f).read()

        if re.search("zookeeper_.*properties", f):
            print os.popen("perl -i -pe 's/server.1=localhost/server.1=" + zookeepers[0] + "/' " + f).read()


def loadClusterProperties(clusterProp):
    inFile = open(clusterProp, "r")
    data = json.load(inFile)
    inFile.close()
    global kafkaHome, javaHome, zkPort, zookeepers, producers, consumers, brokers

    if not "zookeepers" in data:
        print >> sys.stderr, "'zookeepers' list not specified"
    else:
        for zk in data["zookeepers"]:
            zookeepers.append(zk)

    if not "brokers" in data:
        print >> sys.stderr, "'brokers' list not specified"
    else:
        for b in data["brokers"]:
            brokers.append(b)

    if not "producers" in data:
        print >> sys.stderr, "'producers' list not specified"
    else:
        for p in data["producers"]:
            producers.append(p)

    if not "zkPort" in data:
        print >> sys.stderr, "'zkPort' not specified"
    else:
        zkPort = data["zkPort"]

    if not "consumers" in data:
        print >> sys.stderr, "'consumers' list not specified"
    else:
        for c in data["consumers"]:
            consumers.append(c)

    if not "javaHome" in data:
        print >> sys.stderr, "'javaHome' not specified"
    else:
        javaHome = data["javaHome"]

    if not "kafkaHome" in data:
        print >> sys.stderr, "'kafaHome' not specified"
    else:
        kafkaHome = data["kafkaHome"]


# Main

def usage():
    print "Usage :"
    print sys.argv[0] + " cluster.json testsuite_dir/"

if not len(sys.argv) == 3:
    usage()
    exit(1)

clusterProp = sys.argv[1]
directory = sys.argv[2]  # "./system_test/offset_management_testsuite"

loadClusterProperties(clusterProp)

print "-Kafka Home: " + kafkaHome
print "-Java Home: " + javaHome
print "-ZK port : " + zkPort
print "-Consumers : " + ",".join( consumers )
print "-Producers : " + ",".join( producers )
print "-Brokers : " + ",".join( brokers )
print "-Zookeepers : " + ",".join( zookeepers )


# 1 Update all cluster_config.json files
fix_cluster_config_files(directory)

# 2 Update testcase_*_properties.json files
fix_json_properties_files(directory)

# 3 fix role specific property files
fix_other_properties_file(directory)