#!/usr/bin/env python

from subprocess import check_output
import urllib, json, sys

def get_name_nodes(clusterName):
    ha_ns_nodes = check_output(['hdfs', 'getconf', '-confKey',
                                'dfs.ha.namenodes.' + clusterName])
    nodes = ha_ns_nodes.strip().split(',')
    nodeHosts = []
    for n in nodes:
        nodeHosts.append(get_node_hostport(clusterName, n))
    return nodeHosts

def get_node_hostport(clusterName, nodename):
    hostPort = check_output(['hdfs','getconf','-confKey',
                             'dfs.namenode.rpc-address.{0}.{1}'.format(clusterName, nodename)])
    return hostPort.strip()

def is_node_active(nn):
    jmxPort = 50070
    host, port = nn.split(':')
    url = "http://{0}:{1}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus".format(host, jmxPort)
    nnstatus = urllib.urlopen(url)
    parsed = json.load(nnstatus)
    return parsed.get('beans', [{}])[0].get('State', '') == 'active'

def get_active_namenode(clusterName):
    node_active = 0
    for node in get_name_nodes(clusterName):
        if is_node_active(node):
            node_active = 1
            return node
    if node_active == 0:
        print("All nodes in standby")
        #Put namenode restart code over here if required
        #Send a mail stating that both namenodes were down, please look for the RCA
    else:
        print("We got an active namenode, njoy the time for now!!!")

clusterName = (sys.argv[1] if len(sys.argv) > 1 else None)
if not clusterName:
    raise Exception("Specify cluster name.")

print('Cluster: {0}'.format(clusterName))
print("Nodes: {0}".format(get_name_nodes(clusterName)))
print("Active Name Node: {0}".format(get_active_namenode(clusterName)))
