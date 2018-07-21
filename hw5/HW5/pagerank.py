#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is an example implementation of PageRank. For more conventional use,
Please refer to PageRank implementation provided by graphx

Example Usage:
bin/spark-submit examples/src/main/python/pagerank.py data/mllib/pagerank_data.txt 10
"""
from __future__ import print_function

import re
import sys
from operator import add
from operator import itemgetter
import networkx as nx
import numpy as np
#from pyspark import SparkContext
from pyspark import SparkConf, SparkContext
sc =SparkContext.getOrCreate()
from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def parseNeighbor(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[1], parts[0]


if __name__ == "__main__":
#    if len(sys.argv) != 3:
#        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
#        exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)
    
    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()
#    sc = spark.sparkContext

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    linkss = lines.map(lambda urls: parseNeighbor(urls)).distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    lll=lines.collect()
    l=[]
    for i in range(len(lll)):
        l.append(lll[i].split())
    G = nx.Graph()
    for i in range(len(l)):
        G.add_edge(int(l[i][0]), int(l[i][1]))
#        
#    listadj=G.adjacency_list()
#    lleenn=len(links.collect())
#    h0=[(unicode(i),1.0) for i in range(1,lleenn+1)]
#    h0=[1.0 for i in range(1,lleenn+1)]
#    L=links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
#    LT=links.map(lambda url_neighbors: (url_neighbors[1], 1.0))
#    lleenn=len([(n, nbrdict) for n, nbrdict in G.adjacency()])
    re=G.nodes()
#    def mult(x,y):
#        newh=[]
#        for i in range(len(x)):
#            s=0
#            for j in range(len(y)):
#                if unicode(i+1) in x[i]:
#                    print(i+1,x[i])
#                    s+=y[j][1]*x[i][1]
#            newh.append((unicode(i+1),s))
#        print(newh)
#        
#        mm=max(newh,key=itemgetter(1))[1]
#        for i in range(len(newh)):
#            ph=newh[i][1]*1.0/mm
#            newh[i]=(unicode(i+1),ph)
#        return newh
#            
        
            
    outpuf=sys.argv[3]
#    output = open(outputf,"w") 
    la=[]
    lh=[]
    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
#        if iteration !=0:
#            links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
#        
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        
        
        a0=contribs.reduceByKey(add).collect()
        
        mm=max(a0,key=itemgetter(1))[1]
        for i in range(len(a0)):
            ph=a0[i][1]*1.0/mm
            a0[i]=(a0[i][0],ph)
            
        a0p=a0    
        a0=sc.parallelize(a0)
        
        #
        
        contribs = linkss.join(a0).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
        
        ranks=contribs.reduceByKey(add).collect()
        mm=max(ranks,key=itemgetter(1))[1]
        for i in range(len(ranks)):
            ph=ranks[i][1]*1.0/mm
            ranks[i]=(ranks[i][0],ph)
          
        ranksp=ranks
        ranks=sc.parallelize(ranks)
        
        printlista=[]
        printlisth=[]
        
        for i in range(len(re)):
            c=1
            for j in range(len(a0p)):
                if unicode(re[i]) == a0p[j][0]:
                    printlista.append((i+1,"%.5f" %(a0p[j][1])))
                    c=0
            if c == 1:
                printlista.append((i+1,0.00000))
        la.append(printlista)
                                      
        for i in range(len(re)):
            c=1
            for j in range(len(ranksp)):
                if unicode(re[i]) == ranksp[j][0]:
                    printlisth.append((i+1,"%.5f" %(ranksp[j][1])))
                    c=0
            if c == 1:
                printlisth.append((i+1,"%.5f" %(0.00000)))
        lh.append(printlisth)
                                      
                                      
     
    output = open(outputf+'/authority.txt',"w") 
                                      
              
    for i in la:
        for j in i:
            output.write(str(j[0])+','+str(j[1])+'\n')
    output.close()
                                      
    output = open(outputf+'/hub.txt',"w") 
                                      
              
    for i in lh:
        for j in i:
            output.write(str(j[0])+','+str(j[1])+'\n')
    output.close()
        
#
#        # Re-calculates URL ranks based on neighbor contributions.
    
    
    
                                    
    
#        lltt=LT.collect()
#        a0=mult(lltt,h0)
#        ll=L.collect()
#        h0=mult(ll,a0)
#        print(a0,h0)
##        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
#
#    # Collects all URL ranks and dump them to console.
#    for (link, rank) in ranks.collect():
#        print("%s has rank: %s." % (link, rank))

    spark.stop()
