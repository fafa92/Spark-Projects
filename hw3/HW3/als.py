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
This is an example implementation of ALS for learning how to use Spark. Please refer to
pyspark.ml.recommendation.ALS for more conventional use.

This example requires numpy (http://www.numpy.org/)
"""
from __future__ import print_function
import numpy.matlib
from pyspark.sql import SQLContext
import sys

import numpy as np
from numpy.random import rand
from numpy import matrix
from pyspark.sql import SparkSession

LAMBDA = 0.01   # regularization
np.random.seed(42)


def rmse(R, ms, us):
    diff = R - ms * us.T
    return np.sqrt(np.sum(np.power(diff, 2)) / (M * U))


def update(i, mat, ratings):
    uu = mat.shape[0]
    ff = mat.shape[1]

    XtX = mat.T * mat
    Xty = mat.T * ratings[i, :].T

    for j in range(ff):
        XtX[j, j] += LAMBDA * uu

    return np.linalg.solve(XtX, Xty)


if __name__ == "__main__":

    """
    Usage: als [M] [U] [F] [iterations] [partitions]"
    """

    print("""WARN: This is a naive implementation of ALS and is given as an
      example. Please use pyspark.ml.recommendation.ALS for more
      conventional use.""", file=sys.stderr)

    spark = SparkSession\
        .builder\
        .appName("PythonALS")\
        .getOrCreate()

    sc = spark.sparkContext
    M = int(sys.argv[2]) if len(sys.argv) > 1 else 100
    U = int(sys.argv[3]) if len(sys.argv) > 2 else 500
    F = int(sys.argv[4]) if len(sys.argv) > 3 else 10
    ITERATIONS = int(sys.argv[5]) if len(sys.argv) > 4 else 5
    partitions = int(sys.argv[6]) if len(sys.argv) > 5 else 2
    outputf = sys.argv[7]
    inputf = sys.argv[1]

    print("Running ALS with M=%d, U=%d, F=%d, iters=%d, partitions=%d\n" %
          (M, U, F, ITERATIONS, partitions))
    
    
    ff=[]
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("C:/C/Spring2018/INF553/hw3/HW3/ratings_task2.csv")
    f=df.collect()
    
    for i in f:
        s=[]
        s.append(int(i[0]))
        s.append(int(i[1]))
        s.append(float(i[2]))
        
        ff.append(s)
    fff=np.array(ff)
    final=np.zeros((M,U))

    q=int(np.max(fff[:,0],axis=0))
    w=int(np.max(fff[:,1],axis=0))
    mm=np.zeros((q+1,w+1))
    for i in ff:
        mm[int(i[0])][int(i[1])]=i[2]
    mm=mm[1:,:]
    
    dic={}
    counter=0
    for i in range(len(mm[0])):
        for j in range(len(mm)):
            if mm[j][i]!=0:
                dic[counter]=i
                final[:,counter]=mm[:,i]
                counter+=1
                break
    
    R=matrix(final)
    
    
#    R =  np.matlib.zeros((M,U))
    c=0
#    with open(inputf,'r') as fread:
#        for i in fread:
#            if c==0:
#                c=1
#            else:
#                print(i.split(','))
#                Row = i.split(',')
#                
#                r,c,rate=Row[0],Row[1],Row[2]
#                print(r,c,rate)
#                R[int(r)-1,int(c)-1] = float(rate)
#    R = matrix(rand(M, F)) * matrix(rand(U, F).T)
    ms = matrix(rand(M, F))
    us = matrix(rand(U, F))
    Rb = sc.broadcast(R)
    msb = sc.broadcast(ms)
    usb = sc.broadcast(us)
    output = open(outputf,"w") 
    for i in range(ITERATIONS):
        ms = sc.parallelize(range(M), partitions) \
               .map(lambda x: update(x, usb.value, Rb.value)) \
               .collect()
        # collect() returns a list, so array ends up being
        # a 3-d array, we take the first 2 dims for the matrix
        ms = matrix(np.array(ms)[:, :, 0])
        msb = sc.broadcast(ms)
        us = sc.parallelize(range(U), partitions) \
               .map(lambda x: update(x, msb.value, Rb.value.T)) \
               .collect()
        us = matrix(np.array(us)[:, :, 0])
        usb = sc.broadcast(us)
        error = rmse(R, ms, us)
        print("Iteration %d:" % i)
        print("\nRMSE: %5.4f\n" % error)
        output.write(str("%5.4f\n"%error))
    output.close()
    spark.stop()
