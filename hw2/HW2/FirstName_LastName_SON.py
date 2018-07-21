
# coding: utf-8

# In[2]:


import findspark
findspark.init()
from pyspark import SparkContext
from collections import defaultdict
from itertools import combinations
import time
from operator import add
import sys


# In[3]:


sc = SparkContext(appName="sonalgorithm")


# In[4]:


def counter(x,query):
    count = 0
    for i in x:
        if(set(query).issubset(i)):
            count+=1
    return count


# In[5]:


def apriorialg(x,sup):
    x = list(x)
    d1={}
    for i in x:
        for j in i:
            if(j in d1):
                d1[j]+=1
            else:
                d1[j]=1
    supCount = sup*len(x)
    tempList=set()
    finalList=[]
    for i in d1:
        if(d1[i]>=supCount):
            finalList.append(i)
            tempList.add(i)
    curSize = 2
    candidates = tempList
    while(len(candidates)!=0):
        newPairs=[]
        k=0
        for p in candidates:
            for q in candidates:
                if(curSize>2):
                    i=set(p)
                    j=set(q)
                else:
                    i = set()
                    j = set()
                    i.add(p)
                    j.add(q)
                uni = i.union(j)
                sort1 = tuple(sorted(tuple(uni)))
                if(sort1 not in finalList):
                    if(len(uni)==curSize):
                        flag = 0
                        if(curSize!=2):
                            y = list(combinations(uni,curSize-1))
                            flag = 0
                            for z in y:
                                if(tuple(sorted(tuple(z))) not in finalList):
                                    flag = 1
                                    break
                        if(flag == 0):
                            if(counter(x,uni)>=supCount):
                                newPairs.append(uni)
                                finalList.append(sort1)
    
        curSize+=1
        candidates = newPairs
    return tuple(finalList)


# In[6]:


def checkcount(x,pairs):
    retList=[]
    x = list(x)
    tempSet = set()
    count =0
    for i in pairs:
        if(type(i[0])==str):
            temp = set()
            count+=1
            temp.add(i[0])
        else:
            temp = set(i[0])
        retList.append([i[0],counter(x,temp)])
    return retList


# In[ ]:


def main():
    ifile = sys.argv[1]
    support = float(sys.argv[2])
    ofile = sys.argv[3]
    rdd = sc.textFile(ifile).map(lambda x:x.split(','))
    size = len(rdd.collect())
    rdd2 = rdd.mapPartitions(lambda x:apriorialg(x,support)).map(lambda x:(x,1)).reduceByKey(lambda x,y:1)
    locaLFrequent1 =  rdd2.collect() 
    supportc=support*size
    rdd3 = rdd.mapPartitions(lambda x: checkcount(x,locaLFrequent1)).reduceByKey(lambda x,y:x+y).filter(lambda x:x[1]>=supportc)
    answer = rdd3.collect()
    file = open(ofile,'w')

    for val in answer:
        if(type(val[0])==str) or (type(val[0])==unicode):
            for i in range(len(val[0])):
                op = str(val[0][i])
                file.write("%s"%op)
        else:
            file.write("(")
            for i in range(len(val[0])):
                op = str(val[0][i])
                if(i!=(len(val[0])-1)):
                    file.write("%s,"%op)
                else:
                    file.write("%s"%op)
            file.write(")")
        file.write("\n")
        
    file.close()
    
    
    file = open(ofile,'r')
    f1=[]
    f2=[]
    for i in file:
        k=i.split('\n')[0]
        k=k.split(',')
        if len(k)==1:
            f1.append(int(k[0]))
        else:
            f2.append(k)
    
    f1.sort()
    gg=[]
    g=[]
    for i in f2:
        for j in i:
            if '(' in j:
                gg.append(int(j[1:]))
            elif ')' in j:
                gg.append(int(j[:-1]))
            
            else:
                gg.append(int(j))
        g.append(gg)
        gg=[]
    
    finall=[]
    for i in g:
        if len(i)==2:
            finall.append(i)
                
    finall.sort()
    file.close()
    file = open(ofile,'w')
    for i in f1:
        file.write(str(i))
        file.write('\n')
    
    for i in finall:
        file.write(str(tuple(i)))
        file.write('\n')
    
    file.close()
    
            
    
    

if __name__ == '__main__':
    main()

