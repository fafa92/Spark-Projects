from pyspark import SparkConf, SparkContext
sc =SparkContext.getOrCreate()
from operator import add
import ast
import re
from operator import itemgetter

menu=sc.textFile("menu.csv").map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[3],line[18])).collect()
menu=menu[1:]

    
def count(x):
    word=x[0]
    word=re.sub(r'[^\x00-\x7f]',r'',word ) 
    word = re.sub('-','',word)
    word = re.sub("'",'',word)
    word = re.sub(r'[^a-zA-Z0-9\s]', ' ', word)
    if word.decode('utf-8') !='':
        page=int(x[1])
        word=word.strip()

        word=' '.join(word.split())

        word=word.lower()




        return (str(word),1)


def avg(x):
    word=x[0]
#    pp=[]
#    for p in word:
#        if ord(p)<128:
#            pp.append(p)
#    word=''.join(pp)
#
##    word=re.sub(r'[^\x00-\x7f]',r'',word ) 
    word = re.sub('-','',word)
    word = re.sub("'",'',word)
#    word = re.sub(r'[!@#$%^&?\[\]{}=_:\/\\<>`~*"()/+".,/;]',' ',word)
##    word = re.sub(r'[^a-zA-Z0-9\s]', '', word)
    word=re.sub(r'[^\x00-\x7f]',r'',word ) 
    word = re.sub(r'[^a-zA-Z0-9\s]', ' ', word)
    if word.decode('utf-8') !='':
        page=int(x[1])
        word=word.strip()

        word=' '.join(word.split())

        word=word.lower()




        return (str(word),page)
    

ds1=sc.parallelize(menu).map(count).filter(lambda x: x is not None).collect()
ds2=sc.parallelize(menu).map(avg).filter(lambda x: x is not None).collect()
d={}    
for i in range(len(ds1)):


    a=ds1[i][0].split()
    cc=0

    if d:

        for j in d:
            if set(ast.literal_eval(j))==set(a):
                ds1[i]=(' '.join(ast.literal_eval(j)),ds1[i][1])
                cc=1
        if cc==0:
            d[str(a)]=d.get(str(a),0)+ds1[i][1]
        else:
            cc=0


    else:
        d[str(a)]=d.get(str(a),0)+ds1[i][1]


d={}
for i in range(len(ds2)):


    a=ds2[i][0].split()
    cc=0

    if d:

        for j in d:
            if set(ast.literal_eval(j))==set(a):
                ds2[i]=(' '.join(ast.literal_eval(j)),ds2[i][1])
                cc=1
        if cc==0:
            d[str(a)]=d.get(str(a),0)+ds2[i][1]
        else:
            cc=0


    else:
        d[str(a)]=d.get(str(a),0)+ds2[i][1]
    
ds1=sc.parallelize(ds1).reduceByKey(add).collect()
ds2 = sc.parallelize(ds2)


ds2 = ds2 \
    .mapValues(lambda v: (v, 1)) \
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
    .mapValues(lambda v: float(v[0])/float(v[1])) \
    .collect()
    
    
ds1 = sc.parallelize(ds1)
ds2 = sc.parallelize(ds2)
ds1=ds1.join(ds2).collect()

a=[]
aa=[]
F = open("output.txt","w") 

for i in ds1:
    a.append(i[0])
    a.append(i[1][0])
    a.append(i[1][1])
    aa.append(a)
    a=[]

aa=sorted(aa, key=itemgetter(0))
for i in range(1,len(aa)):    
    F.write('\t'.join(map(str,aa[i])))
    F.write("\n")
    
F.close()