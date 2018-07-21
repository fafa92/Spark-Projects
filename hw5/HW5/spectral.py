import sys
import numpy as np
import networkx as nx
import networkx.utils 
def  second_smallest(ns):
    m1=ns[0]
    m2=ns[1]
    if m1>m2:
       f=m1
       m1=m2
       m2=f
    for x in ns:
        if x <= m1:
            if m1 != x:
                m1 ,m2=x ,m1
        elif m2>x:
            m2=x             
    return ns.index( m2)
np.random.seed(1)
G=[]
G.append(nx.Graph())
intputfile=sys.argv[1] 
no_clusters=int(sys.argv[2])
outputfile=sys.argv[3]
ifile=open(intputfile,'r')
ofile=open(outputfile,'w')
kl=[]
gl=[]
for line in ifile.readlines():
    w, h  = map(int, line.split())
    if w not in kl:kl.append(w)
    if h not in kl:kl.append(h)  
    gl.append((w,h))
kl.sort()    
G[0].add_nodes_from(kl)
G[0].add_edges_from(gl)
while no_clusters!=0 :
    no_clusters=no_clusters-1
    ilarge=0
    large=0
    for idx, val in enumerate(G):
        if(val.size()>large):
            ilarge=idx
            large=val.size()		
    L=nx.laplacian_matrix(G[ilarge], nodelist=None, weight='weight')
    val ,vec=np.linalg.eig(L.toarray())
    print(second_smallest(list(val)))
    ks=vec[:,second_smallest(list(val))]
    nodlst=list(G[ilarge].nodes())
    G.append(G[ilarge].copy())
    new=len(G)-1
    for i ,k in enumerate(ks):
        if(k<0):
            G[new].remove_node(nodlst[i])
        else:
            G[ilarge].remove_node(nodlst[i]) 
lk=sorted(list(G[0].nodes()))
for vg in G:
    ofile.write(",".join(str(x) for x in sorted(list(vg.nodes())))+"\n")
ofile.flush()
