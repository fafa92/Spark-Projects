import sys
inp=sys.argv[1]
a=int(sys.argv[2])
b=int(sys.argv[3])
n=int(sys.argv[4])
s=float(sys.argv[5])
output=sys.argv[6]


inp = open(inp,'r').readlines()
one={}
two={}
bitone={}
bittwo={}
li=[]
final={}
finalone=[]
finaltwo=[]
for i in inp:
    k=i.split('\n')[0]
    k=k.split(',')
    for j in k:
        
        one[int(j)]=one.get(int(j),0)+1
        if int(j) not in li:
            li.append(int(j))


        
li.sort()        

for i in range(len(li)):
    for j in range(i+1,len(li)+1):
        gg=((a*i)+(b*j))%n
        for p in inp:
            k=p.split('\n')[0]
            k=k.split(',')
            if str(i) in k and str(j) in k:
                two[gg]=two.get(gg,0)+1 
        
for i in two:
    if two[i]>=s:
        bittwo[i]=1
    else:
        bittwo[i]=0
        
        
for i in one:
    if one[i]>=s:
        bitone[i]=1
    else:
        bitone[i]=0

        
for i in range(1,len(li)):
    for j in range(i+1,len(li)+1): 
        if (bitone[i]==1) and (bitone[j]==1) and (bittwo[((a*i)+(b*j))%n]==1):
            for o in inp:
                k=o.split('\n')[0]
                k=k.split(',')
                if str(i) in k and str(j) in k:
                    final[(i,j)]=final.get((i,j),0)+1
#        else:
#            if bittwo[((a*i)+(b*j))%n] !=1:
#                
#                
##                print(i,j,'22222')
            
                    
for i in bitone:
    if bitone[i]==1:
        finalone.append(i)
        
finalone.sort()

for i in final:
    if final[i]>=s:
        finaltwo.append(i)
        
finaltwo.sort()

fh = open(output,"w")
for i in finalone:
    fh.write(str(i))
    fh.write('\n')
for i in finaltwo:
    fh.write(str(i))
    fh.write('\n')

fh.close()

#done={}
#dtwo={}
#for pp in finalone:
#    for i in inp:
#        k=i.split('\n')[0]
#        k=k.split(',')
#        if str(pp) in k:
#            done[pp]=done.get(pp,0)+1
#            
#            
#for pp in finaltwo:
#    for i in inp:
#        k=i.split('\n')[0]
#        k=k.split(',')
#        if str(pp[0]) in k and str(pp[1]) in k :
#            dtwo[pp]=dtwo.get(pp,0)+1
#            
# 
#fp=0           
#for i in finalone:
#    if done[i]<s:
#        fp+=1
#        
#for i in finaltwo:
#    if dtwo[i]<s:
#        fp+=1
#        
        
            
#print(done,'done')
#print(dtwo,'dtwo')
so=0
for i in bitone:
    if bitone[i]==1:
        so+=1
        
for i in bittwo:
    if bittwo[i]==1:
        so+=1
        
print(float(so)/float(len(bitone)+len(bittwo))*100.0)
#print((float(fp)/(len(finalone)+len(finaltwo)))*100.0)
            
        

        
        
              
                        

    