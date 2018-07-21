import numpy as np
from numpy import genfromtxt
from timeit import default_timer as timer

#fi = pd.read_csv("C:/C/Spring2018/INF553/hw3/rates.csv") 
#print(fi)
import sys
inp=sys.argv[1]
nnn=int(sys.argv[2])
mmm=int(sys.argv[3])
fff=int(sys.argv[4])
kkk=int(sys.argv[5])

my_data = genfromtxt(inp,delimiter=',')

#nnn,mmm,fff,kkk=a,b,40,10
#nnn,mmm,fff,kkk=5,5,2,30

#fi=fi.iloc[:,:3]

u=np.ones((nnn,fff))
v=np.ones((fff,mmm))
def rmse(x,y):
#    q=np.array(x)
#    w=np.array(y)
    aa=((x-y) ** 2)
    s=0
    non=np.count_nonzero(x!=0)
    for i in range(len(x)):
        for j in range(len(x[0])):
            if x[i][j]!=0:
                s+=(x[i][j]-y[i][j])**2
    
    
    return (s/float(non))**(1/2.0)
    
    
    
my_data= my_data[1:,:3]

q=int(np.max(my_data[:,0],axis=0))
w=int(np.max(my_data[:,1],axis=0))
mm=np.zeros((q+1,w+1))
for i in my_data:
    mm[int(i[0])][int(i[1])]=i[2]
mm=mm[1:,:]


uu=u
vv=v
final=np.zeros((nnn,mmm))

dic={}
counter=0
for i in range(len(mm[0])):
    for j in range(len(mm)):
        if mm[j][i]!=0:
            dic[counter]=i
            final[:,counter]=mm[:,i]
            counter+=1
            break
        
            
cpp=0      
beforefinal=final

for t in range(kkk):
    
    for i in range(len(u[:,0])):
        for j in range(len(u[0,:])):
            sorat,makh=0,0
            
            
            for k in range(mmm):
                
                if final[i][k]!=0:
                    index=[]
                    for o in range(fff):
                        if o!=j:
                            
                            index.append(o)
                    mult=0
                    for h in index:
                        mult=mult+ u[i][h]*v[h][o]
                            
                    differances = final[i][k]-mult
                    sorat=sorat+v[j][k]*differances
                    makh=makh+v[j][k]*v[j][k]
            if makh==0:
                u[i][j]=0
            else:
                
                u[i][j]=float(sorat)/float(makh)

                
                
                
                
    for i in range(len(v[0,:])):
        for j in range(len(v[:,0])):
            sorat,makh=0,0
            for k in range(nnn):
                if final[k][i]!=0:
                    index=[]
                    for o in range(fff):
                        if o!=j:
                            index.append(o)
                    mult=0
                    for h in index:
                        mult=mult+ u[k][h]*v[h][i]
                    differances=final[k][i]-mult
                    sorat=sorat+u[k][j]*differances
                    makh=makh+u[k][j]*u[k][j]
            if makh==0:
                v[j][i]=0
            else:
                
                v[j][i]=float(sorat)/float(makh)
                    
            
            
#            A=u
#            B=v[:,i]
#            
#
#            ind=i*j+j
#            B[j]=x
#
#            C=A.multiply(B)
#
#            f=0
#            for k in range(len(u[:,0])):
#    #            print(i,j,k,A.shape,B.shape,ind,C.shape)
#    #            print(i,j,k,'kkkk')
#                
#                f += (final[k][i]-C[k])**2
#    #            print(final[k][i],C[k,i],'1111')
#            fprime = f.diff(x)
#            answer=solve(fprime, x)
#            v[ind]=answer[0]
#
#            cpp+=1
    
    print(rmse(final,np.dot(u,v)))
#    final=beforefinal
#    print(beforefinal)
#    print(u.multiply(v))
