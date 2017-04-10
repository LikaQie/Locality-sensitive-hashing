from pyspark import SparkContext
import sys
import itertools
from collections import defaultdict


def readInput(data):
  returnMapping=[]
  mapping={}
  global maxCol
  maxCol=0
  for inp in data:
    inp=inp.rstrip()
    temp=inp.split(",")
    user=temp[0]
    count=1
    while (count < len(temp)):
      if temp[count] not in mapping:
        users=[]
        if (int(user[1:]) > maxCol):  ## Updating Max Column
          maxCol=int(user[1:])
        users.append(user[1:])
        mapping[temp[count]]=users
        count+=1
      else:
          users=mapping.get(temp[count])
          if (int(user[1:]) > maxCol):
            maxCol=int(user[1:])
          users.append(user[1:])
          mapping[temp[count]]=users  ##latest change
          count+=1
  tup=(mapping,maxCol)
  returnMapping.append(tup)
  return returnMapping

#Create Charaxteristic matrix
def getCharacteristicMatrix(rawData,charMatrix):
  #Updating with 1's whereever applicable
  for row in rawData.keys():
    cols=rawData.get(row)
    for col in cols:
      col=int(col)-1
      charMatrix[int(row)][col]=1
  return charMatrix

def createPermutedHashedValues():
  hashedMatrix = [[0 for col in range(0,20)] for row in range(0,100)]
  for row in range(0,100):
    for col in range(0,20):
      hashedMatrix[row][col]= ((3 * row) + (13 * (col))) % 100
  return hashedMatrix


def createSignature(charMatrix,hashedMat):
  signatureMatrix = [[9999999 for col in range(0,maxCol)] for row in range(0,20)]
  for row in range(len(charMatrix)):
    for col in range(len(charMatrix[row])):
      if charMatrix[row][col]==1:
        update_with=hashedMat[row]
        rowcount = 0
        for val in update_with:
          if val < signatureMatrix[rowcount][col]:
            signatureMatrix[rowcount][col]=val
          rowcount=rowcount+1
  return signatureMatrix

 ## update signature matrix with values from hashed matrix


def getCandidates(chunk):
  maxCol=maxC.value
  cMat=charMat.value
  hMat=hashedM.value
  band=[]
  for row in chunk:
    band.append(row)
  candidateKeyList=[]
  candidateDict=defaultdict(lambda:[])
  for col in range(0,maxCol):
    key=[row[col] for row in band]
    key=tuple(key)

    if key not in candidateDict:
      values=[]
      values.append(col)
      candidateDict[key]=values
    else:
      values=candidateDict.get(key)
      values.append(col)
      candidateDict[key]=values
  count=0
  returnCandidateList=[]
  for k in candidateDict:
      vals=candidateDict.get(k)
      for subseq in itertools.combinations(vals,2):
      #if len(vals)>1:
        # vals=tuple(vals)
         returnCandidateList.append(subseq)
  return returnCandidateList

#Find JD
def findJD(tups):
    cMat=charMat.value
    tupList=[]
    for tup in tups:
        #for i in range(0,len(tup)): 
        #    for j in range(i+1,len(tup)):
                jd=findJackard(cMat,tup[0],tup[1]) 
                mappedTup=((tup[0],tup[1]),jd)
                tupList.append(mappedTup)
    return list(set(tupList))

def findJackard(charMatrix,cand1,cand2):
  list1=[row[cand1] for row in charMatrix]
  list2=[row[cand2] for row in charMatrix]
  l1=''.join(str(x) for x in list1)
  l2=''.join(str(x) for x in list2)
  #print l1
  #print l2
  unioncnt=0
  intersectioncnt=0
  for i in range(0,len(l1)):
    if l1[i] == '1' and l2[i]=='1':
   #   print "Inside intersection"
      unioncnt+=1
      intersectioncnt+=1
    elif l1[i] == '1' or l2[i] =='1':
      unioncnt+=1

  #print float(intersectioncnt) / unioncnt
  return float(intersectioncnt) / unioncnt

def secondElement(element):
  return element[1]

def calculateLastPair(jdList):
    similarUsers=[]
    reverseCand=[]
    for tup in jdList:
        candidate=tup[0]
        reverseCandidate=((candidate[1],candidate[0]),tup[1])
        reverseCand.append(reverseCandidate)
    completeCandList=jdList+reverseCand
#    print completeCandList
    completeCandList=list(set(completeCandList))
#    print completeCandList
   
    ## Creating Key Value List
    userMapping={}
    for tup in completeCandList:
        elem=tup[0]
        if elem[0] not in userMapping:
           values=[]
           tupl=(elem[1],tup[1])
           values.append(tupl)
           userMapping[elem[0]]=values
        else:
            values=userMapping.get(elem[0])
            tupl=(elem[1],tup[1])
            values.append(tupl)
            userMapping[elem[0]]=values
 
    similarUsers.append(userMapping)
    return similarUsers 


s=sys.argv
out_file=open(s[2],"w")
sc = SparkContext(appName="inf553")
data=sc.textFile(s[1])

#data=open(s[1],"r")
rawData=data.mapPartitions(readInput).collect()
#rawData=readInput(data)

# Finding maxcol
maxCol=0
for returnedmaps in rawData:
  if returnedmaps[1] > maxCol:
    maxCol=returnedmaps[1]
maxC=sc.broadcast(maxCol)
## Initialise charMatrix
charMatrix = [[0 for col in range(0,maxCol)] for row in range(0,100)]

#Creating charactistic matrix
for returnedmaps in rawData:
  charMatrix=getCharacteristicMatrix(returnedmaps[0],charMatrix)
charMat=sc.broadcast(charMatrix)

#Creating hashed Matrix
hashedMat=createPermutedHashedValues()
hashedM=sc.broadcast(hashedMat)
# Creating Signature Matrix
signatureMatrix=createSignature(charMatrix,hashedMat)

##Passing Bands
band=sc.parallelize(signatureMatrix,5)

##bands to be called through mapPartition
#print "banding starts"
candidatePair=band.mapPartitions(getCandidates)
uniquePairs=candidatePair.map(lambda x: tuple(map(str, x))).groupByKey().flatMap(lambda x: [(x[0], v) for v in set(x[1])])
#candidatePair.distinct()
uPairs=uniquePairs.collect()
uniquelist=[]
for tup in uPairs:
    first=int(tup[0])
    sec=tup[1].strip()
    sec=int(sec)
    unique=(first,sec)
    uniquelist.append(unique)

uCP=sc.parallelize(uniquelist)
## Getting jackard
jdList=uCP.mapPartitions(findJD)

uniquecandlist=jdList.collect()

reverseCand=[]


####Comment this
for tup in uniquecandlist:
    candidate=tup[0]
    reverseCandidate=((candidate[1],candidate[0]),tup[1])
    reverseCand.append(reverseCandidate)
completeCandList=uniquecandlist+reverseCand
completeCandList=list(set(completeCandList))
#print "Complete list is"
#print completeCandList


## Create key,values list
userMapping={}
for tup in completeCandList:
    elem=tup[0]
    if elem[0] not in userMapping:
       values=[]
       new_tup=(elem[1],tup[1])
       values.append(new_tup)
       userMapping[elem[0]]=values
    else:
       values=userMapping.get(elem[0])
       new_tup=(elem[1],tup[1])
       values.append(new_tup)
       userMapping[elem[0]]=values
#print "User Mapping"
print userMapping.get(0)


## Finding Top 5
finalResult={}
for key in userMapping:
    tupList=userMapping.get(key)
    sortedList = [v[0] for v in sorted(tupList, key=lambda(k, v): (-v, k))]
#updated = sorted(set(tupleList), key=lambda tup: tup[1])
    final = sortedList[0:5]
    final.sort()
    finalResult[key]=final


sortedUsers=sorted(finalResult.keys())
#print sortedUsers
for user in sortedUsers:
    key="U"+str(user+1)+":"
    out_file.write(key)
    temp=finalResult.get(user)
    relatedUsers = [x+1 for x in temp]
    out=""
    for user in relatedUsers:
        out+="U"+str(user)+","
    out=out[:-1]
    out_file.write(out+"\n")

