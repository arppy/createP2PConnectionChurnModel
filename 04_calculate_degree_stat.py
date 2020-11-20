import json
import os
import math
import networkx as nx
import multiprocessing

def calculate_degree_stat(hour):
  outDegreeSum = 0
  outDegreeCount = 0
  max = 0
  min = 100
  G = nx.Graph()
  for user in hourOutSnapShot[hour] :
    outDegreeCount+=1
    outDegreeSum+=len(hourOutSnapShot[hour][user])
    if max < len(hourOutSnapShot[hour][user]) :
      max = len(hourOutSnapShot[hour][user])
    if min > len(hourOutSnapShot[hour][user]) :
      min = len(hourOutSnapShot[hour][user])
    if len(hourOutSnapShot[hour][user]) > 0 :
      G.add_node(user)
      for neigh in hourOutSnapShot[hour][user] :
        G.add_node(neigh)
        G.add_edge(user,neigh)
        if user not in  hourTotalSnapShot[hour] :
          hourTotalSnapShot[hour][user] = {}
        hourTotalSnapShot[hour][user][neigh] = 1
        if neigh not in hourTotalSnapShot[hour] :
          hourTotalSnapShot[hour][neigh] = {}
        hourTotalSnapShot[hour][neigh][user] = 1
    else :
      if user not in hourTotalSnapShot[hour]:
        hourTotalSnapShot[hour][user] = {}
  print(hour,"DIAMETER:")
  if nx.is_connected(G) :
    print(nx.diameter(G))
    print(nx.degree_assortativity_coefficient(G))
  else :
    print(len(G.edges()))
    print(len(G.nodes()))
    lenComp = [len(c) for c in sorted(nx.connected_components(G), key=len, reverse=True)]
    print(lenComp)
    for component in nx.connected_components(G) :
      print(component)
      #S = G.subgraph(component).copy() nx.diameter(S),
    #  print("len:",len(component))
  print("edges:",len(G.edges()),"nodes_in_connected_component:",len(G.nodes()),"nodes:",outDegreeCount)
  avgWithOffline = float(outDegreeSum/100000)
  avg = float(outDegreeSum/outDegreeCount)
  stdev = 0
  for user in hourOutSnapShot[hour] :
    stdev += (len(hourOutSnapShot[hour][user]) - avg) * (len(hourOutSnapShot[hour][user]) - avg)
  stdev = math.sqrt((1 / ( outDegreeCount - 1 ) ) * stdev)
  #print(" ")
  print(hour,float(outDegreeSum/outDegreeCount),float(outDegreeSum/100000), min, max, stdev)
  distribution = {}
  outDegreeSum = 0
  outDegreeCount = 0
  max = 0
  min = 100
  for user in hourTotalSnapShot[hour]:
    outDegreeCount += 1
    numberOfConnectedNeighbor = len(hourTotalSnapShot[hour][user])
    outDegreeSum += numberOfConnectedNeighbor
    if max < numberOfConnectedNeighbor :
      max = numberOfConnectedNeighbor
    if min > numberOfConnectedNeighbor :
      min = numberOfConnectedNeighbor
    #if numberOfConnectedNeighbor == 1 :
    #  print(user, hourTotalSnapShot[hour][user])
    if numberOfConnectedNeighbor not in distribution :
      distribution[numberOfConnectedNeighbor] = 0
    distribution[numberOfConnectedNeighbor] += 1
  stdev = 0
  for user in hourTotalSnapShot[hour] :
    stdev += (len(hourTotalSnapShot[hour][user]) - avg) * (len(hourTotalSnapShot[hour][user]) - avg)
  stdev = math.sqrt((1 / ( outDegreeCount - 1 ) ) * stdev)
  print(hour, float(outDegreeSum / outDegreeCount), float(outDegreeSum / 100000), min, max, stdev)
  fileName = "degreeDistribution-" + str(hour) + ".csv"
  outFile = open('' + OUTFILE_PATH + fileName, "w", encoding="utf-8")
  for numberOfConnectedNeighbor in range(0,max+1) :
    if numberOfConnectedNeighbor not in distribution :
      distribution[numberOfConnectedNeighbor] = 0
    outFile.write(str(numberOfConnectedNeighbor)+" "+str(distribution[numberOfConnectedNeighbor])+" "+str(distribution[numberOfConnectedNeighbor]/outDegreeCount)+"\n")
  outFile.close()

#INFILE_PATH = 'stat/'
#OUTFILE_PATH = 'distribution/'
#INFILE_PATH = 'statX/'
#OUTFILE_PATH = 'distributionX/'
INFILE_PATH = 'statR/'
OUTFILE_PATH = 'distributionR/'
#INFILE_PATH = 'statXRb/'
#OUTFILE_PATH = 'distributionXRb/'

hourOutSnapShot = {}
if os.path.isdir(INFILE_PATH):
  files = os.listdir(INFILE_PATH)
  for fileName in files :
    with open(INFILE_PATH+fileName) as json_file:
      for line in json_file :
        str_line = str(line).replace("\'", "\"")
        content = json.loads(str_line)
        baseName = str(fileName).split('.')[0]
        fileNameArray = str(baseName).split('-')
        if fileNameArray[1] not in hourOutSnapShot :
          hourOutSnapShot[fileNameArray[1]] = dict(content)
        else :
          hourOutSnapShot[fileNameArray[1]].update(content)
        #print(fileName, len(content))

hourTotalSnapShot = {}
for hour in hourOutSnapShot :
  hourTotalSnapShot[hour] = {}

processes = []
STEP = 3
for hour in hourOutSnapShot :
  processes.append(multiprocessing.Process(target=calculate_degree_stat, args=(hour,)))
  processes[-1].start()  # start the thread we just created
for t in processes:
  t.join()
