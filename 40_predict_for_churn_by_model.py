import json
import csv
import sys
import multiprocessing
import os
import shutil
import random

NETWORK_SIZE = 100000
EXPECTED_NUMBER_OF_CONNECTED_NEIGHBOR = 20

predictions = {}
with open("trace_network100000xoutAll.csv") as f1, open("trace_network100000xoutAllPrediction10x.csv") as f2:
  for l1, l2 in zip(f1, f2):
    predictions[str(l1).split('\n')[0]] = str(l2).split('\n')[0]

with open('../res/trace_assignment100.json') as json_file:
  traceAssignment = json.load(json_file)
with open('../res/peersimNeighborhood100.json') as json_file:
  dataNeighborhood = json.load(json_file)

ERRFILE_PATH = 'error/'
if os.path.isdir(ERRFILE_PATH):
  shutil.rmtree(ERRFILE_PATH)
os.mkdir(ERRFILE_PATH)

OUTFILE_PATH = 'neighbor/'
if os.path.isdir(OUTFILE_PATH):
  shutil.rmtree(OUTFILE_PATH)
os.mkdir(OUTFILE_PATH)

if len(sys.argv) > 1 and sys.argv[1] and sys.argv[1] is not None and str.isnumeric(sys.argv[1]):
  NUMBER_OF_CORES = int(sys.argv[1])
else :
  NUMBER_OF_CORES = 50

traceFilePath = '../res/trace/'
trace = {}
for user in traceAssignment :
  with open('' + traceFilePath + traceAssignment[user]) as csvfile:
    traceCSV = csv.reader(csvfile, delimiter=';', quotechar='|')
    trace[user] = {}
    lineI = 0
    for line in traceCSV :
      trace[user][lineI] = []
      for record in line :
        trace[user][lineI].append(record)
      lineI += 1

def get_day_time(timestamp) :
  day = timestamp / 1000 / 60 / 60 / 24
  return round(( day - int(day) ) * 1000 * 60 * 60 * 24)

def get_day_time_hour(hour) :
  return hour * 1000 * 60 * 60

def make_feature_vector(initiator,receiver,initiatorUserName,receiverUserName,initiatorLineI,receiverLineI) :
  if str(initiator[2]) == str(receiver[2]) :
    returnList = ""+str(initiator[2])
    for recordI in range(3,len(initiator)) :
      try :
        returnList += ";" + str(initiator[recordI])
      except:
        print("IndexError:", str(recordI), str(initiatorUserName), str(initiatorLineI), str(initiator))
      try:
        returnList += ";" + str(receiver[recordI])
      except :
        print("IndexError:",str(recordI), str(receiverUserName), str(receiverLineI), str(receiver))
    return returnList
  else :
    #print("initiator",initiator,"receiver",receiver)
    return "-1"

def predict_churn(userList, coreNumber) :
  fileName = str(coreNumber) + ".csv"
  errorfile = open('' + ERRFILE_PATH + fileName, "w", encoding="utf-8")
  misssedFeatureVector = {}
  for user in userList :
    potentialNeighborIndex = {} #TODO nevezd át "potential"ra
    connectedNeighbors = {}
    for lineI in range (0,len(trace[user])-1) :
      if int(trace[user][lineI][1]) == 1:
        sessionStartUser = get_day_time(int(trace[user][lineI][0]))
        if int(trace[user][lineI + 1][1]) == -1:
          sessionEndUser = 1000 * 60 * 60 * 24
        else:
          sessionEndUser = get_day_time(int(trace[user][lineI + 1][0]))
        # 1) FROM sessionStartUser
        #TODO itt kell végignézni, hogy minden connectedNeighbors beli elemnek folytatódik-e a sessiönje?
        for neighbor in connectedNeighbors :
          for lineNeighborI in range(potentialNeighborIndex[neighbor],len(trace[neighbor])-1) :
            potentialNeighborIndex[neighbor] = lineNeighborI
            sessionStartNeighbor = get_day_time(int(trace[neighbor][lineNeighborI][0]))
            if int(trace[neighbor][lineNeighborI + 1][1]) == -1:
              sessionEndNeighbor = 1000 * 60 * 60 * 24
            else:
              sessionEndNeighbor = get_day_time(int(trace[neighbor][lineNeighborI + 1][0]))
            if sessionStartNeighbor <= sessionStartUser < sessionEndNeighbor and \
               int(trace[neighbor][lineNeighborI][1]) == 1:
              featureVector = make_feature_vector(trace[user][lineI], trace[neighbor][lineNeighborI],
                                                  traceAssignment[user], traceAssignment[neighbor], str(lineI),
                                                  str(lineNeighborI))
              try:
                if predictions[featureVector] == "1":
        # get new neighbor up to EXPECTED_NUMBER_OF_CONNECTED_NEIGHBOR
        listOfPotentialNodes = list(range(0, NETWORK_SIZE))
        while len(connectedNeighbors) < EXPECTED_NUMBER_OF_CONNECTED_NEIGHBOR:
          indexOfRandomNode = random.randrange(len(listOfPotentialNodes))
          neighbor = str(listOfPotentialNodes[indexOfRandomNode])
          del listOfPotentialNodes[indexOfRandomNode]
          if len(listOfPotentialNodes) <= 0:
            break
          if user == neighbor or neighbor in connectedNeighbors:
            continue
          if neighbor not in potentialNeighborIndex:
            potentialNeighborIndex[neighbor] = 0
          for lineNeighborI in range(potentialNeighborIndex[neighbor], len(trace[neighbor]) - 1):
            potentialNeighborIndex[neighbor] = lineNeighborI
            sessionStartNeighbor = get_day_time(int(trace[neighbor][lineNeighborI][0]))
            if int(trace[neighbor][lineNeighborI + 1][1]) == -1:
              sessionEndNeighbor = 1000 * 60 * 60 * 24
            else:
              sessionEndNeighbor = get_day_time(int(trace[neighbor][lineNeighborI + 1][0]))
            if sessionStartNeighbor <= sessionStartUser < sessionEndNeighbor and \
                int(trace[neighbor][lineNeighborI][1]) == 1:
              featureVector = make_feature_vector(trace[user][lineI], trace[neighbor][lineNeighborI],
                                                  traceAssignment[user], traceAssignment[neighbor], str(lineI),
                                                  str(lineNeighborI))
              try:
                if predictions[featureVector] == "1":
                  connectedNeighbors[neighbor] = sessionStartNeighbor
              except:
                if featureVector in misssedFeatureVector:
                  misssedFeatureVector[featureVector] += 1
                else:
                  misssedFeatureVector[featureVector] = 1
                  errorfile.write('' + featureVector + '\n')
              break
            if sessionStartUser < sessionStartNeighbor:
              break
        # 2) TO sessionEndUser
        #TODO itt kell végignézni, hogy minden connectedNeighbors beli elemnek folytatódik-e a sessiönje?


  errorfile.close()

#MAIN
userList = {}
for core in range(NUMBER_OF_CORES) :
  userList[core] = []

THREAD_FILE_NUMBER_BLOCK_SIZE = int(len(trace)/NUMBER_OF_CORES)
fi=0
core = 0
#print(str(0),sumOfSize,str(THREAD_FILE_SIZE_BLOCK_SIZE),str(NUMBER_OF_CORES*THREAD_FILE_SIZE_BLOCK_SIZE))
for user in trace:
  #searchObj = re.search(r'^[0-9]{6}_2014[0-9]{4}-[0-9]{4}\.csv$', fileName)
  userList[core].append(user)
  fi+=1
  if fi / THREAD_FILE_NUMBER_BLOCK_SIZE >= 1 and core != NUMBER_OF_CORES-1:
    core += 1
    #print(fi, sumOfSize, str(THREAD_FILE_SIZE_BLOCK_SIZE), str(NUMBER_OF_CORES-core))
    fi = 0
#print(fi, sumOfSize, str(THREAD_FILE_SIZE_BLOCK_SIZE), str(NUMBER_OF_CORES-core))
processes = []
for core in range(NUMBER_OF_CORES) :
  processes.append(multiprocessing.Process(target=predict_churn, args=(userList[core], core,)))
  processes[-1].start()  # start the thread we just created
  print(len(userList[core]))
for t in processes:
  t.join()
