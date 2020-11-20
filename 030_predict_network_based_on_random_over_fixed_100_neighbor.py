import json
import csv
import sys
import multiprocessing
import os
import shutil
import random

def get_day_time(timestamp) :
  day = timestamp / 1000 / 60 / 60 / 24
  return round(( day - int(day) ) * 1000 * 60 * 60 * 24)

def get_day_time_hour(hour) :
  return hour * 1000 * 60 * 60

def predict_network(userList, coreNumber) :
  '''This copied from  01_create_real_example_parallel'''
  numberOfPrediction = {}
  numberOfPositivePrediction = {}
  i=0
  STEP = 3
  printTable = {}
  for hour in range(0, 24, STEP):
    printTable[hour] = {}
    numberOfPrediction[hour] = 0
    numberOfPositivePrediction[hour] = 0
  for user in userList :
    i+=1
    neighborIndex = {}
    userLineI = 0
    for hour in range(0, 24, STEP):
      hourStamp = get_day_time_hour(hour)
      connectedNeighborList = []
      offline = True
      for lineI in range(userLineI, len(trace[user]) - 1) :
        userLineI = lineI
        if int(trace[user][lineI][1]) == 1:
          sessionStartUser = get_day_time(int(trace[user][lineI][0]))
          if int(trace[user][lineI + 1][1]) == -1:
            sessionEndUser = get_day_time_hour(24)
          else:
            sessionEndUser = get_day_time(int(trace[user][lineI + 1][0]))
          if hourStamp < sessionStartUser :
            # this session could be good for next examination time
            break
          elif sessionStartUser <= hourStamp < sessionEndUser :
            offline = False
            for neighbor in dataNeighborhood[user] :
              if neighbor not in neighborIndex :
                neighborIndex[neighbor] = 0
              for lineNeighborI in range(neighborIndex[neighbor],len(trace[neighbor])-1) :
                neighborIndex[neighbor] = lineNeighborI
                if int(trace[neighbor][lineNeighborI][1]) == 1:
                  sessionStartNeighbor = get_day_time(int(trace[neighbor][lineNeighborI][0]))
                  if int(trace[neighbor][lineNeighborI+1][1]) == -1 :
                    sessionEndNeighbor = 1000 * 60 * 60 * 24
                  else:
                    sessionEndNeighbor = get_day_time(int(trace[neighbor][lineNeighborI + 1][0]))
                  if hourStamp < sessionStartNeighbor:
                    # this session could be good for next examination time
                    break
                  elif sessionStartNeighbor <= hourStamp < sessionEndNeighbor :
                    numberOfPrediction[hour]+=1
                    randomFloat01 = random.random()
                    if randomFloat01 < probabilityTresholds[hour] :
                      connectedNeighborList.append(neighbor)
                      numberOfPositivePrediction[hour]+=1
                    break
                  elif sessionEndNeighbor <= hourStamp  :
                    # jump to next session because this is outdated
                    continue
                else :
                  continue
            # this session could be good for next examination time
            break
          elif sessionEndUser <= hourStamp  :
            # jump to next session because this is outdated
            continue
        else :
          # offline nodes are not printed so jump to next session
          continue
      if offline == False :
        printTable[hour][user] = connectedNeighborList
  for hour in range(0, 24, STEP):
    fileName = str(coreNumber) + "-" + str(hour) + ".csv"
    outFile = open('' + OUTFILE_PATH_FOR_GRAPH + fileName, "w", encoding="utf-8")
    outFile.write(str(printTable[hour]))
    outFile.close()
    fileName = str(coreNumber) + "-" + str(hour) + ".csv"
    outFile = open('' + OUTFILE_PATH_FOR_STAT + fileName, "w", encoding="utf-8")
    outFile.write(str(numberOfPrediction[hour])+" "+str(numberOfPositivePrediction[hour])+"\n")
    outFile.close()

with open('../res/trace_assignment100.json') as json_file:
  traceAssignment = json.load(json_file)
with open('../res/peersimNeighborhood100.json') as json_file:
  dataNeighborhood = json.load(json_file)
probabilityTresholds = {}
with open('trace_network100000x100_probabilities.csv') as csv_file:
  probCSV = csv.reader(csv_file, delimiter=' ', quotechar='|')
  for line in probCSV:
    probabilityTresholds[int(line[0])] = float(line[3])

traceFilePath = '../res/trace/'

OUTFILE_PATH_FOR_GRAPH = 'statR/'
if os.path.isdir(OUTFILE_PATH_FOR_GRAPH):
  shutil.rmtree(OUTFILE_PATH_FOR_GRAPH)
os.mkdir(OUTFILE_PATH_FOR_GRAPH)

OUTFILE_PATH_FOR_STAT = 'statR2/'
if os.path.isdir(OUTFILE_PATH_FOR_STAT):
  shutil.rmtree(OUTFILE_PATH_FOR_STAT)
os.mkdir(OUTFILE_PATH_FOR_STAT)

if len(sys.argv) > 1 and sys.argv[1] and sys.argv[1] is not None and str.isnumeric(sys.argv[1]):
  NUMBER_OF_CORES = int(sys.argv[1])
else :
  NUMBER_OF_CORES = 1

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
  processes.append(multiprocessing.Process(target=predict_network, args=(userList[core], core,)))
  processes[-1].start()  # start the thread we just created
  print(len(userList[core]))
for t in processes:
  t.join()