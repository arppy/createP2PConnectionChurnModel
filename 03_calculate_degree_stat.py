import json
import csv
import sys
import multiprocessing
import os
import shutil

def get_day_time(timestamp) :
  day = timestamp / 1000 / 60 / 60 / 24
  return round(( day - int(day) ) * 1000 * 60 * 60 * 24)

def get_day_time_hour(hour) :
  return hour * 1000 * 60 * 60

i = 1
predictions = {}
with open("trace_network100000x100.csv") as f1, open("1010rr1101rr10network100000x100-prediction.out") as f2:
  for l1, l2 in zip(f1, f2):
    i+=1
    predictions[str(l1).split('\n')[0]] = str(l2).split('\n')[0]

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

def calculateDegreeStat(userList,coreNumber) :
  '''This copied from  01_create_real_example_parallel'''
  i=0
  STEP = 3
  printTable = {}
  for hour in range(0, 24, STEP):
    printTable[hour] = {}
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
                    featureVector = make_feature_vector(trace[user][lineI],trace[neighbor][lineNeighborI],traceAssignment[user],traceAssignment[neighbor],str(lineI),str(lineNeighborI))
                    try :
                      if predictions[featureVector] == "1" :
                        connectedNeighborList.append(neighbor)
                      '''print("OK", coreNumber, hourStamp, traceAssignment[user], lineI, sessionStartUser, sessionEndUser,
                            str(trace[user][lineI]), str(trace[user][lineI + 1]),
                            traceAssignment[neighbor], lineNeighborI, sessionStartNeighbor, sessionEndNeighbor,
                            str(trace[neighbor][lineNeighborI]), str(trace[neighbor][lineNeighborI + 1]),"-->",str(featureVector))'''
                    except :
                      print("ERROR", coreNumber, hourStamp, traceAssignment[user], lineI, sessionStartUser, sessionEndUser,
                            str(trace[user][lineI]), str(trace[user][lineI + 1]),
                            traceAssignment[neighbor], lineNeighborI, sessionStartNeighbor, sessionEndNeighbor,
                            str(trace[neighbor][lineNeighborI]), str(trace[neighbor][lineNeighborI + 1]),"-->",str(featureVector))
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
    outFile = open('' + OUTFILE_PATH + fileName, "w", encoding="utf-8")
    outFile.write(str(printTable[hour]))
    outFile.close()

with open('../res/trace_assignment100.json') as json_file:
  traceAssignment = json.load(json_file)
with open('../res/peersimNeighborhood100.json') as json_file:
  dataNeighborhood = json.load(json_file)
traceFilePath = '../res/trace/'

OUTFILE_PATH = 'stat/'
if os.path.isdir(OUTFILE_PATH):
  shutil.rmtree(OUTFILE_PATH)
os.mkdir(OUTFILE_PATH)

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
  processes.append(multiprocessing.Process(target=calculateDegreeStat, args=(userList[core],core,)))
  processes[-1].start()  # start the thread we just created
  print(len(userList[core]))
for t in processes:
  t.join()