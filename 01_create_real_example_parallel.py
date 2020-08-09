import json
import csv
import sys
import multiprocessing


def get_day_time(timestamp) :
  day = timestamp / 1000 / 60 / 60 / 24
  return round(( day - int(day) ) * 1000 * 60 * 60 * 24)

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

def make_feature_vector_for_users(userList,coreNumber) :
  fileName = str(coreNumber) + ".csv"
  file = open('' + OUTFILE_PATH + fileName, "a+", encoding="utf-8")
  possibleFeatureVector = {}
  for user in userList :
    neighborIndex = {}
    for lineI in range (0,len(trace[user])-1) :
      if int(trace[user][lineI][1]) == 1 :
        sessionStartUser = get_day_time(int(trace[user][lineI][0]))
        if int(trace[user][lineI+1][1]) == -1 :
          sessionEndUser = 1000 * 60 * 60 * 24
        else :
          sessionEndUser = get_day_time(int(trace[user][lineI + 1][0]))
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
              if (sessionStartUser <= sessionStartNeighbor < sessionEndUser <= sessionEndNeighbor ) or \
                 (sessionStartNeighbor <= sessionStartUser < sessionEndNeighbor <= sessionEndUser ) or \
                 (sessionStartUser <= sessionStartNeighbor and sessionEndUser >= sessionEndNeighbor) or \
                 (sessionStartUser >= sessionStartNeighbor and sessionEndUser <= sessionEndNeighbor)  :
                if str(trace[user][lineI][2]) == str(trace[neighbor][lineNeighborI][2]):
                  featureVector = make_feature_vector(trace[user][lineI],trace[neighbor][lineNeighborI],traceAssignment[user],traceAssignment[neighbor],str(lineI),str(lineNeighborI))
                  if featureVector in possibleFeatureVector :
                    possibleFeatureVector[featureVector] += 1
                  else :
                    possibleFeatureVector[featureVector] = 1
                    file.write('' + featureVector + '\n')
                  if sessionEndNeighbor < sessionEndUser :
                    continue
                  else :
                    break
                else :
                  print(traceAssignment[user],str(lineI),str(trace[user][lineI][2]),"sessionStartUser",sessionStartUser,str(trace[user][lineI][0]),"sessionEndUser",sessionEndUser,str(trace[user][lineI+1][0]),
                        traceAssignment[neighbor],str(lineNeighborI),str(trace[neighbor][lineNeighborI][2]),"sessionStartNeighbor",sessionStartNeighbor,str(trace[neighbor][lineNeighborI][0]),"sessionEndNeighbor",sessionEndNeighbor,str(trace[neighbor][lineNeighborI+1][0]),
                        "1",str((sessionStartUser <= sessionStartNeighbor < sessionEndUser <= sessionEndNeighbor )),
                        "2",str((sessionStartNeighbor <= sessionStartUser < sessionEndNeighbor <= sessionEndUser )),
                        "3",str((sessionStartUser <= sessionStartNeighbor and sessionEndUser >= sessionEndNeighbor)),
                        "4",str((sessionStartUser >= sessionStartNeighbor and sessionEndUser <= sessionEndNeighbor)),)
  print(len(possibleFeatureVector))
  file.close()


with open('../res/traceAssignment.json') as json_file:
  traceAssignment = json.load(json_file)
with open('../res/peersimNeighborhood.json') as json_file:
  dataNeighborhood = json.load(json_file)
traceFilePath = '../res/trace/'

OUTFILE_PATH = 'out50/'

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
  processes.append(multiprocessing.Process(target=make_feature_vector_for_users, args=(userList[core],core,)))
  processes[-1].start()  # start the thread we just created
  print(len(userList[core]))
for t in processes:
  t.join()

