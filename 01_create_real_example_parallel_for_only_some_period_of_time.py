import json
import csv
import sys
import multiprocessing


def get_day_time_hour(hour) :
  return hour * 1000 * 60 * 60

def get_day_time(timestamp) :
  day = timestamp / 1000 / 60 / 60 / 24
  return round(( day - int(day) ) * 1000 * 60 * 60 * 24)

def make_feature_vector(initiator,receiver,initiatorUserName,receiverUserName,initiatorLineI,receiverLineI) :
  if len(initiator) > 2 and len(receiver) > 2 and str(initiator[2]) == str(receiver[2]) :
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

def create_real_example_paralell(userList,hour,core) :
  fileName = str(hour) + "-" + str(core) + ".csv"
  file = open('' + OUTFILE_PATH + fileName, "w", encoding="utf-8")
  possibleFeatureVector = {}
  i=0
  for user in userList :
    i+=1
    neighborIndex = {}
    userLineI = 0
    hourStamp = get_day_time_hour(hour)
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
          for neighborInt in range(0, 100000):
            neighbor = str(neighborInt)
            if neighbor == user:
              continue
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
                  if featureVector in possibleFeatureVector:
                    possibleFeatureVector[featureVector] += 1
                  else:
                    possibleFeatureVector[featureVector] = 1
                    file.write('' + featureVector + '\n')
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
  file.close()

with open('../res/trace_assignment100.json') as json_file:
  traceAssignment = json.load(json_file)
with open('../res/peersimNeighborhood100.json') as json_file:
  dataNeighborhood = json.load(json_file)
traceFilePath = '../res/trace/'

OUTFILE_PATH = 'outHourAll3/'

if len(sys.argv) > 1 and sys.argv[1] and sys.argv[1] is not None and str.isnumeric(sys.argv[1]):
  NUMBER_OF_CORES = int(sys.argv[1])
else :
  NUMBER_OF_CORES = 48
  #NUMBER_OF_CORES = 8

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

processes = []
STEP = 3
for hour in range(0, 24, STEP):
  THREAD_FILE_NUMBER_BLOCK_SIZE = int(len(trace) / int(NUMBER_OF_CORES/8))
  userList = {}
  for core in range(int(NUMBER_OF_CORES/8)):
    userList[core] = []
  fi = 0
  core = 0
  # print(str(0),sumOfSize,str(THREAD_FILE_SIZE_BLOCK_SIZE),str(NUMBER_OF_CORES*THREAD_FILE_SIZE_BLOCK_SIZE))
  for user in trace:
    # searchObj = re.search(r'^[0-9]{6}_2014[0-9]{4}-[0-9]{4}\.csv$', fileName)
    userList[core].append(user)
    fi += 1
    if fi / THREAD_FILE_NUMBER_BLOCK_SIZE >= 1 and core != int(NUMBER_OF_CORES/8) - 1:
      core += 1
      # print(fi, sumOfSize, str(THREAD_FILE_SIZE_BLOCK_SIZE), str(NUMBER_OF_CORES-core))
      fi = 0
  for core in range(int(NUMBER_OF_CORES / 8)):
    processes.append(multiprocessing.Process(target=create_real_example_paralell, args=(userList[core],hour,core)))
    processes[-1].start()  # start the thread we just created
for t in processes:
  t.join()

