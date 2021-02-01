import json
import csv
import sys
import multiprocessing
import os
import shutil
import random
import operator

NETWORK_SIZE = 100000
EXPECTED_NUMBER_OF_CONNECTED_NEIGHBOR = 20
NEW_CONNECTION_ATTEMPT_STRING = "new_connection_attempt"
NEIGHBOR_STATE_CHANGED = "neighbor_state_changed"
USER_STATE_CHANGED = "user_state_changed"
BOTH_STATE_CHANGED = "both_state_changed"

END_OF_A_DAY = 1000 * 60 * 60 * 24
END_OF_A_HOUR = 1000 * 60 * 60

predictions = {}
#with open("trace_network100000x100.csv") as f1, open("trace_network100000x100_probabilities.csv") as f2:
with open("trace_network100000xoutAll.csv") as f1, open("trace_network100000xoutAllPrediction10x.csv") as f2:
  for l1, l2 in zip(f1, f2):
    predictions[str(l1).split('\n')[0]] = str(l2).split('\n')[0]

with open('../res/trace_assignment100.json') as json_file:
  traceAssignment = json.load(json_file)

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

class Session :
  def __init__(self, traceRecord, next_session_start, nextSessionType):
    self.traceRecord = traceRecord
    self.sessionStart = self.__get_day_time(int(traceRecord[0]))
    if int(nextSessionType) == -1:
      self.sessionEnd = END_OF_A_DAY
    else:
      self.sessionEnd = self.__get_day_time(int(next_session_start))

  def __get_day_time(self,timestamp):
    day = timestamp / 1000 / 60 / 60 / 24
    return round((day - int(day)) * 1000 * 60 * 60 * 24)

'''
  def time_to_online_intersection_at_time(self, examinedTime):
    if int(self.traceRecord[1]) == 1  and self.start <= examinedTime < self.end :
      return self.end
    elif int(self.traceRecord[1]) == 1  and examinedTime < self.start :
      return 0
    else:
      return -1

  def time_to_online_intersection_with(self, neighborSession):
    if int(neighborSession.traceRecord[1]) == 1 and int(self.traceRecord[1]) == 1 :
      if self.sessionEnd <= neighborSession.sessionEnd :
        if (self.sessionStart <= neighborSession.sessionStart < self.sessionEnd) or (self.sessionStart >= neighborSession.sessionStart) :
          return self.sessionEnd
        else :
          -1
      elif self.sessionEnd >= neighborSession.sessionEnd :
        if (neighborSession.sessionStart <= self.sessionStart < neighborSession.sessionEnd) or (self.sessionStart <= neighborSession.sessionStart) :
          return neighborSession.sessionEnd
        else:
          -1
    else :
      return -1
    #(self.sessionStart <= sessionStartNeighbor < self.sessionEnd <= sessionEndNeighbor) or \
    #(sessionStartNeighbor <= self.sessionStart < sessionEndNeighbor <= self.sessionEnd) or \
    #(self.sessionStart <= sessionStartNeighbor and self.sessionEnd >= sessionEndNeighbor) or \
    #(self.sessionStart >= sessionStartNeighbor and self.sessionEnd <= sessionEndNeighbor) :
'''


traceFilePath = '../res/trace/'
trace = {}
for user in traceAssignment :
  with open('' + traceFilePath + traceAssignment[user]) as csvfile:
    traceCSV = csv.reader(csvfile, delimiter=';', quotechar='|')
    trace[user] = {}
    lineI = 0
    for line in traceCSV :
      traceRecord = []
      recordI = 0
      for record in line :
        traceRecord.append(record)
        if recordI == 0 :
          next_session_start = int(record)
        if recordI == 1 :
          nextSessionType = int(record)
        recordI += 1
      if lineI > 0 :
        trace[user][lineI-1] = Session(prevTraceRecord, next_session_start, nextSessionType)
      lineI += 1
      prevTraceRecord = traceRecord
def get_day_time_hour(hour) :
  return hour * 1000 * 60 * 60

def make_feature_vector(initiator,receiver,initiatorUserName,receiverUserName,initiatorLineI,receiverLineI) :
  try :
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
      #print("initiator", str(initiator),"receiver", str(receiver))
      return "-1"
  except :
    print("initiator",  str(initiator),  str(initiatorLineI), "receiver",  str(receiver), str(receiverLineI))

def create_print_string_for_churn(examinedTime,printingList) :
  out_str = ""+str(examinedTime)
  for printing in printingList :
    out_str = out_str+ " " + str(printing)
  return out_str

def is_changed_something_important_about_the_connection(trace_record_recent, trace_record_previous) :
  if len(trace_record_recent) > 2 and len(trace_record_previous) > 2 and \
    trace_record_recent[3] == trace_record_previous[3] and \
    trace_record_recent[4] == trace_record_previous[4] and \
    trace_record_recent[5] == trace_record_previous[5] and \
    trace_record_recent[6] == trace_record_previous[6] and \
    trace_record_recent[7] == trace_record_previous[7] and \
    trace_record_recent[9] == trace_record_previous[9] and \
    trace_record_recent[10] == trace_record_previous[10] and \
    (trace_record_recent[2] != trace_record_previous[2] or \
     trace_record_recent[8] != trace_record_previous[8] ) :
    return False
  return True

def is_a_new_connection(whose_state_is_changed, user, lineI, neighbor, lineNeighborI) :
  if whose_state_is_changed == NEW_CONNECTION_ATTEMPT_STRING:
    return True
  elif whose_state_is_changed == NEIGHBOR_STATE_CHANGED:
    if lineNeighborI > 0:
      return is_changed_something_important_about_the_connection(
        trace[neighbor][lineNeighborI].traceRecord, trace[neighbor][lineNeighborI - 1].traceRecord)
    else:
      return True
  elif whose_state_is_changed != USER_STATE_CHANGED:
    if lineI > 0:
      return is_changed_something_important_about_the_connection(trace[user][lineI].traceRecord,
                                                                                trace[user][lineI - 1].traceRecord)
    else:
      return True
  else:
    if lineI > 0:
      user_changed = is_changed_something_important_about_the_connection(trace[user][lineI].traceRecord,
                                                                         trace[user][lineI - 1].traceRecord)
    else:
      user_changed = True
    if lineNeighborI > 0:
      neighbor_changed = is_changed_something_important_about_the_connection(trace[neighbor][lineNeighborI].traceRecord,
                                                                             trace[neighbor][
                                                                               lineNeighborI - 1].traceRecord)
    else:
      neighbor_changed = True
    if user_changed == False and neighbor_changed == False:
      return False
    else:
      return True


def predict_churn(userList, coreNumber) :
  def connect_to_neighbor_at(examinedTime, lineI, neighbor, whose_state_is_changed) :
    for lineNeighborI in range(indexOfPotentialNeighbor[neighbor], len(trace[neighbor])):
      indexOfPotentialNeighbor[neighbor] = lineNeighborI
      #time_to_online_intersection = trace[neighbor][lineNeighborI].time_to_online_intersection_at_time(examinedTime)
      if int(trace[neighbor][lineNeighborI].traceRecord[1]) == 1 \
          and trace[neighbor][lineNeighborI].sessionStart <= examinedTime < trace[neighbor][lineNeighborI].sessionEnd :
        featureVector = make_feature_vector(trace[user][lineI].traceRecord,
                                            trace[neighbor][lineNeighborI].traceRecord,
                                            traceAssignment[user], traceAssignment[neighbor], str(lineI),
                                            str(lineNeighborI))
        try:
          if predictions[featureVector] == "1":
            if is_a_new_connection(whose_state_is_changed, user, lineI, neighbor, lineNeighborI) == True :
              printingList.append("+"+neighbor)
            connectedNeighbors[neighbor] = trace[neighbor][lineNeighborI].sessionEnd
            return True
        except:
          if featureVector in misssedFeatureVector:
            misssedFeatureVector[featureVector] += 1
          else:
            misssedFeatureVector[featureVector] = 1
            errorfile.write('' + featureVector + '\n')
        break
      elif int(trace[neighbor][lineNeighborI].traceRecord[1]) == 1  and examinedTime < trace[neighbor][lineNeighborI].sessionStart :
        break
      else:
        if whose_state_is_changed == USER_STATE_CHANGED :
          whose_state_is_changed = BOTH_STATE_CHANGED
        continue
    return False
  def connect_to_neighbors_at(examinedTime, lineI):
    listOfPotentialNodes = list(range(0, NETWORK_SIZE))
    while len(connectedNeighbors) < EXPECTED_NUMBER_OF_CONNECTED_NEIGHBOR:
      if len(listOfPotentialNodes) <= 0:
        break
      indexOfRandomNode = random.randrange(len(listOfPotentialNodes))
      neighbor = str(listOfPotentialNodes[indexOfRandomNode])
      del listOfPotentialNodes[indexOfRandomNode]
      if user == neighbor or neighbor in connectedNeighbors:
        continue
      if neighbor not in indexOfPotentialNeighbor:
        indexOfPotentialNeighbor[neighbor] = 0
      connect_to_neighbor_at(examinedTime, lineI, neighbor, NEW_CONNECTION_ATTEMPT_STRING)
  def reconnect_to_neigbors(examined_time,lineI, whose_state_is_changed) :
    removeList = []
    for neighbor in connectedNeighbors :
      if connect_to_neighbor_at(examined_time,lineI, neighbor, whose_state_is_changed) == False :
        removeList.append(neighbor)
    for neighbor in removeList :
      printingList.append("-" + neighbor)
      del connectedNeighbors[neighbor]
  def check_the_connected_neighbor_changes_in_current_session(lineI) :
    nonlocal printingList
    if len(connectedNeighbors) >= 1:
      expired_neighbor = min(connectedNeighbors.items(), key=operator.itemgetter(1))
    else:
      expired_neighbor = ('-', trace[user][lineI].sessionEnd)
    prev_time = expired_neighbor[1]
    printingList = []
    while expired_neighbor[1] < trace[user][lineI].sessionEnd:
      if prev_time != expired_neighbor[1] and len(printingList) > 0 :
        outfile.write(create_print_string_for_churn(prev_time, printingList) + "\n")
        printingList = []
      if connect_to_neighbor_at(expired_neighbor[1], lineI, expired_neighbor[0], NEIGHBOR_STATE_CHANGED) == False:
        printingList.append("-" + expired_neighbor[0])
        del connectedNeighbors[expired_neighbor[0]]
        connect_to_neighbors_at(expired_neighbor[1], lineI)
      prev_time = expired_neighbor[1]
      if len(connectedNeighbors) >= 1:
        expired_neighbor = min(connectedNeighbors.items(), key=operator.itemgetter(1))
      else:
        expired_neighbor = ('-', trace[user][lineI].sessionEnd)

  fileName = str(coreNumber) + ".csv"
  errorfile = open('' + ERRFILE_PATH + fileName, "w", encoding="utf-8")
  misssedFeatureVector = {}
  for user in userList :
    fileName = str(user) + ".csv"
    outfile = open('' + OUTFILE_PATH + fileName, "w", encoding="utf-8")
    indexOfPotentialNeighbor = {}
    connectedNeighbors = {}
    for lineI in range (0,len(trace[user])) :
      printingList = []
      examined_time = trace[user][lineI].sessionStart
      if int(trace[user][lineI].traceRecord[1]) != 1 :
        if len(connectedNeighbors) > 0 :
          for neighbor in connectedNeighbors :
            printingList.append("-" + neighbor)
          outfile.write(create_print_string_for_churn(examined_time, printingList) + "\n")
          connectedNeighbors = {}
      else :
        if lineI == 0 or int(trace[user][lineI-1].traceRecord[1]) != 1  :
          connectedNeighbors = {}
        else :
          if (examined_time/END_OF_A_HOUR)%1 == 0.0 :
            reconnect_to_neigbors(examined_time, lineI, BOTH_STATE_CHANGED)
          else :
            reconnect_to_neigbors(examined_time, lineI, USER_STATE_CHANGED)
        connect_to_neighbors_at(examined_time,lineI)
        if len(printingList) > 0 :
          outfile.write(create_print_string_for_churn(examined_time, printingList)+"\n")
        check_the_connected_neighbor_changes_in_current_session(lineI)
    if len(connectedNeighbors) > 0:
      for neighbor in connectedNeighbors:
        printingList.append("-" + neighbor)
      outfile.write(create_print_string_for_churn(END_OF_A_DAY, printingList) + "\n")
    outfile.close()

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
