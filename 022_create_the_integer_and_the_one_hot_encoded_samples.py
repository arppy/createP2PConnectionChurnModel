import csv

HOUR_LOOKUP = {}
HOUR_INPUT_FILE_PATH = "../res/featureDict/dict_hours.csv"
HOUR_MAX_VALUE = 0
with open(HOUR_INPUT_FILE_PATH) as csvfile :
  dictReader = csv.reader(csvfile, delimiter=';')
  for line in dictReader :
    HOUR_LOOKUP[str(line[2])] = line[1]
    if int(line[1]) > HOUR_MAX_VALUE :
      HOUR_MAX_VALUE = int(line[1])


minAndroidVersion = 999999
maxAndroidVersion = 0
ANDROID_VERSION_LOOKUP = {}
ANDROID_VERSION_INPUT_FILE_PATH = "../res/featureDict/dict_android_versions.csv"
ANDROID_MAX_VALUE = 0
with open(ANDROID_VERSION_INPUT_FILE_PATH) as csvfile :
  dictReader = csv.reader(csvfile, delimiter=';')
  for line in dictReader :
    ANDROID_VERSION_LOOKUP[str(line[2])] = line[1]
    if maxAndroidVersion < int(line[2]) :
      maxAndroidVersion = int(line[2])
    if minAndroidVersion > int(line[2]) :
      minAndroidVersion = int(line[2])
    if int(line[1]) > ANDROID_MAX_VALUE:
      ANDROID_MAX_VALUE = int(line[1])

WIFI_LOOKUP = {}
WIFI_INPUT_FILE_PATH = "../res/featureDict/dict_wifi_bandwidth10.csv"
WIFI_MAX_VALUE = 0
with open(WIFI_INPUT_FILE_PATH) as csvfile :
  dictReader = csv.reader(csvfile, delimiter=';')
  for line in dictReader :
    WIFI_LOOKUP[str(line[2]).replace(" ", "")] = line[1]
    if int(line[1]) > WIFI_MAX_VALUE:
      WIFI_MAX_VALUE = int(line[1])

MOBNET_LOOKUP = {}
MOBNET_INPUT_FILE_PATH = "../res/featureDict/dict_mobile_net_type10.csv"
MOBNET_MAX_VALUE = 0
with open(MOBNET_INPUT_FILE_PATH) as csvfile :
  dictReader = csv.reader(csvfile, delimiter=';')
  for line in dictReader :
    MOBNET_LOOKUP[str(line[2])] = line[1]
    if int(line[1]) > MOBNET_MAX_VALUE:
      MOBNET_MAX_VALUE = int(line[1])

ROAMING_LOOKUP = {}
ROAMING_INPUT_FILE_PATH = "../res/featureDict/dict_roaming.csv"
ROAMING_MAX_VALUE = 0
with open(ROAMING_INPUT_FILE_PATH) as csvfile :
  dictReader = csv.reader(csvfile, delimiter=';')
  for line in dictReader :
    ROAMING_LOOKUP[str(line[2])] = line[1]
    if int(line[1]) > ROAMING_MAX_VALUE:
      ROAMING_MAX_VALUE = int(line[1])

NAT_LOOKUP = {}
NAT_INPUT_FILE_PATH = "../res/featureDict/dict_nat.csv"
NAT_MAX_VALUE = 0
with open(NAT_INPUT_FILE_PATH) as csvfile :
  dictReader = csv.reader(csvfile, delimiter=';')
  for line in dictReader :
    NAT_LOOKUP[str(line[2])] = line[1]
    if int(line[1]) > NAT_MAX_VALUE:
      NAT_MAX_VALUE = int(line[1])

WEBRTC_TEST_LOOKUP = {}
WEBRTC_TEST_INPUT_FILE_PATH = "../res/featureDict/dict_webrtctest.csv"
WEBRTC_MAX_VALUE = 0
with open(WEBRTC_TEST_INPUT_FILE_PATH) as csvfile :
  dictReader = csv.reader(csvfile, delimiter=';')
  for line in dictReader :
    WEBRTC_TEST_LOOKUP[str(line[2])] = line[1]
    if int(line[1]) > WEBRTC_MAX_VALUE:
      WEBRTC_MAX_VALUE = int(line[1])

COUNTRY_LOOKUP = {}
COUNTRY_INPUT_FILE_PATH = "../res/featureDict/dict_country10.csv"
COUNTRY_MAX_VALUE = 0
with open(COUNTRY_INPUT_FILE_PATH) as csvfile:
  dictReader = csv.reader(csvfile, delimiter=';')
  for line in dictReader:
    COUNTRY_LOOKUP[str(line[2])] = line[1]
    if int(line[1]) > COUNTRY_MAX_VALUE:
      COUNTRY_MAX_VALUE = int(line[1])

ORG_LOOKUP = {}
ORG_INPUT_FILE_PATH = "../res/featureDict/dict_org10.csv"
ORG_MAX_VALUE = 0
with open(ORG_INPUT_FILE_PATH) as csvfile:
  dictReader = csv.reader(csvfile, delimiter=';')
  for line in dictReader:
    ORG_LOOKUP[str(line[2])] = line[1]
    if int(line[1]) > ORG_MAX_VALUE:
      ORG_MAX_VALUE = int(line[1])

def categoryToOneHotEncodeVector(value, max_value) :
  oneHotVector = [0] * (max_value+1)
  if value >=0  :
    oneHotVector[value] = 1
  return oneHotVector

def categoryToIntegerEncodeVector(value, startPoz) :
  return startPoz+value+1

def listToSvmlightStr(outList) :
  outStr='0 '
  i=0
  for element in outList :
    if element != 0 :
      outStr = outStr + str(i) + ":" +str(element) + " "
    i+=1
  return outStr[:-1]

def listToCsvStr(outList, delim=',') :
  outStr=''
  if len(outList) > 1 :
    outStr=str(outList[0])
  skipFirst = True
  for element in outList :
    if skipFirst == True :
      skipFirst = False
      continue
    outStr = outStr + str(delim) + str(element)
  return outStr

'''
print("HOUR_MAX_VALUE",HOUR_MAX_VALUE)
print("ANDROID_MAX_VALUE",ANDROID_MAX_VALUE)
print("WIFI_MAX_VALUE",WIFI_MAX_VALUE)
print("MOBNET_MAX_VALUE",MOBNET_MAX_VALUE)
print("ROAMING_MAX_VALUE",ROAMING_MAX_VALUE)
print("NAT_MAX_VALUE",NAT_MAX_VALUE)
print("WEBRTC_MAX_VALUE",WEBRTC_MAX_VALUE)
print("COUNTRY_MAX_VALUE",COUNTRY_MAX_VALUE)
print("ORG_MAX_VALUE",ORG_MAX_VALUE)
exit()
'''

fileNameSuffix = "allHour"
fileCSV = open('1010rr1101rr10network100000x'+fileNameSuffix+'.csv', "w", encoding="utf-8")
fileSVMLight = open('1010rr1101rr10network100000x'+fileNameSuffix+'.svmlight', "w", encoding="utf-8")
with open('trace_network100000x'+fileNameSuffix+'.csv') as csv_file :
  featureVectorReader = csv.reader(csv_file, delimiter=' ', quotechar='|')
  lineI = 0
  for wholeLine in featureVectorReader:
    lineI += 1
    if lineI % 1000000 == 0:
      print(lineI)
    sampleListOneHot = []
    sampleListInteger = []
    key = str(wholeLine[0])
    line = str(wholeLine[0]).split(";")
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[0]), HOUR_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[1]), ANDROID_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[2]), ANDROID_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[3]), WIFI_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[4]), WIFI_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[5]), MOBNET_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[6]), MOBNET_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[7]), ROAMING_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[8]), ROAMING_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[9]), NAT_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[10]), NAT_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[11]), WEBRTC_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[12]), WEBRTC_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[13]), COUNTRY_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[14]), COUNTRY_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[15]), ORG_MAX_VALUE))
    sampleListOneHot.extend(categoryToOneHotEncodeVector(int(line[16]), ORG_MAX_VALUE))
    oneHotEncode = listToSvmlightStr(sampleListOneHot)
    fileSVMLight.write('' + oneHotEncode + '\n')
    startPoz = 0
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[0]), startPoz))
    startPoz += HOUR_MAX_VALUE + 2
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[1]), startPoz))
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[2]), startPoz))
    startPoz += ANDROID_MAX_VALUE + 2
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[3]), startPoz))
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[4]), startPoz))
    startPoz += WIFI_MAX_VALUE + 2
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[5]), startPoz))
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[6]), startPoz))
    startPoz += MOBNET_MAX_VALUE + 2
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[7]), startPoz))
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[8]), startPoz))
    startPoz += ROAMING_MAX_VALUE + 2
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[9]), startPoz))
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[10]), startPoz))
    startPoz += NAT_MAX_VALUE + 2
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[11]), startPoz))
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[12]), startPoz))
    startPoz += WEBRTC_MAX_VALUE + 2
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[13]), startPoz))
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[14]), startPoz))
    startPoz += COUNTRY_MAX_VALUE + 2
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[15]), startPoz))
    sampleListInteger.append(categoryToIntegerEncodeVector(int(line[16]), startPoz))
    startPoz += ORG_MAX_VALUE + 2
    integerEncode = listToCsvStr(sampleListInteger)
    fileCSV.write('' + integerEncode + '\n')
fileCSV.close()
fileSVMLight.close()
