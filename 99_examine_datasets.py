import json
import csv
import sys


dataSetInputFile = "dataset/1010rr1101rr10.csv"
predictionInputFile = "1010rr1101rr10network100000x100.csv"


trainingDataSet = {}
i=0
trainingMaxValues = []
trainingMinValues = []
with open(dataSetInputFile) as file1:
  fileIn = csv.reader(file1, delimiter=',', quotechar='|')
  for line in fileIn :
    i+=1
    if i > 1 :
      trainingDataSet[str(line[1:])] = 1
      for i in range(1,len(line)) :
        if len(trainingMinValues) < i :
          trainingMinValues.append(line[i])
          trainingMaxValues.append(line[i])
        else :
          if line[i] > trainingMaxValues[i-1] :
            trainingMaxValues[i-1] = line[i]
          if line[i] < trainingMinValues[i-1] :
            trainingMinValues[i-1] = line[i]

    #if i<10 :
    #  print(str(line[1:]))

predictionDataSet = {}
i=0
predictionMaxValues = []
predictionMinValues = []
with open(predictionInputFile) as file2:
  fileIn = csv.reader(file2, delimiter=',', quotechar='|')
  for line in fileIn :
    i += 1
    predictionDataSet[str(line)] = 1
    for i in range(0, len(line)):
      if len(predictionMinValues) < i + 1:
        predictionMinValues.append(line[i])
        predictionMaxValues.append(line[i])
      else:
        if line[i] > predictionMaxValues[i]:
          predictionMaxValues[i] = line[i]
        if line[i] < predictionMinValues[i]:
          predictionMinValues[i] = line[i]
    #if i<10 :
    #  print(str(line))

counter = 0
for sample in trainingDataSet :
   if sample in predictionDataSet :
     counter += 1

print("number of identical sample in both dataset:"+str(counter))
print("min-max values by position")
for i in range(0, len(predictionMinValues)):
  print(str(i),"trainingSet",str(trainingMinValues[i]),str(trainingMaxValues[i]),"predictionSet:",str(predictionMinValues[i]),predictionMaxValues[i])