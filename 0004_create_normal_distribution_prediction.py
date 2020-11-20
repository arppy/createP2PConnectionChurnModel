import numpy as np
import csv

OUTFILE_PATH = "random_distribution/"
infile_name = "x100/base_statx100.csv"
with open('' + infile_name) as csvfile:
  statCSV = csv.reader(csvfile, delimiter=' ', quotechar='|')

  for line in statCSV:
    if len(line) > 0 :
      hour = line[0]
      mean = float(line[1])
      stdev = float(line[5])
      number_of_nodes = int(line[6])
      predList = np.random.normal(mean, stdev, number_of_nodes)
      distribution = {}
      max = 0
      min = 100
      for number_of_neighbor in predList :
        final_number_of_neighbor = int(round(number_of_neighbor))
        if max < final_number_of_neighbor:
          max = final_number_of_neighbor
        if min > final_number_of_neighbor:
          min = final_number_of_neighbor
        if final_number_of_neighbor not in distribution:
          distribution[final_number_of_neighbor] = 0
        distribution[final_number_of_neighbor] += 1
      fileName = "degreeDistribution-" + str(hour) + ".csv"
      outFile = open('' + OUTFILE_PATH + fileName, "w", encoding="utf-8")
      for numberOfConnectedNeighbor in range(0, max + 1):
        if numberOfConnectedNeighbor not in distribution:
          distribution[numberOfConnectedNeighbor] = 0
        outFile.write(str(numberOfConnectedNeighbor) + " " + str(distribution[numberOfConnectedNeighbor]) + " " + str(
          distribution[numberOfConnectedNeighbor] / number_of_nodes) + "\n")
      outFile.close()