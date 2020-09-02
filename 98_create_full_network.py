OUTFILE_PATH = "../res/"
fileName = "peersimNeighborhood100000.json"
outFile = open('' + OUTFILE_PATH + fileName, "w", encoding="utf-8")
outFile.write("{\n")
for i in range(0,99999) :
  outFile.write("\""+str(i)+"\":[ ")
  for j in range(0,99999) :
    if i != j :
      outFile.write("\"" + str(j) + "\", ")
  outFile.write("\"" + str(99999) + "\" ],\n")
outFile.write("\""+str(99999)+"\":[ ")
for j in range(0,99999) :
  outFile.write("\"" + str(j) + "\", ")
outFile.write("\"" + str(99998) + "\" ]\n")
outFile.write("}\n")
outFile.close()
