import json

with open('../res/peersimNeighborhood.json') as json_file:
  dataNeighborhood = json.load(json_file)
  for node in dataNeighborhood :
    print(node, dataNeighborhood[node])