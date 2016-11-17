import json
import sys

outputFile = open("data/" + sys.argv[1] + "/" + sys.argv[1] + "-textOnly-raw", "w")
inputFile = open("data/" + sys.argv[1] + "/" + sys.argv[1] + ".json", "r")

for line in inputFile:
    data = json.loads(line)
    textAsString = data["text"]["string"].encode("utf-8").replace('\n', ' ')
    
    #print data["text"]["string"].encode("utf-8")
    #print("=========================")

    outputFile.write(data["text"]["string"].encode("utf-8") + '\n')
