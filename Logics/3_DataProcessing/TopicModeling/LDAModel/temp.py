
#!/usr/bin/env python3
import json

with open(r"/home/matteowissel/BigDataFinal/BIG_DATA_TWITTER_ANALYSIS/Logics/3_DataProcessing/TopicModeling/LDAModel/hyperparam.json") as json_file:
    hyperparameters = json.load(json_file)

# Extract hyperparameters
numTopics = hyperparameters["numTopics"]
maxIter = hyperparameters["maxIter"]
maxTopicsPerRow = hyperparameters["maxTopicsPerRow"]
print(numTopics)