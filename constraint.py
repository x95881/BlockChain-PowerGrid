#Code from Team Apache Spark (AY17) referenced to implement pyspark
#Apache Spark Python documentation referenced at https://spark.apache.org/docs/2.1.2/api/python/index.html

"""
Create a dictionary where the keys are signal IDs, and the values are dictionaries. Thse sub-dictionaries contain theconstraint information given in the constraint file.
"""
def createConstraintMap(inFile):
    constraintMap = dict()
    with open(inFile,'r') as f:
        for line in f:
            line = line.strip().split(',')
            sigId = line[0]
            pmu = float(line[1])
            sigType = line[2] #voltage, current etc
            lowBound = float(line[3])
            upBound = float(line[4])
            constraintMap[sigId] = {"PMU":pmu, "Signal Type":sigType, "Lower Bound":lowBound, "Upper Bound":upBound}
    return constraintMap

"""
Allows to export our work done in the 'if __name__ == "__main__":' portion of this file. See below.
"""
def constraintAnalysis(distFile, cm):
    return distFile.flatMap(lambda chunk: chunk.split('\n'))\
              .map(lambda line: line.split(','))\
              .map(lambda line: (line[1], line[2:]))\
              .filter(lambda line: line[0] in cm)\
              .filter(lambda line: cm[line[0]]["Lower Bound"] > float(line[-1][-1]) or cm[line[0]]["Upper Bound"] < float(line[-1][-1]))

"""
Code that is run be default when constraint.py is run.
"""
if __name__ == "__main__":
    from pyspark import SparkContext
    import sys
    #A spark context is the object that allows to parallize our work
    sc = SparkContext(appName="RTSGconstraintParallel")
    #Reduces the amount of loggin done by pyspark to STDERROR
    sc.setLogLevel("ERROR")
    #Ensure the proper number of arguments are passed to the command line
    if len(sys.argv) != 3:
        raise Exception("Constraint algorithm takes 3 arguments: {Script Name} {Constraints} {Data}")
    dataFile = sys.argv[2]
    #Parallelize our dataFile into chunks to be operated on in parallel
    distFile = sc.textFile(dataFile,1*8).cache() 
    distFile.setName("PartitionedPMUData")
    cm = createConstraintMap(sys.argv[1])

    #The part that actually modifies our input data as follows:
        #Split each 'chunk' on the newline character
        #Split the individual lines by comma into lists
        #Change each list into a tuple (v1,v2), where v1 is the SIGID and v2 is the rest of the information
        #Remove (v1,v2) pairs where v1 is not present in our constraints file (represnted by a dict)
        #Remove (v1,v2) pairs where the measurement in v2 falls within the constraint bounds
    #What remains are the (v1,v2) pairs where v2 falls outside the constraints (i.e. an anomaly) 
    map_phase = print(distFile.flatMap(lambda chunk: chunk.split('\n'))
                                .map(lambda line: line.split(','))
                                .map(lambda line: (line[1], line[2:]))
                                .filter(lambda line: line[0] in cm)
                                .filter(lambda line: cm[line[0]]["Lower Bound"] > float(line[-1][-1]) or cm[line[0]]["Upper Bound"] < float(line[-1][-1]))
                                .collect())
