from __future__ import print_function
from pyspark import SparkContext
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

if __name__ == "__main__":
    sc = SparkContext()
    # Load and parse the data file into an RDD of LabeledPoint.
    trainingData = MLUtils.loadLibSVMFile(sc, 'totalTrainRes1')
    testData = MLUtils.loadLibSVMFile(sc, 'totalTestRes1')

    model = RandomForest.trainClassifier(trainingData, numClasses=9, categoricalFeaturesInfo={},
                                         numTrees=1000, featureSubsetStrategy="auto",
                                         impurity='gini', maxDepth=20, maxBins=100)

    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)

    res = labelsAndPredictions.collect()
    f = open("totalLPRes1", "w")
    for i in xrange(len(res)):
        f.write(str(res[i])+'\n')
    f.close()

    # Save model
    model.save(sc, "totalRFModel-2")

    sc.stop()