"""
Random Forest Classification for Microsoft Malware Data
"""
from __future__ import print_function
from pyspark import SparkContext
# $example on$
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PythonRandomForestClassification")
    # Here the data should be combination of asm and bytes files 
    
    # train set
    trainingasm = MLUtils.loadLibSVMFile(sc, 'path-to-asm-matrix-file')
    trianingbytes = MLUtils.loadLibSVMFile(sc, 'path-to-bytes-sparse-matrix-file')
    
    # test set
    testasm = MLUtils.loadLibSVMFile(sc, 'path-to-asm-matrix-file')
    testbytes = MLUtils.loadLibSVMFile(sc, 'path-to-bytes-sparse-matrix-file')

    # Train a RandomForest model.
    model = RandomForest.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                         numTrees=1000, featureSubsetStrategy="auto",
                                         impurity='gini', maxDepth=4, maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification forest model:')
    print(model.toDebugString())

    # Save and load model
    model.save(sc, "target/tmp/myRandomForestClassificationModel")
    sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
    # $example off$
