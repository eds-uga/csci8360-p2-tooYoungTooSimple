from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.tree import DecisionTree
from sys import argv
import numpy as np
import re
import urllib2
import csv
from nltk.util import ngrams
import itertools
from collections import Counter

if __name__ == '__main__':

    def convertASMtoSVM(Str):
        '''
        convert each line of asm training/test file to the SVM format which can be loaded by MLUtils.loadLibSVMFile()
        '''
        flag = 65793 # The first 65792 coordinate is from byte files.
        newStr = ''
        for i in xrange(len(Str)):
            if Str[i] != '[' and Str[i] != ']' and Str[i] != ' ' \
                    and Str[i] != "'":
                if Str[i] == ",":
                    newStr += ' ' + str(flag) + ':'
                    flag += 1
                else:
                    newStr += Str[i]
        return newStr

    conf = SparkConf().set("spark.driver.maxResultSize", "6g")
    sc = SparkContext(conf=conf)

    labelRDDTest = sc.textFile("X_test.txt").zipWithIndex()

    byteTrainFiles = sc.textFile("trainDataTotal-2")
    byteTestFiles = sc.textFile("testDataTotal-2")

    asmTrainFiles = sc.textFile("asmfeature.txt")
    asmTestFiles = sc.textFile("asmfeaturetest.txt")

    asmTrain = asmTrainFiles.map(lambda x: convertASMtoSVM(x).split(' ', 1))
    asmTest = asmTestFiles.map(lambda x: convertASMtoSVM(x).split(' ', 1))

    byteTrain = byteTrainFiles\
        .filter(lambda x: len(x.split("#", 1)[0].split("-", 1)[1]) == 20)\
        .map(lambda x: (x.split("#", 1)[0].split("-", 1)[1],
                        x.split("#", 1)[0].split("-", 1)[0] + '-' + x.split("#", 1)[1]))

    byteTest = byteTestFiles\
        .map(lambda x: (x.split("#", 1)[0].split("-", 1)[1],
                        x.split("#", 1)[0].split("-", 1)[0] + '-' + x.split("#", 1)[1]))

    totalTrain = byteTrain.join(asmTrain)\
        .map(lambda x: (x[0], x[1][0] + " " + x[1][1]))\
        .map(lambda x: str(int(x[1].split("-", 1)[0])-1) + " " + x[1].split("-", 1)[1])

    totalTest = byteTest.join(asmTest)\
        .map(lambda x: (x[0], x[1][0] + " " + x[1][1]))\
        .join(labelRDDTest)\
        .map(lambda x: (x[1][1], x[0] + "*" + x[1][0]))\
        .sortByKey()\
        .map(lambda x: '1' + " " + x[1].split("-", 1)[1])

    # Write the file of total training/test data.
    testRes = totalTest.collect()
    f1 = open('totalTestRes2', 'w')
    for i in xrange(len(testRes)):
        f1.write(str(testRes[i]) + '\n')
    f1.close()

    trainRes = totalTrain.collect()
    f2 = open('totalTrainRes2', 'w')
    for i in xrange(len(trainRes)):
        f2.write(str(trainRes[i]) + '\n')
    f2.close()

    sc.stop()
