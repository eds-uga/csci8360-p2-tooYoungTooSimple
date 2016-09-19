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


class preprocessor(object):
    '''
    preprocess byte data for microsoft malware detection project

    parameters:
    - gramList: a list of integers, representing the number of grams to use
      default value: [1, 2], use unigram and bigram
    - freqThreshold: a threshold to filter term (grams) frequency
      default value: 200

    methods:
    - byteFeatureGenerator(X, y)
      convert byte file and labels into a sparse matrix
      parameters:
        - X: pyspark rdd, with (id, rawDoc) format
        - y: pyspark rdd, with (id, label) format
    '''

    def __init__(self, grams=[1, 2], freqThreshold=200):
        self.grams = grams
        self.freqThreshold = freqThreshold

    # helper methods
    def stripFileNames(self, stringOfName):
        splits = stringOfName.split("/")
        name = splits[-1][:20]

        return name

    # token a document, only keep 2-digit code, and its grams
    def tokenEachDoc(self, aDoc):
        '''
        return a dictionary of item-freq, here items are single words and grams
        '''
        tmpWordList = [x for x in re.sub('\r\n', ' ', aDoc).split() if len(x) == 2 and x != '??']
        tmpGramList = []
        for i in xrange(len(self.grams)):
            tmpGramList.append([''.join(x) for x in list(ngrams(tmpWordList, self.grams[i]))])

        # here tmpGramList is a list of list, here we should remove the inner lists
        sumGramList = tmpWordList + list(
            itertools.chain.from_iterable(tmpGramList))  # this is a very long list, depends on the gram numbers
        sumGramDict = dict(Counter(sumGramList))

        for keys in sumGramDict.keys():
            if sumGramDict[keys] < self.freqThreshold:
                del sumGramDict[keys]

        return sumGramDict

    def byteFeatureGenerator(self, X, y):  # format, (id, dictionary of items)
        '''
        return an rdd of (id, (freq dictionary of items, label))
        '''
        tokenizedX = X \
            .map(lambda x: (self.stripFileNames(x[0]), self.tokenEachDoc(x[1]))) \
            .join(y)

        return tokenizedX

    def convertHexToInt(self, hexStr):
        return (int('1' + str(hexStr), 16) - 255)

    def convertDict(self, textDict):
        tmp = {}
        for oldKey, value in textDict.items():
            # textDict[self.convertHexToInt(oldKey)] = textDict[oldKey]
            # del textDict[oldKey]
            tmp[self.convertHexToInt(oldKey)] = value
        return tmp

    def convertToSVMFormat(self, Str):
        newStr = re.sub("\), \(", " ", Str)
        newStr = re.sub("\, ", ":", newStr)
        newStr = re.sub("\)\]\)", "", newStr)
        newStr = re.sub("\:\[\(", " ", newStr)
        newStr = re.sub("\(", "", newStr)

        return newStr


if __name__ == '__main__':
    sc = SparkContext()
    prc = preprocessor()

    byteFiles = sc.wholeTextFiles("/Users/songyang/Documents/CSCI8360/Project2/byteTrainVSmall/")

    trainFileIndex = sc.textFile("/Users/songyang/Documents/CSCI8360/Project2/X_train_small.txt").zipWithIndex()\
        .map(lambda x: (x[1], x[0]))
    trainFileLabel = sc.textFile("/Users/songyang/Documents/CSCI8360/Project2/y_train_small.txt").zipWithIndex()\
        .map(lambda x: (x[1], x[0]))

    ###
    vSmallFileIndex = sc.textFile("/Users/songyang/Documents/CSCI8360/Project2/indexVSmall")\
        .map(lambda x: (x, 1))
    ###

    labelRDD = trainFileIndex.join(trainFileLabel)\
        .map(lambda x: (x[1][0], x[1][1]))


    ###
    labelVSmallRDD = labelRDD.join(vSmallFileIndex)\
        .map(lambda x: (x[0], x[1][0]))
    ###

    trainData = byteFiles.map(lambda x: (prc.stripFileNames(x[0]), prc.tokenEachDoc(x[1])))\
        .map(lambda x: (x[0], prc.convertDict(x[1])))\
        .join(labelVSmallRDD)\
        .map(lambda x: (int(x[1][1]), sorted(x[1][0].iteritems(), key = lambda d:d[0])))\
        .map(lambda x: prc.convertToSVMFormat(str(x)))








    # res1 = labelVSmallRDD.collect()
    # f1 = open('labelVSmallRDD', 'w')
    # for i in xrange(len(res1)):
    #     f1.write(str(res1[i]) + '\n')
    # f1.close()
    # #print(res1)
    #
    res2 = trainData.collect()
    f2 = open('trainData', 'w')
    for i in xrange(len(res2)):
        f2.write(str(res2[i]) + '\n')
    f2.close()




















