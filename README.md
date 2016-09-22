# Scalable Document Classification with Naive Bayes in Spark

This is project 2 in CSCI 8360 course at University of Georgia, Spring 2016. In this project we were challenged to predict the class labels of the test set by thousands of malware documents from 9 different labeled classes (the train set). The total data size is 500 GB. Our approach contains two parts: 1) feature engineering; 2) model training. In the end, the model achieves   prediction accuracy in the test set.

## Getting Started

Here are the instructions to run the python scripts with Spark locally on Mac OS or Linux, if one wants to run this code in Amazon AWS EMR, be sure to change the data path, such as Hadoop path in your master node.

### The data
There are in total 9 class labels for each of the documents, which are Ramnit, Lollipop, Kelihos_ver3, Vundo, Simda, Tracur, Kelihos_ver1, Obfuscator.ACY and Gatak. Each of the files is recognized by a unique 20 serial number (hash), and consists of two types of files: asm file and binary file. The binary file contains lines of hexadecimal pairs; the asm file contains specific structures, names of extensions, characters as well as other information.Lists of documents in the train set and test set as well as labels of training documents are provided in the following files:

```
X_train_small.txt, y_train_small.txt
X_test_small.txt, y_test_small.txt
X_train.txt, y_train.txt
X_test.txt
```
### Installing

Install necessary python modules as below,

```
pip install nltk stop_words numpy  
```

## Procedure
There are two parts in this project: feature engineering and model training.  
###Feature Engineering
We consider and extract the following features from the train and test data:

1. 1 to 4 byte ngrams from the bytes files;
2. asm file pixel intensity: proposed by the winner's solution for this challenge, gray-scale images are extracted from asm files and we pick the first 1000 pixels in the asm image;
3. File properties: asm file size, bytes file size and the size ratio (size of asm file divided by size of bytes file);
4. Proportion of lines or characters in each "sectoin", which is recognized as the first word on each line, such as .data, .rdata etc;
5. Number of occurences of specific dll's which is recognized by the .dll extension in the text.

###Model Training
* The ngrams and asm pixel intensities are calculated with Spark RDD operations.
* We apply Random Forest as the model to be trained with the above features, setting number of trees to be 1000 and .
* For the idea of pixel intensity, Please see L. Nataraj, [here](http://sarvamblog.blogspot.com/), 2014 for details.

###Prediction





## Running
To run the code, be sure change the data path and install all dependent python modules, and run with spark-submit command simply as below,

```
spark-submit main.py
```

## Authors

* **[Xiaodong Jiang](https://www.linkedin.com/in/xiaodongjiang)** - Ph.D. Student, *Department of Statistics*
* **[Yang Song](https://www.linkedin.com/in/yang-song-74298a118/en)** - M.S. Student, *Department of Statistics*
* **[Yaotong Cai](https://www.linkedin.com/in/yaotong-colin-cai-410ab026)** - Ph.D. Candidate, *Department of Statistics*
* **Jiankun Zhu** - Ph.D. Student, *Department of Statistics*

## Acknowledgments

* Thanks all team members for the laborious work and great collaboration.
* Thanks [Dr. Quinn](http://cobweb.cs.uga.edu/~squinn/), .
