# Microsoft Malware Detection on Spark

This is project 2 in CSCI 8360 course at University of Georgia, Spring 2016. In this project we were challenged to predict the class labels of the test set by thousands of malware documents from 9 different labeled classes (the train set). The total data size is around 500 GB. Our approach contains two parts: 1) feature engineering; 2) model training. In the end, the model achieves 98.9% prediction accuracy in the testing set.

## Getting Started

Here are the instructions to run the python scripts with Spark on Amazon AWS EMR, be sure to change the data path, such as Hadoop path in your master node.

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
pip install nltk numpy scipy 
```

## Procedure
There are two parts in this project: feature engineering and model training.  
###Feature Engineering
We consider and extract the following features from the train and test data:

1. Single hexadecimal codes and ngrams (here n = 2) from the bytes files;
2. Asm file pixel intensity: proposed by the winner's solution in Kaggle challenge, gray-scale images are extracted from asm files and we pick the first 1000 pixels in the asm image;
3. File properties: asm file size, bytes file size and the size ratio (size of asm file divided by size of bytes file);
4. Proportion of lines or characters in each "sectoin", which is recognized as the first word on each line, such as .data, .rdata etc;
5. Number of occurences of specific dll's which is recognized by the .dll extension in the text.

###Model Training
* The ngrams and asm pixel intensities are calculated with Spark on AWS EMR.
* The best model is Random Forest with max_depth = 20, num_trees = 1000, max_bin = 30. Another version of random forest with more number of trees and deeper depth was killed by AWS for some reason, which should have better performance than our current model. 
* For the idea of pixel intensity, please see **[L. Nataraj 2014](http://sarvamblog.blogspot.com/)**,for details.

###Prediction

We generate the same type of features for the testing data, and use our saved model to predict the labels.

## Authors

* **[Xiaodong Jiang](https://www.linkedin.com/in/xiaodongjiang)** - Ph.D. Student, *Department of Statistics*
* **[Yang Song](https://www.linkedin.com/in/yang-song-74298a118/en)** - M.S. Student, *Department of Statistics*
* **[Yaotong Cai](https://www.linkedin.com/in/yaotong-colin-cai-410ab026)** - Ph.D. Candidate, *Department of Statistics*


## Acknowledgments

* Thanks all team members for the laborious work and great collaboration.
* Thanks **[Dr. Quinn](http://cobweb.cs.uga.edu/~squinn/)** for setting up AWS tools for us, even though our potential best model was killed/disappear in the last minute.
