import numpy as np
import pandas as pd
import VA_PYTHON as va
import os
import gzip
from collections import defaultdict
from dateutil.parser import parse
import datetime as dt
import nltk
from sklearn import svm
from ftplib import FTP
import urllib2
import ipdb
import re
import utils


DATA_ADD = os.getenv('DATA')

class Forecaster(object):
    '''
    Forecaster is wrapper class for different classifiers

    Forecaster can:
        (1) train and test on historical edgar data

        (2) make predictions on new incoming data

    Constructor Args:
        indexFilePath(str): path to the index file of all training data

    '''

    def __init__(self,indexFilePath):
        self.indexFilePath = indexFilePath
        self.indexfile = pd.read_csv(indexFilePath,sep = ',',names = ['date','symbol','label','path'], parse_dates = ['date',])
        self.classifier = None

        self.featNum = 0
        self.trainNum = 0
        self.trainFeature = np.zeros(1)
        self.trainLabel = np.zeros(1)

        self.testNum = 0
        self.testFeature = np.zeros(1)
        self.testLabel = np.zeros(1)

        self.devNum = 0
        self.devFeature = np.zeros(1)
        self.devLabel = np.zeros(1)

    def readTrainFile(self,dictionarypath,trainfilename):

        with open(dictionarypath,'r') as f:
            wordlist = f.read()
            wordlist = wordlist.split('\n')
            wordlist.remove('')
            wordlist = [item.split(':')[0] for item in wordlist]

        self.Vocabulary = {}
        keyIndex = 1
        for key in set(wordlist):
            self.Vocabulary[key] = keyIndex
            keyIndex += 1
        self.featNum = len(self.Vocabulary) + 1
        print 'length of voc = ' + str(len(self.Vocabulary))

        trainfile = pd.read_csv(trainfilename, header = None)
        self.trainNum = trainfile.shape[0]
        self.trainFeature = np.zeros((self.trainNum,self.featNum))
        self.trainLabel = np.zeros(self.trainNum)

        index = 0
        for item in utils.DataIterator(trainfilename):
            label = int(item['label'])
            BOW = item['BOW']
            sue =  float(item['SUE'] + 200)

            for word,count in BOW.items():
                try:
                    position = self.Vocabulary[word]
                    self.trainFeature[index][position] = count
                except KeyError:
                    pass
            self.trainLabel[index] = label
            self.trainFeature[index][0] = sue
            index += 1

    def readTestFile(self,testfilename):
        testfile = pd.read_csv(testfilename, header = None)

        self.testNum = testfile.shape[0]
        self.testFeature = np.zeros((self.testNum,self.featNum))
        self.testLabel = np.zeros(self.testNum)

        index = 0
        for item in utils.DataIterator(testfilename):
            label = int(item['label'])
            BOW = item['BOW']
            sue = float(item['SUE'] + 200)
            for word,count in BOW.items():
                try:
                    position = self.Vocabulary[word]
                    self.testFeature[index][position] = count
                except KeyError:
                    pass
            self.testLabel[index] = label
            self.testFeature[index][0] = sue
            index += 1

    def readDevFile(self,devfilename):
        devfile = pd.read_csv(devfilename, header = None)
        self.devNum = devfile.shape[0]
        self.devFeature = np.zeros((self.devNum,self.featNum))
        self.devLabel = np.zeros(self.devNum)

        index = 0
        for item in utils.DataIterator(devfilename):
            label = int(item['label'])
            BOW = item['BOW']
            sue = float(item['SUE']+200)
            for word,count in BOW.items():
                try:
                    position = self.Vocabulary[word]
                    self.devFeature[index][position] = count
                except KeyError:
                    pass
            self.devLabel[index] = label
            self.devFeature[index][0] = sue
            index += 1

    def setClassifier(self,classifier_):
        '''
        set the classifier of the forecaster.

        The classifier should have a fit(X,y), score(X,y) and predict(X) functions

        Args:
            classifier: instance of a classifier
        '''
        self.classifier = classifier_

    def GenerateTrainTestDateSet(self,traincut,devcut):

        '''
        cut training set and testing set based on cut date(cut date include in training set)
        '''

        length = self.indexfile.shape[0]
        trainCut = int(traincut*length)
        devCut = int(devcut*length)
        trainfile = self.indexfile[:trainCut]
        devfile = self.indexfile[trainCut:devCut]
        testfile = self.indexfile[devCut:]

        trainfileName = 'train.key'
        devfileName = 'dev.key'
        testfileName = 'test.key'

        trainfile.to_csv(trainfileName, sep = ',', header = False, index = False)
        devfile.to_csv(devfileName, sep = ',', header = False, index = False)
        testfile.to_csv(testfileName, sep = ',', header = False, index = False)

        print str(trainfile.shape[0]) + ' lines written to ' + os.path.join(os.getcwd(), trainfileName)
        print str(devfile.shape[0]) + ' lines written to ' + os.path.join(os.getcwd(), devfileName)
        print str(testfile.shape[0]) + ' lines written to ' + os.path.join(os.getcwd(), testfileName)


def doMostCommonClassify(keyfile,outfilename):
    '''
    guess all label to be 1, 2,3
    Most Common Baseline
    '''
    with open(outfilename, 'w') as f:
        for inst in utils.DataIterator(keyfile):
            f.write('3\n')

def doWordListClassify(keyfile, outfilename, sue_weight = 0.0, k = 0):
    '''
    count pos/neg words to make classification
    '''

    dictionary = utils.getSentVoc()
    pos = dictionary['pos']
    neg = dictionary['neg']

    with open(outfilename, 'w') as f:
        for item in utils.DataIterator(keyfile):
            BOW = item['BOW']
            SUE = item['SUE']
            poscount = 0
            negcount = 0
            for word, count in BOW.items():
                if word in pos:
                    poscount += count
                elif word in neg:
                    negcount += count
            score = float((poscount - negcount)) + sue_weight * SUE
            #score = float(poscount - negcount)
            if score > k:
                f.write('1\n')
            elif score < -k:
                f.write('3\n')
            else:
                f.write('2\n')


def getAllCounts(dataiterator):
    allcounts = defaultdict(int)
    for result in dataiterator:
        for word,count in result['BOW'].items():
            allcounts[word] += count
    return allcounts

import operator
class DictionaryGenerator():
    '''
    Generate a dictionary from the files indicated in the keyfile
    '''

    def __init__(self):
        self.allcounts = defaultdict(int)

    def GenerateDictionary(self,keyfile,outdictname):
        '''
        Args:
            keyfile(str): path to look for the keyfile
            outdictname: name of the output dictionary
        '''
        for point in utils.DataIterator(keyfile):
            for word,count in point['BOW'].items():
                self.allcounts[word] += count
        totalWordNum = sum(self.allcounts.values())
        wordfilter = float(totalWordNum) * 0.000001
        filterVocabulary = {key:value for key,value in self.allcounts.items() if (value > wordfilter and len(key) > 3)}
        sortedVocabulary = sorted(filterVocabulary.iteritems(),key = operator.itemgetter(1) )
        with open(outdictname,'w') as f:
            for item in sortedVocabulary:
                word,count = item
                f.write(word + ':' + str(count) + '\n')


