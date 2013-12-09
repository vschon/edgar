'''
This file contains useful supporting functions
'''
import pandas as pd
import os
import gzip
from collections import defaultdict
from dateutil.parser import parse
import datetime as dt
import nltk
from ftplib import FTP
import urllib2
import ipdb
import re


def DataIterator(keyfile):
    '''
    make a generator which iteraively read a key file and returns the data point

    Args:
        keyfile(str):path to the keyfile

    Return:
        results(dict): the data point to be returned
    '''
    indexfile = pd.read_csv(keyfile,sep = ',', names = ['date','symbol', 'label', 'path'], parse_dates = ['date',])
    for index,row in indexfile.iterrows():
        label = row['label']
        with open(row['path'],'rb') as f:
            inBOWsection = False
            sue = 0.0
            fcounts = defaultdict(int)
            for line in f:
                if inBOWsection == False and 'SUE: ' in line:
                    sue = float(line[5:].rstrip('\n'))
                if inBOWsection == False and 'BOWBEGIN' in line:
                    inBOWsection = True
                    continue
                if inBOWsection == True and 'BOWEND' not in line:
                    word,count = line.rstrip('\n').split(':')
                    fcounts[word] = int(count)
        results = {'label':label, 'SUE':sue, 'BOW':fcounts}
        yield results


def Iterator2Memory(keyfile):
    '''
    read the key file into memeroy as a list
    '''

    result = [item for item in DataIterator(keyfile)]
    return result


def getLMDictionary(full):

    '''
    return all the words contained in Loungran McDonald Financial dictionary
    Args:
        full(bool): if true, return all words in a set
                    if false, return positive and negative words separately
    TODO: reset dictionary path
    '''

    pospath = '/home/brandon/edgar/Database/LM-Dictionary/Positive.csv'
    negpath = '/home/brandon/edgar/Database/LM-Dictionary/Negative.csv'
    uncerpath = '/home/brandon/edgar/Database/LM-Dictionary/Uncertainty.csv'
    litipath = '/home/brandon/edgar/Database/LM-Dictionary/Litigious.csv'

    def process(words):
        words = words.split('\r\n')
        words.remove('')
        words = [word.lower() for word in words]
        lemmatizer = nltk.WordNetLemmatizer()
        words = [lemmatizer.lemmatize(word) for word in words]
        return words

    with open(pospath,'r') as f:
        pos = f.read()
        pos = set(process(pos))
    with open(negpath,'r') as f:
        neg = f.read()
        neg = set(process(neg))
    with open(uncerpath,'r') as f:
        uncer = f.read()
        uncer = set(process(uncer))
    with open(litipath,'r') as f:
        liti = f.read()
        liti = set(process(liti))

    if full == True:
        return (pos | neg | uncer | liti)
    else:
        return {'pos':pos,'neg':neg}

def getSentVoc():

    '''
    Return positive and negative words in sentiment dictionary
    TODO:
        Reset sentpath
    '''

    sentpath = '/home/brandon/edgar/Database/sentiment-vocab.tff'
    poswords = set()
    negwords = set()
    with open(sentpath,'r') as f:
        for line in f:
            stuff = dict()
            for kvpair in line.rstrip().split(' '):
                name,var = kvpair.partition('=')[::2]
                stuff[name]=var
            if stuff['type'] == 'strongsubj':
                if stuff['priorpolarity'] == 'negative':
                    negwords.add(stuff['word1'])
                if stuff['priorpolarity'] == 'positive':
                    poswords.add(stuff['word1'])
    return {'pos':poswords,'neg':negwords}

def GenerateBOW(raw):
    '''
    generate bow from raw input

    Args:
        raw(str): raw html input

    Returns:
        fcount(defaultdict(int)): {word:count}
    '''

    raw = nltk.clean_html(raw)
    regexp = r'[a-zA-Z]+'
    words = re.findall(regexp, raw)
    words = [word.lower() for word in words]
    words = [word for word in words if not word in nltk.corpus.stopwords.words('english')]
    words = [word for word in words if len(word) > 2]
    words = [word for word in words if len(word) < 20]
    lemmatizer = nltk.WordNetLemmatizer()
    words = [lemmatizer.lemmatize(word ) for word in words]
    fcounts  = defaultdict(int)
    for word in words:
        fcounts[word] += 1
    return fcounts


def id2Ticker():
    dictpath = '/home/brandon/edgar/Database/AllCap_Ticker.txt'
    id2TickerTable = dict()
    ticker2IDTable = dict()
    dictfile = pd.read_csv(dictpath, sep = '\t')

    def reduceUS(tickerUS):
        try:
            ticker,us = tickerUS.split(' ')
            return ticker
        except:
            return tickerUS
    dictfile['ticker'] = dictfile['ticker'].apply(reduceUS)

    for index, row in dictfile.iterrows():
        id2TickerTable[row['company_id']] = row['ticker']
        ticker2IDTable[row['ticker']] = row['company_id']

    return {'GetID':ticker2IDTable, 'GetTicker':id2TickerTable}

def cusip2Ticker():
    '''
    transfer cusip to ticker
    '''
    #ipdb.set_trace()
    cusipTable = dict()
    dictPath = '/home/brandon/edgar/Database/Industrials/Industrials_cusip2ticker_short.csv'
    dictFile = pd.read_csv(dictPath, sep = ';')
    for index,row in dictFile.iterrows():
        cusipTable[row['CUSIP']] = row['TICKER_SYMBOL']
    return cusipTable

###scorer####

def getConfusion(keyfilename, responsefilename):
    '''
    Generate Confusion matrix
    Args:
        keyfilename(str): path of file containing all true labels
        responsefilename(str): path of file containing all estimate labels
    Return:
        counts(defaultdict)
    '''

    counts = defaultdict(int)
    indexfile = pd.read_csv(keyfilename,sep = ',', names = ['date','symbol', 'label', 'path'], parse_dates = ['date',])
    with open(responsefilename,'r') as f:
        for index,row in indexfile.iterrows():
            trueLabel = str(row['label'])
            estimateLabel = f.readline().rstrip()
            counts[(trueLabel,estimateLabel)] += 1
    return counts

def accuracy(counts):
    '''
    Args:
        count(defaultdict): dict returned by getConfusion matrix

    '''
    return sum([y for x,y in counts.items() if x[0] == x[1]]) / float(sum(counts.values()))

def printScoreMessage(counts):
    true_pos = 0
    total = 0

    keyclasses = set([x[0] for x in counts.keys()])
    resclasses = set([x[1] for x in counts.keys()])
    print "%d classes in key: %s" % (len(keyclasses),keyclasses)
    print "%d classes in response: %s" % (len(resclasses),resclasses)
    print "confusion matrix"
    print "key\\response:\t"+"\t".join(resclasses)
    for i,keyclass in enumerate(keyclasses):
        print keyclass+"\t\t",
        for j,resclass in enumerate(resclasses):
            c = counts[tuple((keyclass,resclass))]
            #countarr[i,j] = c
            print "{}\t".format(c),
            total += float(c)
            if resclass==keyclass:
                true_pos += float(c)
        print ""
    print "----------------"
    print "accuracy: %.4f = %d/%d\n" % (true_pos / total, true_pos,total)


def analyzeResults(keyfilename, responsefilename):
    '''
    print the accuracy of estimation
    Args:
        keyfilename(str): path of file containing all true labels
        responsefilename(str): path of file containing all estimate labels

    '''
    printScoreMessage(getConfusion(keyfilename, responsefilename))


def saveResults(testLabel, responsefilename, outfilename):

    counts = defaultdict(int)
    response = pd.read_csv(responsefilename,header = None, names = ['label'])
    #ipdb.set_trace()
    for index,row in response.iterrows():
        trueLabel = str(int(testLabel[index]))
        estimateLabel = str(int(row['label']))
        counts[(trueLabel,estimateLabel)] += 1
    #ipdb.set_trace()
    acc = accuracy(counts)
    with open(outfilename,'a') as f:
        f.write(str(acc) + '\n')


