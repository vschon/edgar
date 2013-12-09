# Demo Edgar Project
# Rucheng Yang 2013

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
import form10DB


DATA_ADD = os.getenv('DATA')
translator = form10DB.CIKTranslator()



def updateCoreIndex():
    '''
    Core index file stores 10QK filing information of all ticker
    Update core index file based on the indexfile of each stock
    '''
    coreindexpath = DATA_ADD + '/Edgar/coreindex.txt'
    coreindex = pd.DataFrame(columns=['CIK','Ticker','Date','Type','Address'])
    CIKdict = translator.tickerGetter
    for cik in CIKdict:
        indexfilepath = DATA_ADD + '/Edgar/' + str(cik) + \
                '_' + str(CIKdict[cik]) + '/' + 'index.txt'
        if os.path.exists(indexfilepath):
            indexfile = pd.read_table(indexfilepath,sep = ',',
                                        dtype={'CIK':'S10',})
            indexfile['Date'] = indexfile['Date'].apply(parse)
            coreindex = pd.concat([indexfile,coreindex])
    coreindex = coreindex.drop_duplicates()
    coreindex = coreindex.sort('Date')
    coreindex.to_csv(coreindexpath,sep=',',header=True, index = False)



def checkEmpty(symbolList):
    '''
    return ticker list with empty index file

    Args:
        symbolList: list of symbols

    Return:
        result:{'NoSymbol': symbols not in the cik-ticker dict
                'NoIndex': symbol in the cik-ticker dict, but does not have index file}

    '''

    tickerdict = translator.cikGetter
    NotInTickerDict = []
    NoIndexFile = []
    for symbol in symbolList:
        if symbol not in tickerdict.keys():
            NotInTickerDict.append(symbol)
        else:
            cik = tickerdict[symbol]
            indexfilepath = DATA_ADD + '/Edgar/' + str(cik) + \
                '_' + str(symbol) + '/index.txt'
            if not os.path.exists(indexfilepath):
                NoIndexFile.append(symbol)
    result = {'NoSymbol':NotInTickerDict,
              'NoIndex':NoIndexFile}
    return result

def cleanPrice(price):
    '''
    check same ticker used by two companies
    '''
    #sectorPricePath = '/home/brandon/edgar/Database/' + sector + \
    #        '/' + sector + '_Price.csv'
    #price = pd.read_csv(sectorPricePath,sep = ',' ,parse_dates = ['date',])
    #get out all ticker

    #remove price with B,C
    price = price[(price['RET'] != 'C') & (price['RET'] != 'B')]
    ticker_set = set(price['TICKER'])
    for symbol in ticker_set:
        sub = price[price['TICKER'] == symbol]
        sub = sub.sort('date')
        names = sub['COMNAM'].unique()
        length = names.shape[0]
        if length > 1:
            name = names[length-1]
            beginDate = sub[sub['COMNAM'] == name]['date'].irow(0)
            endDate = sub[sub['COMNAM'] == name]['date'].irow(-1)

            for i in range(length-2,-1,-1):
                name = names[i]
                endDate = sub[sub['COMNAM'] == name]['date'].irow(-1)
                delta = beginDate - endDate
                if delta.days > 7 or delta.days < 0:
                    print '\n\n***for symbol: ' + str(symbol)  + '****'
                    print name
                    print beginDate
                    print endDate
                beginDate = sub[sub['COMNAM'] == name]['date'].irow(0)

def loadData(sector):
    '''
    read the pricce, portfolio and sue data into memory
    '''

    id2TickerTable = utils.id2Ticker()['GetTicker']

    def mapID2Ticker(id):
        if id in id2TickerTable.keys():
            return id2TickerTable[id]
        else:
            return '*'

    sectorPricePath = '/home/brandon/edgar/Database/' + sector + \
            '/' + sector + '_Price.csv'
    portfolioRetPath = '/home/brandon/edgar/Database/' + sector + \
            '/' + sector + 'Portfolio.txt'
    suePath = '/home/brandon/edgar/Database/' + sector + \
            '/' + sector + 'SUE.txt'

    #ipdb.set_trace()
    price = pd.read_csv(sectorPricePath,sep = ',' ,parse_dates = ['date',])
    #price.index = price['date']
    price['date'] = pd.to_datetime(price['date'])
    price = price[(price['RET'] != 'C') & (price['RET'] != 'B')]

    '''
    #align price with changed symbols
    symbolset = set(price['TICKER'])
    symbolset.discard(np.nan)
    for symbol in symbolset:
        #ipdb.set_trace()
        sub = price[price['TICKER'] == symbol]
        COMNAM = sub['COMNAM'].irow(0)
        PERMNO = sub['PERMNO'].irow(0)
        price['TICKER'][price['COMNAM'] == COMNAM] = symbol
        price['TICKER'][price['PERMNO'] == PERMNO] = symbol
    '''


    portfolio = pd.read_csv(portfolioRetPath, sep= '\t', parse_dates = ['date',])
    portfolio.index = portfolio['date']
    portfolio['date'] = pd.to_datetime(portfolio['date'])

    sue = pd.read_csv(suePath, sep = '\t', parse_dates = ['Date',])
    sue['symbol'] = sue['companyid'].apply(mapID2Ticker)

    return price,portfolio,sue



def GenerateDataset(sector,data,begin,end):
    '''
    Generate data set
    Args:
        sector(str): currenyly only 'Industrials' available
        data: data returned by loadData
        begin: begin index of the symbol list
        end:    end index of the symbol list
    '''

    #read in sector symbols
    symbolfilepath = '/home/brandon/edgar/Database/' + sector + \
    '/' + sector + '_Symbol_List.csv'
    with open(symbolfilepath,'rb') as f:
        symbolList = f.read().split('\n')
        symbolList.remove('')

    symbolList = symbolList[begin:end]

    #symbolList =['PCO']


    checkResults = checkEmpty(symbolList)
    if len(checkResults['NoSymbol']) is not 0 or len(checkResults['NoIndex']) is not 0:
        print checkResults
        return -1
    else:
        print 'Edgar data is in position\n'

    #load price, portfolio and sue
    #price, portfolio, sue = loadData(sector)
    price,portfolio,sue = data

    Ticker2CIKDict = va.datamanage.edgar.getTickerdict()
    for ticker in symbolList:
        print 'begin processing ticker: ' + ticker + '\n'
        cik = Ticker2CIKDict[ticker]
        indexfilepath = DATA_ADD + '/Edgar/' + str(cik) + \
               '_' + str(ticker) + '/' + 'index.txt'
        indexfile = pd.read_table(indexfilepath, sep = ',', dtype = {'CIK':'S10',})
        indexfile['Date'] = indexfile['Date'].apply(parse)

        #filter files - remove 10-A, remove duplicates
        #ipdb.set_trace()
        indexfile = indexfile[((indexfile['Type'] == '10-K') | (indexfile['Type'] == '10-Q')) &
                              (indexfile['Date'] >= dt.datetime(2000,1,1)) &
                              (indexfile['Date'] <= dt.datetime(2013,6,28))]
        indexfile = indexfile.drop_duplicates(cols = 'Date')

        #build directoy
        tickerDir = '/home/brandon/edgar/Database/' + sector + '/' + ticker
        if not os.path.exists(tickerDir):
            os.makedirs(tickerDir)

        #ipdb.set_trace()
        #get query key index
        sub = price[price['TICKER'] == ticker]
        sub = sub.sort('date')
        COMNAM = sub['COMNAM'].irow(-1)
        PERMNO = sub['PERMNO'].irow(-1)
        queryPool = price[(price['TICKER'] == ticker) | (price['PERMNO'] == PERMNO) | (price['COMNAM'] == COMNAM)]
        queryPool = queryPool.sort('date')

        #generate Feature file
        for index,row in indexfile.iterrows():
            filedate = row['Date']
            #ipdb.set_trace()
            window = queryPool[queryPool['date'] >= filedate].head(2)
            windowBegin = window['date'].irow(0)
            windowEnd = window['date'].irow(-1)

            delay = windowBegin - filedate
            if delay.days > 7:
                #delaylogpath = '/home/brandon/edgar/Database/' + sector + '/delay.log'
                #with open(delaylogpath,'a') as f:
                #    f.write(ticker + ' ' + filedate.strftime('%Y%m%d') + " " + windowBegin.strftime('%Y%m%d') + '\n')
                continue

            windowReturn = np.cumprod(window['RET'].values.astype(float) + 1)[-1]
            portWindow = portfolio[windowBegin:windowEnd]
            windowPortReturn = np.cumprod(portWindow['RET'].values.astype(float) + 1)[-1]
            windowExcessReturn = windowReturn - windowPortReturn
            if windowExcessReturn > 0.003:
                label = 1
            elif windowExcessReturn < -0.003:
                label = 3
            else:
                label = 2

            try:
                SUE = sue[(sue['Date'] == filedate.strftime('%Y%m')) & (sue['symbol'] == ticker)]
                SUE = SUE['IBES_Standardized_Unexpected_Ear'].values[0]
                if np.isnan(SUE):
                    SUE = 0.0
            except IndexError:
                SUE = 0.0

            formdate = row['Date']
            formtype = row['Type']
            formtype = formtype.replace('/','-')
            localfile = DATA_ADD + '/Edgar/' + str(cik) + \
                    '_' + str(ticker) + '/' + str(cik) + '_' + \
                    str(ticker) + '_' + formdate.strftime('%Y%m%d') + \
                    '_' + formtype + '.txt.gz'
            f_in = gzip.open(localfile,'rb')
            raw = f_in.read()
            f_in.close()
            BOW = defaultdict(int)
            BOW = va.datamanage.edgar.GenerateBOW(raw)



            filepath = tickerDir + '/' + ticker + '_' + filedate.strftime("%Y%m%d") + '.txt'
            with open(filepath, 'wb') as f:
                f.write('Symbol: ' + ticker + '\n')
                f.write('filedate: ' + filedate.strftime('%Y%m%d') + '\n')
                f.write('windowBegin: ' + windowBegin.strftime('%Y%m%d') + '\n')
                f.write('windowEnd: ' + windowEnd.strftime('%Y%m%d') + '\n')
                f.write('windowReturn: ' + str(windowReturn) + '\n')
                f.write('windowPortReturn: ' + str(windowPortReturn) + '\n')
                f.write('windowExcessReturn: ' + str(windowExcessReturn) + '\n')
                f.write('SUE: ' + str(SUE) + '\n')
                f.write('Label: ' + str(label) + '\n')
                f.write('BOWBEGIN\n')
                for word, count in BOW.items():
                    f.write("{}:{}\n".format(word,count))
                f.write('BOWEND\n')
            print 'Complete writing to ' + filepath + '\n'


def DatabaseIndex(sector):
    '''
    generate a key file indexing all data points for a sector
    file format:
        date    symbol  label   path
    TODO:
        reset path
    '''

    symbolfilepath = '/home/brandon/edgar/Database/' + sector + \
            '/' + sector + '_Symbol_List.csv'
    with open(symbolfilepath,'rb') as f:
        symbolList = f.read().split('\n')
        symbolList.remove('')

    totalIndex = []
    for symbol in symbolList:
        tickerDir = '/home/brandon/edgar/Database/' + sector + '/' + symbol
        if os.path.exists(tickerDir):
            for f in os.listdir(tickerDir):
                name, date = f.rstrip('.txt').split('_')
                date = parse(date)
                fpath = os.path.join(tickerDir,f)
                label = 0
                with open(fpath,'r') as datafile:
                    for line in datafile:
                        if 'Label: ' in line:
                            label = int(line[7])
                            break
                entry = [date, symbol, label, fpath]
                totalIndex.append(entry)
    totalIndex = sorted(totalIndex, key = lambda x:x[0])
    indexPath = '/home/brandon/edgar/Database/' + sector + '/' + sector + '.key'
    with open(indexPath,'wb') as f:
        for line in totalIndex:
           f.write(line[0].strftime('%Y%m%d')+','+line[1] + ',' + str(line[2]) + ',' + line[3] + '\n')
    #return totalIndex


