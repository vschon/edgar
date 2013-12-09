"""
edgar.form10DB includes functions to download 10 forms from dataset.

It includes ticker to cik transformation, quarter master file download, and 10 form download functions.
"""

#Author: Brandon,Rucheng Yang

import pandas as pd
import os
import gzip
from collections import defaultdict
from dateutil.parser import parse
import datetime as dt
from ftplib import FTP
import urllib2
import ipdb
import re


DATA_ADD = os.getenv('DATA')

###CIK-Ticker Translator####

class CIKTranslator(object):
    '''
    CIKTranslator manages the mapping between cik and ticker

    Users can get the dictionary from two members:
        CIKtranslator.tickerGetter:   dictionary with cik:ticker

        CIKtranslator.cikGetter:      dictionary with ticker:cik
    '''
    def __init__(self):
        self.dictpath = DATA_ADD + '/Edgar/cik2ticker.txt'
        try:

            self.tickerGetter = self.__getCIKdict()
            self.cikGetter = self.__getTickerdict()
        except:
            self.tickerGetter = None
            self.cikGetter = None

    def __tickerCIKMapper(self,ticker):

        '''
        Given a ticker, return corresponding CIK code
        CIK is 10 digit number

        Args:
            ticker(str)

        Return:
            cik(str): the cik code

        '''

        string_match = 'rel="alternate"'
        url = 'http://www.sec.gov/cgi-bin/browse-edgar?company=&match=&CIK=%s&owner=exclude&Find=Find+Companies&action=getcompany' % ticker
        response = urllib2.urlopen(url)
        cik = ''
        for line in response:
            if string_match in line:
                for element in line.split(';'):
                    if 'CIK' in element:
                        cik = element.replace('&amp', '')
        cik = cik[4:]
        return cik

    def __getCIKdict(self):
        '''
        return the dictionary of CIK-ticker
        In the dictionary, you can input a cik code, and the dictionary
        will return the ticker name
        '''

        CIKtable = defaultdict(str)
        with open(self.dictpath, 'rb') as f:
            for line in f.readlines():
                line=line.rstrip('\n')
                ticker, CIK = line.split(',')
                if CIK != '':
                    CIKtable[CIK] = ticker
        return CIKtable

    def __getTickerdict(self):
        '''
        return the dictionary of ticker-CIK
        You can input a ticker, and the dict will return the cik code
        '''

        cikdict = self.__getCIKdict()
        tickerdict = defaultdict(str)
        for key, val in cikdict.items():
            tickerdict[val] = key
        return tickerdict

    def updateCIKtable(self,tickers):
        '''
        generate a txt file that map ticker to cik

        Args:
            sequence of symbol(str): symbols to add to the dictionary

        Return:
            emptycik(list): list of ticker without cik

        Output:
            DATA_ADD + '/Edgar/cik2ticker.txt
        '''
        emptycik = []

        tablepath = DATA_ADD + '/Edgar/cik2ticker.txt'
        if os.path.exists(tablepath):
            self.cikGetter = self.__getTickerdict()
            for ticker in tickers:
                cik = self.__tickerCIKMapper(ticker)
                if cik != '':
                    self.cikGetter[ticker] = cik
                else:
                    emptycik.append(ticker)

        with open(tablepath, 'wb') as fout:
            for ticker,cik in self.cikGetter.items():
                line = ticker + ',' + cik + '\n'
                fout.write(line)
        self.tickerGetter = self.__getCIKdict()

####Master file management####
class Form10Manager(object):
    '''
    MasterFileManager download and maintains quarterly master file of
    EDGAR database
    '''

    def __init__(self):
        self.translator = CIKTranslator()

    def updateQuarterMasterFile(self,beginyear, beginqtr, endyear, endqtr):
        '''Download quarterly master file from edgar database
        Args:
            beginyear, ..., endqtr (int): specify required date range
        '''
        print 'login to edgar ftp'
        ftp = FTP('ftp.sec.gov')
        print ftp.login()
        for year in range(beginyear, (endyear + 1)):
            if year == endyear:
                maxqtr = endqtr + 1
            else:
                maxqtr = 5
            if year == beginyear:
                minqtr = beginqtr
            else:
                minqtr = 1
            for qtr in range(minqtr, maxqtr):
                remotefile = 'RETR /edgar/full-index/' + str(year) + \
                        '/QTR' + str(qtr) + '/master.gz'
                localfile = DATA_ADD + '/Edgar/MasterFile/' + \
                        str(year) + 'QTR' + str(qtr) + 'master.gz'
                ftp.retrbinary(remotefile, open(localfile, 'wb').write)
                print 'download '+ str(year) + 'QTR' + str(qtr) + \
                        'master.gz'
        ftp.quit()

    def __scanQuaterMasterFile(self,year,qtr):
        '''
        Extract 10Q/K addresss information contained in the quarterly masterfile.
        Return as a dataframe

        Args:
            year(int)
            qtr(int)

        Return:
            forms(DataFrame):
                CIK: cik code
                Ticker
                Date: filing date
                Type: form type 10-Q/10-K,..
                Address: url for download
        '''

        masterfilepath = DATA_ADD + '/Edgar/MasterFile/' + \
                str(year) + 'QTR' + str(qtr) + 'master.gz'
        forms = pd.DataFrame(columns=['CIK','Ticker','Date','Type','Address'])
        f = gzip.open(masterfilepath, 'rb')
        for line in f.readlines():
            line = line.rstrip('\n')
            if line[-4:] == '.txt':
                cik, name, formtype, formdate, address = line.split('|')
                if re.search('^10.*$', formtype):
                    if cik.zfill(10) in self.translator.tickerGetter.keys():
                        pdline=pd.DataFrame(
                            {'CIK':[cik.zfill(10)],
                            'Ticker':[self.translator.tickerGetter[cik.zfill(10)]],
                            'Date':[parse(formdate)],
                            'Type':[formtype],
                            'Address':[address]},
                            columns=['CIK','Ticker','Date','Type','Address'])
                        forms = pd.concat([forms,pdline])
        f.close()
        return forms

    def __downloader(self,_ftp,remotefile,localfile):
        '''Download 10QK form  from edgar

        Args:
            ftp:  FTP instance
            remotefile(str): url of the target form
            localfile(str): local address to store the form

        Return:

        '''
        ftp = _ftp
        remotefile = 'RETR /'+ remotefile
        if not os.path.exists(localfile + '.gz'):
            #ipdb.set_trace()
            ftp.retrbinary(remotefile, open(localfile, 'wb').write)
            f_in = open(localfile,'rb')
            f_out = gzip.open(localfile+'.gz','wb')
            f_out.writelines(f_in)
            f_out.close()
            f_in.close()
            os.remove(localfile)
            print dt.datetime.now(),': ',localfile + ' downloaded'
            return 1
        else:
            print localfile + '.gz already exists'
            return 0

    def updateForms(self, year, qtr, range = 'all'):
        '''
        download all 10 forms listed in the master file to corresponding ticker directory

        Args:
            year,qtr(int):  year and quarter of master file
            range(str):     specify date range to download
                            'all': download all forms in the master file
                            'today': download forms with date == today
        return:

        '''

        ftp = FTP('ftp.sec.gov')
        ftp.login()
        masterfile = self.__scanQuaterMasterFile(year,qtr)
        ipdb.set_trace()
        if range == 'today':
            today = dt.date.today()
            today = dt.datetime(today.year,today.month,today.day)
            masterfile = masterfile[masterfile['Date'] == today]
        elif range == 'all':
            pass
        else:
            print 'range Error'
            return -1

        updatefiles = []

        for index,row in masterfile.iterrows():
            ticker = row['Ticker']
            cik = row['CIK']
            formtype = row['Type']
            formtype = formtype.replace('/','-')
            formdate = row['Date']

            isTickerLegal = False
            try:
                self.translator.cikGetter[ticker]
                isTickerLegal = True
            except KeyError:
                pass

            if isTickerLegal:
                localfile = DATA_ADD + '/Edgar/' + str(cik) + \
                        '_' + str(ticker) + '/' + str(cik) + '_' + \
                        str(ticker) + '_' + formdate.strftime('%Y%m%d') + \
                        '_' + formtype + '.txt'
                remotefile = row['Address']
                downloadFlag = self.__downloader(ftp,remotefile,localfile)
                if downloadFlag == 1:
                    updatefiles.append(row)
            else:
                print ticker + 'not in ticker-cik table, pass'
        return updatefiles

    def updateAndDownload(self,year,qtr,range = 'all'):
        '''
        update master file and download new forms
        '''

        self.updateQuarterMasterFile(year,qtr,year,qtr)
        self.updateForms(year,qtr,range)

    def buildIndex(self):
        '''
        build index file for each ticker directory
        '''
        pass

    def listQuraterMasterFile(self):
        '''
        List all available quarterly master file
        '''
        #ipdb.set_trace()
        filepath = DATA_ADD + '/Edgar/MasterFile'
        masterList = os.listdir(filepath)
        masterList = sorted(masterList)
        #masterList=sorted(os.listdir(filepath))
        prevyear = ''
        for masterfile in masterList:
            currentyear = masterfile[0:4]
            currentquarter = masterfile[4:8]
            if currentyear != prevyear:
                print '\n' + currentyear + '  ' + currentquarter,
            else:
                print '  ' + currentquarter,
            prevyear = currentyear

