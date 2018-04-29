# !usr/bin/python
# -*- coding: utf-8 -*-

import codecs
import datetime
import itertools
import os
import platform
import sys
import zipfile

import matplotlib.pyplot as plt


import numpy as np
import pandas as pd
from pandas import DataFrame

import tensorflow as tf
from sklearn import preprocessing
from sklearn.cluster import KMeans
from tensorflow.python.platform import gfile

from six.moves import urllib

class PathListContes(object):
   ''' 
    # ================================================================
    #  COT report address
    # ================================================================
    # Address: 
    # 
    # ================================================================
    # 
    # ================================================================
    # 
    # ================================================================
    # WeeklyUpdates. CFTC data release at each US time friday???
    # http://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm'
    # r'https://www.cftc.gov/files/dea/history/deahistfo2018.zip'
    # ================================================================
    # 
    # ================================================================
    # Main program.
    # http://115.29.204.48/zqgz/YYYYMMDDbond_valuation.zip
    # China Index(stkblocks,codes)
    # http://115.29.204.48/syl/YYYYMMDD.zip
    # A ��ȫ�г���ҵ��ӯ�ʣ���֤��ҵ���ࣩ
    # http://115.29.204.48/syl/csiYYYYMMDD.zip
    # A ���г���Ҫ������ӯ��
    # http://115.29.204.48/syl/bkYYYYMMDD.zip
    # ��ҳ -AMAC ��ҵָ��
    # ���� AMAC ������ֵ��ҵ����ָ����ʷ����
    # http://115.29.204.48/amac/csrcperf.zip
    # ���� AMAC ������ֵ��ҵ����ָ������Ȩ��
    # http://115.29.204.48/amac/csrccwf.zip
    # ���� AMAC ������ֵ��ҵ����ָ���ɷݹ�����
    # http://115.29.204.48/amac/csrccons.zip
    # ����֤������ҵ����
    # http://115.29.204.48/amac/csrcindustry.zip
    '''
        # pass

class PathSets():
    def init(self):
        plf=sys.platform
        WorkEnviro = plf
        
        if WorkEnviro == 'winpc01':
            # Platform is Windows , Dell PC01
            # Store the CFTC Weekly released .zip files
            self.SSEDaily_storefolder = r'C:/WorkBench/Investment/SSESYL/SYLCNIndexZIP/'
            # Store the SSE downloads .xls files
            self.SSEDaily_work_directory = r'C:/WorkBench/Investment/SSESYL/SYLCNIndexXLS/'
            # Store the Daily RZRQ XLS files
            self.RZRQDaily_store_folder = r'C:/WorkBench/Investment/RZRQ/DailyXLS/'
            # Transfer the RZRQ .XLS to .TXT
            self.RZRQDaily_work_directory = r'C:/WorkBench/Investment/RZRQ/DailyTXT/'
            # additional indicator during the transfer way
            self.RZRQDaily_AddIndicators_directory=r'C:/WorkBench/Investment/RZRQ/DailyTXTAddInd/'
            # Store the Time series data after transfered
            self.RZRQDaily_TS_directory = r'C:/WorkBench/Investment/RZRQ/DailyTSdata/'
            # the TOPX,PreXdays ACC stock and its behaviear
            self.RZRQDaily_TOPRank=r'C:/WorkBench/Investment/RZRQ/DailyTSdata/TOPXRank/'
            # Where the Cluster Stock Codes folders
            self.RZRQDailyCluster_work_directory = r'C:/WorkBench/Investment/RZRQ/DailyClusters'
            # Store the COT weekly .zip files
            self.COTWeekly_storefolder = r'C:/WorkBench/Investment/CFTC/WeeklyUpdatedZip/'
            # store the CFTC weekly files after transfer to .TXT files
            self.COTWeekly_ExtractFolder = r'C:/WorkBench/Investment/CFTC/WeeklyUpdatedTXT/'
            # store the CFTC Each codes Time series data
            self.COTWeekly_TimesSeries=r'C:/WorkBench/Investment/CFTC/WeeklyUpdatedTimeSeries/'
            # store the future or option's COT indicator's time series data
            self.COTWeekly_TimesSeriesdir=r'C:/WorkBench/Investment/CFTC/WeeklyUpdatedTSindicators/'

        if WorkEnviro == 'win32':
            # Platform is Windows , Dell PC01
            # Store the CFTC Weekly released .zip files
            self.SSEDaily_storefolder = r'G:/TimeSeriesData/SSESYL/SYLCNIndexZIP/'
            # Store the SSE downloads .xls files
            self.SSEDaily_work_directory = r'G:/TimeSeriesData/SSESYL/SYLCNIndexXLS/'
            # Store the Daily RZRQ XLS files
            self.RZRQDaily_store_folder = r'G:/TimeSeriesData/RZRQ/DailyXLS/'
            # Transfer the RZRQ .XLS to .TXT
            self.RZRQDaily_work_directory = r'G:/TimeSeriesData/RZRQ/PYXLS/'
            # Store the Time series data after transfered
            self.RZRQDaily_TS_directory = r'G:/TimeSeriesData/RZRQ/DailyTSdata/'
            # the TOPX,PreXdays ACC stock and its behaviear
            self.RZRQDaily_TOPRank=r'G:/TimeSeriesData/RZRQ/TOPXRank/'
            # Where the Cluster Stock Codes folders
            self.RZRQDailyCluster_work_directory = r'G:/TimeSeriesData/RZRQ/DailyClusters'

            # Store the COT weekly .zip files
            self.COTWeekly_storefolder = r'G:/TimeSeriesData/CFTC/WeeklyUpdatedZip/'
            # store the CFTC weekly files after transfer to .TXT files
            self.COTWeekly_ExtractFolder = r'G:/TimeSeriesData/CFTC/CFTCWeeklyTXT/'
            # store the CFTC Each codes Time series data
            self.COTWeekly_TimesSeries=r'G:/TimeSeriesData/CFTC/WeeklyUpdatedTimeSeries/'
            # store the future or option's COT indicator's time series data
            self.COTWeekly_TimesSeriesdir=r'G:/TimeSeriesData/CFTC/WeeklyUpdatedTSindicators/'

        if WorkEnviro == 'darwin':
            #Platform is Apple, mac
            # the TOPX,PreXdays ACC stock and its behaviear
            self.RZRQDaily_TOPRank=r'/Users/mac/Desktop/Projects/COT_Report/TOPXRank/'
            # Store the CFTC Weekly released .zip files
            self.SSEDaily_storefolder = r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/SSE/ChinaindexZIP/'
            # Store the SSE downloads .xls files
            self.SSEDaily_work_directory = r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/SSE/ChinaindexXLS/'
            # Store the Daily RZRQ XLS files
            self.RZRQDaily_store_folder = r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/RZRQ/RZRQDailyXLS/'
            # Transfer the RZRQ .XLS to .TXT
            self.RZRQDaily_work_directory = r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/RZRQ/RZRQDailyTXT/'
            # Store the Time series data after transfered
            self.RZRQDaily_TS_directory = r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/RZRQ/RZRQTimeseries/'
            # Where the Cluster Stock Codes folders
            self.RZRQDailyCluster_work_directory = r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/RZRQ/KmeansPY/'
           # the TOPX,PreXdays ACC stock and its behaviear
            self.RZRQDaily_TOPRank=r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/RZRQ/TOPXRank/'
            # Store the COT weekly .zip files
            self.COTWeekly_storefolder = r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/COT/WeeklyUpdatedZIP/'
            # store the CFTC weekly files after transfer to .TXT files
            self.COTWeekly_ExtractFolder = r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/COT/WeeklyUpdatedTXT/'
            # store the CFTC Each codes Time series data
            self.COTWeekly_TimesSeries=r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/COT/WeeklyUpdatedTimeSeries/'
            # store the future or option's COT indicator's time series data
            self.COTWeekly_TimesSeriesdir=r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/COT/WeeklyUpdatedTSindicators/'

        self.Maybecreatedir()

    def GetCOTStoredir(self):
        return self.COTWeekly_storefolder
    def GetCOTWorkdir(self):
        return self.COTWeekly_ExtractFolder
    def GetSSEStoredir(self):
        return self.SSEDaily_storefolder
    def GetSSEWorkdir(self):
        return self.SSEDaily_work_directory
    def GetRZRQStoreDir(self):
        return self.RZRQDaily_store_folder
    def GetRZRQWorkdir(self):
        return self.RZRQDaily_work_directory
    def GetRZRQCLusterDir(self):
        return self.RZRQDailyCluster_work_directory
    def GetRZRQTSdir(self):
        return self.RZRQDaily_TS_directory
    def GetCOTTimeSeriesdir(self):
        return self.COTWeekly_TimesSeries
    def GetCOTTSIndicatorsdir(self):
        return self.COTWeekly_TimesSeriesdir  
    def GetRZRQTOPRankWorkdir(self):
        return self.GetRZRQTOPRankWorkdir 
    def GetRZRQAddindicatorsDailyDir(self):
        return self.RZRQDaily_AddIndicators_directory
        
    def Maybecreatedir(self):

        try:
            os.mkdir(self.GetRZRQAddindicatorsDailyDir())
        except Exception:
            pass

        try:
            os.mkdir(self.GetCOTStoredir())
        except Exception:
            pass
        try:
            os.mkdir(self.GetCOTWorkdir())
        except Exception:
            pass
        try:
            os.mkdir(self.GetRZRQCLusterDir())
        except Exception:
            pass
        try:
            os.mkdir(self.GetCOTTimeSeriesdir())
        except Exception:
            pass
        try:
            os.mkdir(self.GetCOTTSIndicatorsdir())
        except Exception:
            pass           
        try:
            os.mkdir(self.GetRZRQStoreDir())
        except Exception:
            pass
        try:
            os.mkdir(self.GetRZRQTSdir())
        except Exception:
            pass
        try:
            os.mkdir(self.GetRZRQWorkdir())
        except Exception:
            pass
        try:
            os.mkdir(self.GetSSEStoredir())
        except Exception:
            pass
        try:
            os.mkdir(self.GetSSEWorkdir())
        except Exception:
            pass
        try:
            os.mkdir(self.GetRZRQTOPRankWorkdir())
        except Exception:
            pass

# ================================================================
# WeeklyUpdates. CFTC data release at each US time friday???
# http://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm'
# r'https://www.cftc.gov/files/dea/history/deahistfo2018.zip'
# ================================================================
def UpdatingCFTCWeeklyData(zipfolder, Extractfolder, StartYear, EndYear):
    print('*In Year2018 stage data no need to revised, if 2019+,please need to update the codes')
    print('*CFTC update their data weekly in 3.30pm Friday')
    Nowtime = datetime.datetime.now()
    # print(Nowtime)
    NowDay = datetime.datetime.strftime(Nowtime, "%A")
    print(NowDay)
    # if not (NowDay=='Friday'):
    if 1:
        print('Data to be updated in 3.30PM Friday')
        filename = ''
        work_directory = zipfolder
        storefolder = work_directory
        ExtractFolder = Extractfolder
        source_url = r'https://www.cftc.gov/files/dea/history/'
        now = datetime.datetime.now()

        for year in range(StartYear, EndYear+1):
            filename = 'deahistfo'+str(year)+'.zip'
            
            if not(os.path.exists(work_directory+filename)): 
                print('Try to download... the file : ' + filename )                  
                try:
                    maybe_download(filename, work_directory, source_url+filename)
                except Exception as e:
                    print(filename)
                    print(e)
                Fullpathname = storefolder+'deahistfo'+str(year)+'.zip'
                un_zip(Fullpathname, ExtractFolder)
                try:
                    os.remove(ExtractFolder+str(year)+'FutureAndOptions.txt')
                except Exception as e:
                    pass
                try:
                    os.rename(ExtractFolder+'annualof.txt',ExtractFolder+str(year)+'FutureAndOptions.txt')
                except Exception as e:
                    pass
            else:
                print( filename + ' already in, don\'t need to downloand')
    else:
        print('*Data from CFTC not updated yet!!!')

# ================================================================
# DailyUpdates. SSE RZRQ data???
# r'http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk20180404.xls'
# ================================================================
def UpdatingRZRQdataDailyData(storeXLSfolder, predays):
    print('Updating RZRQ............')
    filename = ''
    work_directory = storeXLSfolder
    source_url = r'http://www.sse.com.cn/market/dealingdata/overview/margin/a/'
    now = datetime.datetime.now()
    for days in range(predays):
        delta = datetime.timedelta(-1*days+4)
        n_days = now + delta
        DateStr = n_days.strftime("%Y-%m-%d").replace('-', '')
        filename = 'rzrqjygk'+DateStr+'.xls'
        try:
            maybe_download(filename, work_directory, source_url+filename)
        except Exception as e:
            print(filename)
            print(e)
            continue

# ================================================================
# Updating the China index data for each day from SSE
# http://115.29.204.48/syl/YYYYMMDD.zip
# ================================================================
def UpdatingSSEIndexData(zipfolder, PreDays):
    print('Updating china index............')
    filename = ''
    work_directory = zipfolder
    source_url = r'http://115.29.204.48/syl/'
    now = datetime.datetime.now()
    for days in range(PreDays):
        delta = datetime.timedelta(-1*days+4)
        n_days = now + delta
        DateStr = n_days.strftime("%Y-%m-%d").replace('-', '')
        filename = DateStr+'.zip'
        try:
            maybe_download(filename, work_directory, source_url+filename)
        except Exception:
            print(filename+' not available!!')
            continue

# ================================================================
# @retry(initial_delay=1.0, max_delay=16.0, is_retriable=_is_retriable)
# ================================================================
def urlretrieve_with_retry(url, filename=None):
    return urllib.request.urlretrieve(url, filename)
# ================================================================
def maybe_download(filename, work_directory, source_url):
    if not gfile.Exists(work_directory):
        gfile.MakeDirs(work_directory)
    filepath = os.path.join(work_directory, filename)
    if not gfile.Exists(filepath):
        temp_file_name, _ = urlretrieve_with_retry(source_url)
        gfile.Copy(temp_file_name, filepath)
        with gfile.GFile(filepath) as f:
            size = f.size()
        print('Successfully downloaded', filename, size, 'bytes.')
    return filepath
# ================================================================
# Un-ZipFIles
# ================================================================
def un_zip(file_name, ExtractFolder):
    """unzip zip file"""
    zip_file = zipfile.ZipFile(file_name)
    # print(file_name)
    for names in zip_file.namelist():
        zip_file.extract(names, ExtractFolder)
    zip_file.close()
# ================================================================
def SSEChinaIndexbatch_unzipFiles(zipfolder, extractfolder):
    print('batch unzip china index file from zip to XLS...........')
    storefolder = zipfolder
    ExtractFolder = extractfolder
    zipfileList = os.listdir(storefolder)
    for zipfile in zipfileList:
        # print(zipfile)
        Fullpathname = storefolder+zipfile
        dstFpathname = ExtractFolder+zipfile.replace('.zip', '.xls')
        if not gfile.Exists(dstFpathname):
            print('Now to extracting......'+dstFpathname.split('/')[-1])
            zipfilesize = os.path.getsize(Fullpathname)
            if zipfilesize > 280:
                un_zip(Fullpathname, ExtractFolder)

# ================================================================
# Base on the available TimeSeries files, Calculate the COT diffrent Indicators in one time and save to folders
# to realize the algorithim shows in web site: https://www.mql5.com/en/articles/1573
# ================================================================
class COTIndicatorsBuild(object):

    def __init__(self,clspatch):

        print('Handling the COT report indicators and output to indicators files!!!')
        #********************************************************************************
        # Indicators Type selected in INT
        self.OI                    =0
        self.NONCOMM_LONG          =1
        self.NONCOMM_SHORT         =2
        self.OPERATORS_LONG        =3
        self.OPERATORS_SHORT       =4
        self.NONREP_LONG           =5
        self.NONREP_SHORT          =6
        self.NET_NONCOMM           =7
        self.NET_OPERATORS         =8
        self.NET_NONREP            =9
        self.OI_NONCOMM_LONG      =10
        self.OI_NONCOMM_SHORT     =11
        self.OI_OPERATORS_LONG    =12
        self.OI_OPERATORS_SHORT   =13
        self.OI_NONREP_LONG       =14
        self.OI_NONREP_SHORT      =15
        self.WILLCO_NONCOMM       =16
        self.WILLCO_OPERATORS     =17
        self.WILLCO_NONREP        =18
        self.INDEX_OI             =19
        self.INDEX_NONCOMM        =20
        self.INDEX_OPERATORS      =21
        self.INDEX_NONREP         =22
        self.MOVEMENT_NONCOMM     =23
        self.MOVEMENT_OPERATORS   =24
        self.MOVEMENT_NONREP      =25
        self.MOVEMENT_OI          =26
        self.OI_NET_NONCOMM       =27
        self.OI_NET_OPERATORS     =28
        self.OI_NET_NONREP        =29
        #********************************************************************************
        #                           COT DATA TABLE
        self.n_str=0;               # number of strings
        self.realize_data=[]        # Array with dates
        # Arrays with absolute positions
        self.open_interes=[]        # Open Interest value
        self.noncomm_long=[]        # Long positions of non-commercial traders
        self.noncomm_short=[]       # Short positions of non-commercial traders
        self.noncomm_spread=[]      # Spread of non-commercial traders
        self.operators_long=[]      # Long positions of operators
        self.operators_short=[]     # Short positions of operators
        self.nonrep_long=[]         # Long positions of unreported traders (crowd)
        self.nonrep_short=[]        # Shortpositions of unreported traders (crowd)

        # An arrays contain the result of division of the absolute long position 
        # by short position for each of the category of traders

        self.oi_noncomm_long=[]     # Open Interest / Long positions of noncommercial traders
        self.oi_noncomm_short=[]    # Open Interest / Short positions of noncommercial traders
        self.oi_operators_long=[]   # Open Interest / Long positions of noncommercial traders (operators)
        self.oi_operators_short=[]  # Open Interest / Short positions of noncommercial traders (operators)
        self.oi_nonrep_long=[]      # Open Interest / Long positions of unreported traders (crowd)
        self.oi_nonrep_short=[]     # Open Interest / Short positions of unreported traders (crowd)

        # An arrays contain the result of division of the total net position by Open Interest
        # for each of the category of traders, it used for WILLCO calculation
        
        self.oi_net_noncomm=[]
        self.oi_net_operators=[]
        self.oi_net_nonrep=[]

        # Arrays with net positions of several groups of traders
        self.net_noncomm=[]         # Net position of noncommercial traders
        self.net_operators=[]       # Net position of commercial traders
        self.net_nonrep=[]          # Net position of unreported traders

        self.index_oi=[]            # Index of Open Interest
        self.index_ncomm=[]         # Index of noncommercial traders
        self.index_operators=[]     # Index of commercial traders (operators)
        self.index_nonrep=[]        # Index of unreported traders (crowd)

        # Arrays with Stochastic, calculated on division of
        # Open Interest by Total net position for each category of traders
        # Stohastic(OI/NET_POSITION)
        self.willco_ncomm=[]        # 
        self.willco_operators=[]    #
        self.willco_nonrep=[]       #

        #MOVEMENT INDEX
        self.movement_oi=[]
        self.movement_ncomm=[]
        self.movement_operators=[]
        self.movement_nonrep=[]

        self.IndicatorTitles=["OI ","NONCOMM_LONG ","NONCOMM_SHORT ","OPERATORS_LONG ","OPERATORS_SHORT ","NONREP_LONG ","NONREP_SHORT ","NET_NONCOMM ","NET_OPERATORS ","NET_NONREP ","OI_NONCOMM_LONG ","OI_NONCOMM_SHORT ","OI_OPERATORS_LONG ","OI_OPERATORS_SHORT ","OI_NONREP_LONG ","OI_NONREP_SHORT ","WILLCO_NONCOMM ","WILLCO_OPERATORS ","WILLCO_NONREP ","INDEX_OI ","INDEX_NONCOMM ","INDEX_OPERATORS ","INDEX_NONREP ","MOVEMENT_NONCOMM ","MOVEMENT_OPERATORS ","MOVEMENT_NONREP ","MOVEMENT_OI ","OI_NET_NONCOMM ","OI_NET_OPERATORS ","OI_NET_NONREP "]
        self.indicatorDict={"OI" :0,"NONCOMM_LONG ":1,"NONCOMM_SHORT ":2,"OPERATORS_LONG ":3,"OPERATORS_SHORT ":4,"NONREP_LONG ":5,"NONREP_SHORT ":6,"NET_NONCOMM ":7,"NET_OPERATORS ":8,"NET_NONREP ":9,"OI_NONCOMM_LONG ":10,"OI_NONCOMM_SHORT ":11,"OI_OPERATORS_LONG ":12,"OI_OPERATORS_SHORT ":13,"OI_NONREP_LONG ":14,"OI_NONREP_SHORT ":15,"WILLCO_NONCOMM ":16,"WILLCO_OPERATORS ":17,"WILLCO_NONREP ":18,"INDEX_OI ":19,"INDEX_NONCOMM ":20,"INDEX_OPERATORS ":21,"INDEX_NONREP ":22,"MOVEMENT_NONCOMM ":23,"MOVEMENT_OPERATORS ":24,"MOVEMENT_NONREP ":25,"MOVEMENT_OI ":26,"OI_NET_NONCOMM ":27,"OI_NET_OPERATORS ":28,"OI_NET_NONREP ":29}
        self.testfilename="C:\\WorkBench\\Investment\\CFTC\\WeeklyUpdatedTimeSeries\\20180102_0233BW_SOCAL INDEX - ICE FUTURES ENERGY DIV.csv"

        self.TS=pd.read_csv(self.testfilename)

        self.realize_data=self.TS.iloc[:,1]        # Array with dates
        self.open_interes=self.TS.iloc[:,3]        # Open Interest value
        self.noncomm_long=self.TS.iloc[:,4]        # Long positions of non-commercial traders
        self.noncomm_short=self.TS.iloc[:,5]       # Short positions of non-commercial traders
        self.noncomm_spread=self.TS.iloc[:,6]      # Spread of non-commercial traders
        self.operators_long=self.TS.iloc[:,7]      # Long positions of operators
        self.operators_short=self.TS.iloc[:,8]     # Short positions of operators
        self.nonrep_long=self.TS.iloc[:,9]         # Long positions of unreported traders (crowd)
        self.nonrep_short=self.TS.iloc[:,10]       # Shortpositions of unreported traders (crowd)
        
        try:
            self.oi_noncomm_long=self.noncomm_long/self.open_interes        # Open Interest / Long positions of noncommercial traders
        except Exception as e:
            print(e)
        try:
            self.oi_noncomm_short=self.noncomm_short/self.open_interes      # Open Interest / Short positions of noncommercial traders
        except Exception as e:
            print(e)
        try:
            self.oi_operators_long=self.operators_long/self.open_interes    # Open Interest / Long positions of noncommercial traders (operators)
        except Exception as e:
            print(e)
        try:
            self.oi_operators_short=self.operators_short/self.open_interes  # Open Interest / Short positions of noncommercial traders (operators)
        except Exception as e:
            print(e)
        try:
            self.oi_nonrep_long=self.nonrep_long/self.open_interes          # Open Interest / Long positions of unreported traders (crowd)
        except Exception as e:
            print(e)
        try:
            self.oi_nonrep_short=self.nonrep_short/self.open_interes        # Open Interest / Short positions of unreported traders (crowd)
        except Exception as e:
            print(e)
        
        # Arrays with net positions of several groups of traders
        self.net_noncomm=self.noncomm_long-self.noncomm_short               # Net position of noncommercial traders
        self.net_operators=self.operators_long-self.operators_short          # Net position of commercial traders
        self.net_nonrep=self.nonrep_long-self.nonrep_short                  # Net position of unreported traders
        

        try:
            self.oi_net_noncomm=self.net_noncomm/self.open_interes          
        except Exception as e:
            pass

        try:
            self.oi_net_operators=self.net_operators/self.open_interes
        except Exception as e:
            pass

        try:
            self.oi_net_nonrep=self.net_nonrep/self.open_interes
        except Exception as e:
            pass

    @classmethod
    def Scriptbuild(self,InteresList,period,startday):
        index_Indicators=[]
        period=1  # day time frame is 1????? to  be specify
        for i in range(len(InteresList)):
            Elementmax=self.ArrayMaximum(InteresList,period,i)
            Elementmin=self.ArrayMinimum(InteresList,period,i)
            delta=InteresList[Elementmax]-InteresList[Elementmin]
            if(delta==0):
                delta=1
            index_Indicators.append((InteresList[i]-InteresList[Elementmin])/delta*100)

        return index_Indicators

    @classmethod
    def ArrayMaximum(self, InteresList,period,i):
        pass

    @classmethod
    def ArrayMinimum(self, InteresList,period,i):
        pass

# ================================================================
# Cleaning the CFTC COT weekly report and output to Timeserie format for each future or option's code
# ================================================================
class COTreportsToTimeSeries(object):

    def __init__(self,clspath):

        print('Start to handling the COT report data')
        self.work_dir=clspath.GetCOTWorkdir()
        self.store_dir=clspath.GetCOTStoredir()
        self.TSdir=clspath.GetCOTTimeSeriesdir()

        #==============================================================================
        #Column position in the COT reports, start with 0      
        #==============================================================================  
        #               code date  name   openint  dealder  manager  hedge-fund  other-report  non-report
        #self.posNeeds = [3,   2,    0,      7,       8,9,     11,12,   14,15,      17,18,        22,23]
        #                0    1     2       3        4 5      6  7     8  9        10 11         12 13

        #==============================================================================
        #Column position in the COT reports, start with 0      
        #==============================================================================  
        #               code date  name   openint  NonCom(L-S)    Com(L-S)   NonRep(L-S)
        # self.posNeeds = [3,   2,    0,      7,     8,  9,         11,  12,     15,   16 ]
        #                0    1     2       3      4   5           6    7      8     9

        #==============================================================================
        #{"CFTC Contract Market Code","YYYYMMDD","Market and Exchange Names","Open Interest (All)","Noncommercial Positions-Long (All)","Noncommercial Positions-Short (All)","Noncommercial Positions-Spreading (All)","Commercial Positions-Long (All)","Commercial Positions-Short (All)","Nonreportable Positions-Long (All)","Nonreportable Positions-Short (All)","Change in Open Interest (All)","Change in Noncommercial-Long (All)","Change in Noncommercial-Short (All)","Change in Noncommercial-Spreading (All)","Change in Commercial-Long (All)","Change in Commercial-Short (All)","Change in Total Reportable-Long (All)","Change in Total Reportable-Short (All)","Change in Nonreportable-Long (All)","Change in Nonreportable-Short (All)","% of Open Interest (OI) (All)","% of OI-Noncommercial-Long (All)","% of OI-Noncommercial-Short (All)","% of OI-Noncommercial-Spreading (All)","% of OI-Commercial-Long (All)","% of OI-Commercial-Short (All)","% of OI-Total Reportable-Long (All)","% of OI-Total Reportable-Short (All)","% of OI-Nonreportable-Long (All)","% of OI-Nonreportable-Short (All)";
        # 0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30}
        #
        #==============================================================================

        self.posNeeds=[3,2,0,7,8,9,10,11,12,15,16,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56]
        
        self.COTTXTFileLists=os.listdir(self.work_dir)
        self.TotalCOTList =self.CleanAndGetNeedsColumnData(self.work_dir,self.COTTXTFileLists,self.posNeeds)
        self.TotalCOTList.sort()
        
        print('COT List Sorted!')

        self.TotalCOTList.reverse()
        
        print('COT List Reverse!!')

        self.TitlesArr=self.GetPosMatchTitltes(self.posNeeds)  
        self.WritToCSVFile(self.TotalCOTList,self.TitlesArr,self.TSdir)

    #==============================================================================
    # Write to single csv File by each CFTC code 
    #==============================================================================   
    @classmethod
    def  WSingleCSVfile(self,SubItemList,Titles,storedir):
        try:
            StoreFolder=storedir
            StrTMP=SubItemList[0].split(',')
            FileN=StrTMP[0]+'_'+StrTMP[2].rstrip()
            FileName=StoreFolder+FileN+'.csv'
            print(FileName)
            with open(FileName, 'w') as f:
                f.write(Titles+'\n')
                for item in SubItemList:
                    f.write(item+'\n')
        except Exception as e:
            print(e)

    #==============================================================================
    # Write to csv File by each CFTC code and with latest date in beginning seq
    #==============================================================================
    @classmethod    
    def WritToCSVFile(self,Totallist,Titles,storedir):
        SubItemList=[]
        for item in Totallist:      
            StrTEMP=item.split(',')
            LastStr=StrTEMP[0]
            try:    
                if (LastStr==SubItemList[-1].split(',')[0]):
                    SubItemList.append(item)
                else:
                    self.WSingleCSVfile(SubItemList,Titles,storedir)
                    print('Create New ItemList.............')
                    SubItemList=[]
                    SubItemList.append(item)          
            except Exception as e:
                    print(e)
                    SubItemList.append(item)
                    # continue
        print('CFTC Output To each Code Market and Exchange Names  .CSV Done!!!')

    #==============================================================================
    # Get All the valid lines we needs        
    #==============================================================================   
    @classmethod       
    def GetPosMatchTitltes(self,PosList):
        Titles=[]   
        TitlesTotal=["Market and Exchange Names","YYMMDD","YYYYMMDD","CFTC Contract Market Code","CFTC Market Code in Initials","CFTC Region Code","CFTC Commodity Code","Open Interest (All)","Noncommercial Positions-Long (All)","Noncommercial Positions-Short (All)","Noncommercial Positions-Spreading (All)","Commercial Positions-Long (All)","Commercial Positions-Short (All)"," Total Reportable Positions-Long (All)","Total Reportable Positions-Short (All)","Nonreportable Positions-Long (All)","Nonreportable Positions-Short (All)","Open Interest (Old)","Noncommercial Positions-Long (Old)","Noncommercial Positions-Short (Old)","Noncommercial Positions-Spreading (Old)","Commercial Positions-Long (Old)","Commercial Positions-Short (Old)","Total Reportable Positions-Long (Old)","Total Reportable Positions-Short (Old)","Nonreportable Positions-Long (Old)","Nonreportable Positions-Short (Old)","Open Interest (Other)","Noncommercial Positions-Long (Other)","Noncommercial Positions-Short (Other)","Noncommercial Positions-Spreading (Other)","Commercial Positions-Long (Other)","Commercial Positions-Short (Other)","Total Reportable Positions-Long (Other)","Total Reportable Positions-Short (Other)","Nonreportable Positions-Long (Other)","Nonreportable Positions-Short (Other)","Change in Open Interest (All)","Change in Noncommercial-Long (All)","Change in Noncommercial-Short (All)","Change in Noncommercial-Spreading (All)","Change in Commercial-Long (All)","Change in Commercial-Short (All)","Change in Total Reportable-Long (All)","Change in Total Reportable-Short (All)","Change in Nonreportable-Long (All)","Change in Nonreportable-Short (All)","% of Open Interest (OI) (All)","% of OI-Noncommercial-Long (All)","% of OI-Noncommercial-Short (All)","% of OI-Noncommercial-Spreading (All)","% of OI-Commercial-Long (All)","% of OI-Commercial-Short (All)","% of OI-Total Reportable-Long (All)","% of OI-Total Reportable-Short (All)","% of OI-Nonreportable-Long (All)","% of OI-Nonreportable-Short (All)","% of Open Interest (OI)(Old)","% of OI-Noncommercial-Long (Old)","% of OI-Noncommercial-Short (Old)","% of OI-Noncommercial-Spreading (Old)","% of OI-Commercial-Long (Old)","% of OI-Commercial-Short (Old)","% of OI-Total Reportable-Long (Old)","% of OI-Total Reportable-Short (Old)","% of OI-Nonreportable-Long (Old)","% of OI-Nonreportable-Short (Old)","% of Open Interest (OI) (Other)","% of OI-Noncommercial-Long (Other)","% of OI-Noncommercial-Short (Other)","% of OI-Noncommercial-Spreading (Other)","% of OI-Commercial-Long (Other)","% of OI-Commercial-Short (Other)","% of OI-Total Reportable-Long (Other)","% of OI-Total Reportable-Short (Other)","% of OI-Nonreportable-Long (Other)","% of OI-Nonreportable-Short (Other)","Traders-Total (All)","Traders-Noncommercial-Long (All)","Traders-Noncommercial-Short (All)","Traders-Noncommercial-Spreading (All)","Traders-Commercial-Long (All)","Traders-Commercial-Short (All)","Traders-Total Reportable-Long (All)","Traders-Total Reportable-Short (All)","Traders-Total (Old)","Traders-Noncommercial-Long (Old)","Traders-Noncommercial-Short (Old)","Traders-Noncommercial-Spreading (Old)","Traders-Commercial-Long (Old)","Traders-Commercial-Short (Old)","Traders-Total Reportable-Long (Old)","Traders-Total Reportable-Short (Old)","Traders-Total (Other)","Traders-Noncommercial-Long (Other)","Traders-Noncommercial-Short (Other)","Traders-Noncommercial-Spreading (Other)","Traders-Commercial-Long (Other)","Traders-Commercial-Short (Other)","Traders-Total Reportable-Long (Other)","Traders-Total Reportable-Short (Other)","Concentration-Gross LT = 4 TDR-Long (All)","Concentration-Gross LT =4 TDR-Short (All)","Concentration-Gross LT =8 TDR-Long (All)","Concentration-Gross LT =8 TDR-Short (All)","Concentration-Net LT =4 TDR-Long (All)","Concentration-Net LT =4 TDR-Short (All)","Concentration-Net LT =8 TDR-Long (All)","Concentration-Net LT =8 TDR-Short (All)","Concentration-Gross LT =4 TDR-Long (Old)","Concentration-Gross LT =4 TDR-Short (Old)","Concentration-Gross LT =8 TDR-Long (Old)","Concentration-Gross LT =8 TDR-Short (Old)","Concentration-Net LT =4 TDR-Long (Old)","Concentration-Net LT =4 TDR-Short (Old)","Concentration-Net LT =8 TDR-Long (Old)","Concentration-Net LT =8 TDR-Short (Old)","Concentration-Gross LT =4 TDR-Long (Other)","Concentration-Gross LT =4 TDR-Short(Other)","Concentration-Gross LT =8 TDR-Long (Other)","Concentration-Gross LT =8 TDR-Short(Other)","Concentration-Net LT =4 TDR-Long (Other)","Concentration-Net LT =4 TDR-Short (Other)","Concentration-Net LT =8 TDR-Long (Other)","Concentration-Net LT =8 TDR-Short (Other)","Contract Units","CFTC Contract Market Code (Quotes)","CFTC Market Code in Initials (Quotes)","CFTC Commodity Code (Quotes)"]
        for i in range(len(PosList)):
            Titles.append(TitlesTotal[PosList[i]])
        Separator=","
        TitleStr=Separator.join(Titles)     
        print(TitleStr)
        return TitleStr
        
    @classmethod
    def CleanAndGetNeedsColumnData(self,path,COTTXTFileLists,posNeeds):
        TotalCOTList=[]
        for file in COTTXTFileLists: #遍历文件夹P
            if not os.path.isdir(file): #判断是否是文件夹，不是文件夹才打开
                f = open(path+file,encoding='UTF-8') #打开文件
                iter_f = iter(f) #创建迭代器
                for line in itertools.islice(iter_f, 1, None):#skip the first Line
                    ValidLine=self.GetNeedsCols(line,posNeeds)
                    TotalCOTList.append(ValidLine)  
        return TotalCOTList
   
    # ==============================================================================
    # return back what we needs column inside the CFTC report
    # ==============================================================================
    @classmethod
    def GetNeedsCols(self,LineStr,posNeedsArr): 
        Line=LineStr.replace('/','').replace('\"','').replace('<','').replace('>','').replace('(','').replace(')','')
        Temp=Line.split(',')
        try:
            AdjustPos=0
            DateStr=datetime.datetime.strptime(Temp[2+AdjustPos],'%Y-%m-%d')
        except Exception as e:
            try:
                AdjustPos=1
                DateStr=datetime.datetime.strptime(Temp[2+AdjustPos],'%Y-%m-%d')
            except Exception as e:
                try:
                    AdjustPos=2
                    DateStr=datetime.datetime.strptime(Temp[2+AdjustPos],'%Y-%m-%d')
                except Exception as e:
                    print(e)

        normalizeDatetime=datetime.datetime.strftime(DateStr,'%Y-%m-%d').replace('-','')
        NeededCols=[]
        for item in posNeedsArr:
            if item==0:           
                if AdjustPos==0:
                    NeededCols.append(Temp[item+AdjustPos])  
                else:
                    strT=''
                    for ii in range(AdjustPos):
                        strT+=Temp[item+ii]
                    NeededCols.append(strT)  
            elif item==2:
                NeededCols.append(normalizeDatetime)
            else:   
                NeededCols.append(Temp[item+AdjustPos].strip())  
        Separator=","
        NewSingeline=Separator.join(NeededCols)     
        return NewSingeline          

# ================================================================
# Rank TOP X for each day for the RZYE or other value
# ================================================================
class RankTOPX(object):
    
    def __init__(self,clspath):
        print("=============RANKTOPX STKCode Sets=================================")
        self.RZRQColumn=['标的证券简称', '标的证券代码', '本日融资余额(元)', '本日融资买入额(元)', '本日融资偿还额(元)', '本日融券余量','本日融券卖出量', '本日融券偿还量']
        self.RZRQENColumns=['StkName','stkcode','rzye','rzmle','rzche','rqyl','rqmcl','rqchl']
        self.work_dir=clspath.GetRZRQWorkdir()
        print(self.work_dir)

        self.TOPX=30   # TOP 10 RZRQYE amount
        self.prexday=3 # PreXdays ACC RZRQ total amount  
        # self.testdir=r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/RZRQ/RZRQDailyTXT/'
        self.testdir=self.work_dir
        self.files=os.listdir(self.testdir)
        self.files.sort()
        self.RZRQTOPXRankDataSet=pd.DataFrame([['TBD','700000',0,0,0,0,0,0]],columns=self.RZRQENColumns)
        self.RZRQTOPXRankDataSet['date']='2018-04-24'

        print(self.RZRQTOPXRankDataSet)
        print("=================================================================")
        for file in self.files:                         
            self.df1=pd.read_csv(self.testdir+file,encoding="utf-8")
            self.df2=pd.DataFrame(self.df1.values,columns=self.RZRQENColumns)
            self.rzrqtopx=self.df2.sort_values(by=['rzmle'],ascending=[False])
            self.rzrqtopx['date']=str(file.replace(".txt",""))
            self.rzrqtopx['rank']=self.df2.index+1
            self.TOPXrankdataset=self.rzrqtopx[:self.TOPX]
            self.RZRQTOPXRankDataSet=self.RZRQTOPXRankDataSet.append(self.TOPXrankdataset,ignore_index=True)

        print(self.RZRQTOPXRankDataSet[-self.TOPX:])

        self.individulefolder=r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/RZRQ/DailyTOPXRankList/'
        self.outputfile=r'RZRQTOPXRank.csv'
        try:
            os.mkdir(self.individulefolder)
        except:
            os.mkdir(r'G:/TimeSeriesData/RZRQ/TOPXRankListSummary/')

        self.RZRQTOPXRankDataSet.to_csv(self.individulefolder+self.outputfile,index=False,sep=',',encoding="utf-8")

        self.TOPXRZList=[]
        self.ColumnNeeds=[2,1,0]
        # self.TOPXRZList=self.GettheTOPXList(self.work_dir,self.TOPX)
        # print(self.TOPXRZList[:10])


    
    #==============================================================================
    # Get  Date+Value+code format strings for sort and revered       
    #==============================================================================     
    @classmethod    
    def GettheTOPXList(self,work_dir,TOPX):
        TOPList=[]
        filenames=os.listdir(work_dir)
        for file in filenames:
            EachDayTOPlist=[]
            with open(work_dir+ file,encoding='UTF-8') as f:
                 print(work_dir+file)
                 iter_f = iter(f)
                 for line in itertools.islice(iter_f, 1, None):#skip the first Line
                     Vline=self.GetDayRANKLines(line,file)
                     EachDayTOPlist.append(Vline)
                    #  print(EachDayTOPlist[-1])
            EachDayTOPlist.sort()
            EachDayTOPlist.reverse()
            print(EachDayTOPlist[:10])
            TOPList.append(EachDayTOPlist[:TOPX])
        return TOPList


    @classmethod
    def GetTheACCTOPXlist(self,work_dir,TOPX,PreXdays):
        pass

    @classmethod
    def GetDayRANKLines(self,line,filenamewithdate):
        LineCols=line.split(',')
        sep=','
        return(sep.join([filenamewithdate.replace('.txt',''), LineCols[2],LineCols[1]]))
    
# ================================================================
#Cleaning the RZRQ data and output to timeserires format for each stock code
# ================================================================
class RZRQDataSetToTimeSeries(object):
      
    def __init__(self,clspath):
        print('Now RZRQ daily transfer to day time series files.......')
        self.work_dir=clspath.GetRZRQWorkdir()
        self.store_dir=clspath.GetRZRQStoreDir()
        self.clusterdir=clspath.GetRZRQCLusterDir()
        self.TSdir=clspath.GetRZRQTSdir()
        
        self.rzrqfiles01=os.listdir(self.store_dir)
        self.BatchtransferXLStoCSV(self.store_dir,self.work_dir,self.rzrqfiles01)
        
        self.posNeeds = [2,0,1,3,4,5,6,7,8]
        self.TitlesArr=self.GetPosMatchTitltes(self.posNeeds)         
        self.rzrqfiles02=os.listdir(self.work_dir)
        self.rzrqfiles02.sort()
        self.RZRQTList=self.SummaryAllRZRQListAndDistributeToeachCodeTS(self.work_dir,self.rzrqfiles02,self.posNeeds)
        
        self.RZRQTList.sort()
        print("Sorted RZRQlist!")

        self.RZRQTList.reverse()
        print("reverse RZRQlist!")

        self.WritToCSVFile(self.RZRQTList,self.TitlesArr,self.TSdir)
        # self.TitleIndicators=['Net Buy'']
        # self.IndicatorsBuildForRZRQPanelData(self.RZRQTList,self.TitlesArr,self.TSdir)

        self.prexdays=5
        self.RZRQKMeansList=self.GetPreXdaysSectionData(self.RZRQTList,self.prexdays)
        
        for item in self.RZRQKMeansList:
            print('=====================================================')
            print(str(sum(item))+'\r')

        # print(self.RZRQKMeansList[-1:])

        print("Start to Kmeans to Cluster the Stock Base on preXdays RZRQ days")

        # # Machine learning kmena Algo starting here
        # self.RZRQDataSet=np.array(self.RZRQKMeansList)
        # self.scaler = preprocessing.StandardScaler()  
        # self.RZRQStandarDataSet = self.scaler.fit_transform(self.RZRQDataSet)
        # # #假如我要构造一个聚类数为 X 的聚类器
        
        # # #构造聚类器
        # self.estimator = KMeans(n_clusters=7)
        # # #聚类
        # self.estimator.fit(self.RZRQStandarDataSet)
        # # #获取聚类标签
        # self.label_pred = self.estimator.labels_
        # # #获取聚类中心 
        # self.centroids = self.estimator.cluster_centers_
        # # # 获取聚类准则的总和 
        # self.inertia = self.estimator.inertia_

        # print(self.label_pred) 

    #==============================================================================
    # Get All the valid lines we needs        
    #============================================================================== 
    @classmethod
    def SummaryAllRZRQListAndDistributeToeachCodeTS(self,path,rzrqfiles,posNeeds): 
        TotalRZRQList=[]
        for file in rzrqfiles: #遍历文件夹P
            if not os.path.isdir(file): #判断是否是文件夹，不是文件夹才打开
                f = open(path+file,encoding='UTF-8') #打开文件
                iter_f = iter(f) #创建迭代器
                for line in itertools.islice(iter_f, 1, None):#skip the first Line
                    TsRZRQLine=file.replace('.txt',',')+line
        #              print(TsRZRQLine)
                    ValidLine=self.GetvalidCols(TsRZRQLine,posNeeds)
                    TotalRZRQList.append(ValidLine)  
        return TotalRZRQList

    #==============================================================================
    # Batches Convert XLS to CSV        
    #============================================================================== 
    @classmethod
    def BatchtransferXLStoCSV(self,sourcepath,dstpath,files):
        for file in files: #遍历文件夹
            Sourcefilename=sourcepath+file
            Destinationfilename=dstpath+file.replace('xls','txt').replace('rzrqjygk','')
            if os.path.exists(Destinationfilename):
                print(Destinationfilename.split('/')[-1]+' exist! no need to re-transfer')
            else:
                self.xlsx_to_csv_pd(Sourcefilename,Destinationfilename) 
    #==============================================================================
    # Convert Xls to CSV        
    #==============================================================================
    @classmethod    
    def xlsx_to_csv_pd(self,Fromfile,TotxtFile):   
        data_xls = pd.read_excel(Fromfile, sheetname=1, index_col=1)
        data_xls.to_csv(TotxtFile, encoding='utf-8')
        print(TotxtFile)
    #==============================================================================
    # Return the PreXdayValid data and transfer to X*PreXday Array       
    #============================================================================== 
    @classmethod
    def GetPreXdaysSectionData(self,Totallist,Preday):
        ValidList=[]
        CodeList=[]
        SubItemList=[]
        for item in Totallist:           
            StrTEMP=item.split(',')
            LastStr=StrTEMP[0]
            try:    
                if (LastStr==SubItemList[-1].split(',')[0]):
                    SubItemList.append(item)              
                else:
                    if len(SubItemList)>=Preday:
                        RZBalanceList=[]
                        RZCodeList=[]
                    for ii in range(Preday):
                            SubitemStr=SubItemList[ii].split(',')
                            try:
                                BalanceValue=float(SubitemStr[4])
                            except Exception as e :
                                BalanceValue=float(0.0001)
                                print(e)
                                continue
                            RZBalanceList.append(BalanceValue)
    #                        3 for remaining balance, 4 for todaybuy value
                            RZCodeList.append([SubitemStr[0],SubitemStr[1],SubitemStr[2],SubitemStr[3]])
                    ValidList.append(RZBalanceList)
                    CodeList.append(RZCodeList)
                    print('Geting PreXDays data, now Create New ItemList.............')
                    SubItemList=[]
                    SubItemList.append(item)          
            except :
                    SubItemList.append(item)
                    continue

        return ValidList

    #==============================================================================
    #return back what we needs column inside the RZRQ List        
    #==============================================================================    
    @classmethod
    def GetvalidCols(self,LineStr,posNeedsArr): 
        Line=LineStr.replace('/','').replace('\"','').replace('*','').replace('\n','')
        Temp=Line.split(',')
        if len(Temp)>9:
            print(len(Temp))
        NewLineNeeds=[]
        for i in range(len(posNeedsArr)):
            NewLineNeeds.append(Temp[posNeedsArr[i]])
        NetBuyValue=float(Temp[3])-float(Temp[4])
        NewLineNeeds.append(str(NetBuyValue))
        NewSingeline=','.join(NewLineNeeds)  
        return NewSingeline

    #==============================================================================
    # Get All the valid lines we needs        
    #==============================================================================          
    @classmethod
    def GetPosMatchTitltes(self,PosList):
        Titles=[]   
        TitlesTotal=["日期","标的证券简称","标的证券代码","本日融资余额(元)","本日融资买入额(元)","本日融资偿还额(元)","本日融券余量","本日融券卖出量","本日融券偿还量"]
        for i in range(len(PosList)):
            Titles.append(TitlesTotal[PosList[i]])
        Separator=","
        Titles.append('融资净买入额(元)')
        TitleStr=Separator.join(Titles)    
        print('Adding the Titles') 
        print(TitleStr)
        return TitleStr

    #==============================================================================
    # Output to Files by the kmeans Cluster, and the same time adjust Titles
    #==============================================================================
    @classmethod
    def OutputClusterStockToFiles(self,Filename,RZNamelistT,RZRQKMeansListT,label_predT):   
    #    FileName=r'G:/TimeSeriesData/RZPAN/PyKMeansClusterResults/PyKmeansStockCluster.csv'    
        FileName=Filename
        Titles=[]
        Titles.append('StockCode')
        Titles.append('StockName')
        Titles.append('kmeansCluster')
        for ii in range(len(RZRQKMeansListT[0])):
            Titles.append('D_N-'+str(ii))
        TitlesStr=(','.join(Titles))
        
        BalanceList=[]
        for i in range(len(RZNamelistT)):
            LinesNameClusterBalance=[]
    #        Str=str(RZNamelistT[i][0][0])+','+RZNamelistT[i][0][2]+','+str(label_predT[i])
            LinesNameClusterBalance.append(str(RZNamelistT[i][0][0]))
            LinesNameClusterBalance.append(str(RZNamelistT[i][0][2]))
            LinesNameClusterBalance.append(str(label_predT[i]))
            for j in range(len(RZRQKMeansListT[i])):
                LinesNameClusterBalance.append(str(RZRQKMeansListT[i][j]))
            SEP=','
            Str=SEP.join(LinesNameClusterBalance)
    #        print(Str)
            BalanceList.append(Str)
            
        with open(FileName, 'w',encoding='UTF-8') as f:
            f.write(TitlesStr+'\n')
            for item in BalanceList:
                f.write(item+'\n')
                
        print('Output The Kmeans Cluster reuslts !!!')

    #==============================================================================
    # Write to single csv File by each CFTC code 
    #==============================================================================   
    @classmethod
    def  WSingleCSVfile(self,SubItemList,Titles, storedir):
        try:
            # StoreFolder=r'C:/WorkBench/Investment/RZRQ/PYXLS2TXT2EachCodeTS/'
            StoreFolder=storedir
            StrTMP=SubItemList[0].split(',')
            # FileN=StrTMP[1]+'_'+StrTMP[0]+'_'+StrTMP[2].rstrip()
            FileN=StrTMP[0]+'_'+StrTMP[2].rstrip()
            FileName=StoreFolder+FileN+'.txt'
            print(FileName)
            with open(FileName, 'w',encoding='utf-8') as f:
                f.write(Titles+'\n')
                for item in SubItemList:
                    f.write(item+'\n')
        except Exception as e:
            print(e)

    #==============================================================================
    # Write to csv File by each CFTC code and with latest date in beginning seq
    #==============================================================================    
    @classmethod
    def WritToCSVFile(self,Totallist,Titles,TSdir):
        # os.removedirs(TSdir)
        SubItemList=[]
        for item in Totallist:      
            StrTEMP=item.split(',')
            LastStr=StrTEMP[0]
            try:    
                if (LastStr==SubItemList[-1].split(',')[0]):
                   SubItemList.append(item)
                else:
                    # SubItemList.sort()
                    self.WSingleCSVfile(SubItemList,Titles,TSdir)
                    print('Create New ItemList.............')
                    SubItemList=[]
                    SubItemList.append(item)          
            except :
                SubItemList.append(item)
                continue  
        print('融资融券每日数据输出转换成每只代码的时间序列数据完成!!!')     


class RZRQ_Pd_Algo_TS(object):

    def __init__(self,clspatch):    
        
        self.work_dir=clspatch.GetRZRQWorkdir()
        self.TS_dir=clspatch.GetRZRQTSdir()

        self.individuleFile=r'C:/WorkBench/Investment/RZRQ/DailyTXT/20180418.txt'
        self.RZRQColumn=['标的证券简称', '标的证券代码', '本日融资余额(元)', '本日融资买入额(元)', '本日融资偿还额(元)', '本日融券余量','本日融券卖出量', '本日融券偿还量']
        self.RZRQENColumns=['StkName','stkcode','rzye','rzmle','rzche','rqyl','rqmcl','rqchl']
        self.TOPX=20   # TOP 10 RZRQYE amount
        self.prexday=3 # PreXdays ACC RZRQ total amount  
        # self.testdir=r'/Users/mac/Desktop/Projects/COT_Report/CFTCCOTRZRQ/RZRQ/RZRQDailyTXT/'
        self.files=os.listdir(self.work_dir)
        self.RZRQTOPXRankDataSet=pd.DataFrame([['TBD','700000',0,0,0,0,0,0]],columns=self.RZRQENColumns)
        self.RZRQTOPXRankDataSet['date']='2018-04-24'

        print(self.RZRQTOPXRankDataSet)
        print("=================================================================")
        for file in self.files:                         
            self.df1=pd.read_csv(self.work_dir+file,encoding="utf-8")
            self.rzrqtopx=pd.DataFrame(self.df1.values,columns=self.RZRQENColumns)
            self.rzrqtopx['date']=str(file.replace(".txt",""))
            self.TOPXrankdataset=self.rzrqtopx
            self.RZRQTOPXRankDataSet=self.RZRQTOPXRankDataSet.append(self.TOPXrankdataset,ignore_index=True)
            print(self.RZRQTOPXRankDataSet.count())
        print(self.RZRQTOPXRankDataSet[-self.TOPX:])
        
        # self.RZRQTOPXRankDataSet.to_csv(r'C:/WorkBench/Investment/RZRQ/DailyTOPXRankList/RZRQTOPXRank.csv',index=False,sep=',',encoding="utf-8")

# ================================================================
# ================================================================
if __name__ == '__main__':
    updateDayneeds=100
    Faddress = PathSets()
    Faddress.init()
    Faddress.Maybecreatedir()

    # UpdatingSSEIndexData(Faddress.GetSSEStoredir(), updateDayneeds) 
    # SSEChinaIndexbatch_unzipFiles(Faddress.GetSSEStoredir(), Faddress.GetSSEWorkdir())
    UpdatingRZRQdataDailyData(Faddress.GetRZRQStoreDir(), updateDayneeds)
    # UpdatingCFTCWeeklyData(Faddress.GetCOTStoredir(), Faddress.GetCOTWorkdir(), StartYear=2005, EndYear=2018)

    # ==============================================================================
    RZRQHandls=RZRQDataSetToTimeSeries(Faddress) 

    COTreportHandls=COTreportsToTimeSeries(Faddress)

    # RZRQTSPDhandls=RZRQ_Pd_Algo_TS(Faddress) # Not fast enough, forget it

    RankRZRQLIST=RankTOPX(Faddress)   

    # FileName=Faddress.GetRZRQCLusterDir+'PyKmeansStockCluster.csv'       
    # OutputClusterStockToFiles(FileName,RZNamelist,RZRQKMeansList,label_pred)
    # FileNameNormalizeInside=Faddress.GetRZRQCLusterDir+'PyKmeansStockClusterNormalize.csv'    
    # OutputClusterStockToFiles(FileNameNormalizeInside,RZNamelist,RZRQStandarDataSet,label_pred)
