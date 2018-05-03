# !usr/bin/python
# -*- coding: utf-8 -*-


import datetime
import os
import sys

import pandas as pd
import tushare as ts

import itertools

import importlib 

# sys.setdefaultencoding('gbk')

SZfolder=r'G:/TimeSeriesData/RZRQ/SHSZDailytxt/'

Dates=pd.date_range(start='2018-04-22',end='2018-05-03',freq='D')

for item in Dates:
    nDay=datetime.datetime.strftime(item,'%Y-%m-%d')  
    try:
        ndayfilename=SZfolder+'sz'+nDay.replace('-','')+'.txt'
        if not(os.path.exists(ndayfilename)):
            SZRZRQ=ts.sz_margin_details(date=nDay)
            if len(SZRZRQ)>2:    
                print(SZRZRQ[-5:])
                SZRZRQ.to_csv(ndayfilename,sep=',',encoding='utf-8')
                print(ndayfilename)
            else:
                print('Len <2, no margin data for '+nDay)
        else:
            print(ndayfilename+' available,no need to download!!')
    except:
        pass

    try:
        ndayfilename=SZfolder+'sh'+nDay.replace('-','')+'.txt'
        if not(os.path.exists(ndayfilename)):
            SHRZRQ=ts.sh_margin_details(start=nDay,end=nDay)
            if len(SZRZRQ)>2:             
                print(SHRZRQ[-5:])
                SHRZRQ.to_csv(ndayfilename,sep=',',encoding='utf-8')
                print(ndayfilename)
            else:
                print('Len <2, no margin data for '+nDay)
        else:
            print(ndayfilename+' available,no need to download!!')
    
    except:
        pass

filesnlist=os.listdir(SZfolder)
filesnlist.sort()
path=SZfolder

def GetNeedspos(Temp,Poslist):
    sep=','
    Tlist=[]
    for i in range(len(Poslist)):
        Tlist.append(Temp[Poslist[i]])
    return sep.join(Tlist)

def NormalizeZLines(Poslist,inputline):
    Temp=inputline.split(',')
    neededline=GetNeedspos(Temp,Poslist)
    return neededline

TotalRZRQList=[]


# opDate:信用交易日期          1
# stockCode:标的证券代码       2
# securityAbbr:标的证券简称    3
# rzye:本日融资余额(元)        4
# rzmre: 本日融资买入额(元)    5
# rzche:本日融资偿还额(元)     6
# rqyl: 本日融券余量           7
# rqmcl: 本日融券卖出量        8
# rqchl: 本日融券偿还量        9
# SH
# , opDate ,stockCode,securityAbbr,rzye,rzmre,rzche,rqyl,rqmcl,rqchl
# 0,2014-09-22,510010,治理ETF,45002403,326308,92803,1858045,276373,444421
# 0,         1,     2,     3,       4,     5,    6,      7,      8,     9

# opDate:信用交易日期(index)   9
# rzmre: 融资买入额(元)        3
# rzye:融资余额(元)            4
# rqmcl: 融券卖出量            5
# rqyl: 融券余量               6
# rqye: 融券余量(元)           7
# rzrqye:融资融券余额(元)       8
# SZ
# ,stockCode,securityAbbr,rzmre,   rzye,  rqmcl,    rqyl,   rqye,    rzrqye,    opDate
# 0,000001,平安银行,143964922,2298591511,1634827,2249400,22628965,2321220476,2014-09-22
# 0,      1,     2,         3,         4,      5,      6,       7,         8,        9

for file in filesnlist: #遍历文件夹P
    if not os.path.isdir(file): #判断是否是文件夹，不是文件夹才打开
        f = open(path+file,'r',encoding='utf-8') #打开文件
        iter_f = iter(f) #创建迭代器   
        if 'sh' in file:
            PosSHSZ=[2,1,3,4,5,8,7]
        elif 'sz' in file:
            PosSHSZ=[1,9,2,4,3,5,6]
        for line in itertools.islice(iter_f, 1, None):#skip the first Line
            NormalizeLine=NormalizeZLines(PosSHSZ,line.replace('\n',''))
            TotalRZRQList.append(NormalizeLine)
            # print(TotalRZRQList[-1])
TotalRZRQList.sort()
Dfrzrq=pd.DataFrame(TotalRZRQList)
Dfrzrq.to_csv(r'G:/TimeSeriesData/RZRQ/SHSZSummary/TotalList.txt',encoding='utf-8')
# sorted(TotalRZRQList)
# for item in TotalRZRQList:
#     print(item)
# TotalRZRQList[-10:]



def  WSingleCSVfile(SubItemList,Titles, storedir):
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
            print(Titles)
            for item in SubItemList:
                print(item)
                f.write(item+'\n')
    except Exception as e:
        print(e)

def WritToCSVFile(Totallist,Titles,TSdir):
    # os.removedirs(TSdir)
    SubItemList=[]
    for item in Totallist: 
        print(item)     
        StrTEMP=item.split(',')
        LastStr=StrTEMP[0]
        # print(LastStr)
        
        try:    
            if (LastStr==SubItemList[-1].split(',')[0]):
                SubItemList.append(item)
            else:
                # SubItemList.sort()
                print(SubItemList[-1].split(',')[0])
                WSingleCSVfile(SubItemList,Titles,TSdir)
                print('Create New ItemList.............')
                SubItemList=[]
                SubItemList.append(item)          
        except :
            SubItemList.append(item)
            continue  
    print('融资融券每日数据输出转换成每只代码的时间序列数据完成!!!') 

TimeSeriesDir=r'G:/TimeSeriesData/RZRQ/SHSZDailyTimeSeries/'


Titles=['index','rzrqstrings']

WritToCSVFile(TotalRZRQList,Titles,TimeSeriesDir)    