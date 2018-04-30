import datetime
import os
import sys

import pandas as pd
import tushare as ts

SZfolder=r'G:/TimeSeriesData/RZRQ/SHSZDailytxt/'

Dates=pd.date_range(start='2018-04-25',end='2018-04-30',freq='D')

for item in Dates:
    nDay=datetime.datetime.strftime(item,'%Y-%m-%d')  
    try:
        ndayfilename=SZfolder+'sz'+nDay.replace('-','')+'.txt'
        if not(os.path.exists(ndayfilename)):
            SZRZRQ=ts.sz_margin_details(date=nDay)
            if len(SZRZRQ)>2:    
                print(SZRZRQ[-5:])
                SZRZRQ.to_csv(ndayfilename,sep=',')
                print(ndayfilename)
    except:
        pass

    try:
        ndayfilename=SZfolder+'sh'+nDay.replace('-','')+'.txt'
        if not(os.path.exists(ndayfilename)):
            SHRZRQ=ts.sh_margin_details(date=nDay)
            if len(SZRZRQ)>2:             
                print(SHRZRQ[-5:])
                SHRZRQ.to_csv(ndayfilename,sep=',')
                print(ndayfilename)
    except:
        pass
