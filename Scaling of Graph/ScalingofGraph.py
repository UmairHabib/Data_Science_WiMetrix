import sys,os

import sqlalchemy as db
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math as m
from datetime import datetime
import arrow
engine = db.create_engine(
    "mssql+pyodbc://dev:spts@3311@192.168.4.114,50003/cfl_spts?driver=SQL+Server+Native+Client+11.0")
Interval=26

def smoothing(x,y,width):
    poly=np.polyfit(x,y,3)
    poly_y = np.poly1d(poly)(x)
    plt.plot(x,poly_y,linewidth=width)
def timeInHours(x):
    minDate = arrow.get(str(x[0]))  # datetime.strptime(str(dates[0]), date_format)
    maxDate = arrow.get(str(x[-1]))  # datetime.strptime(str(dates[-1]), date_format)
    delta = maxDate - minDate
    totaldays = delta.days + 1  # 1 is added to including last date
    totalhours = totaldays * 24
    #print('Total Hours', totalhours)
    return totalhours
def  OrderDateGetter(orderno):
    strOrderNO = "'" + str(orderno) + "'"
    query=''' select distinct p_date
    from cfl_spts.progresscomplete
    where [orderID] =  ''' + strOrderNO + '''
    order by p_date
     '''

    df = pd.read_sql_query(query, engine)
    dates=list(df['p_date'].values)
    #print(dates[0], dates[-1])
    HourTime = []
    sum = 0
    res= timeInHours(dates)
    Gap =res/Interval
    Gap = m.ceil(Gap)
    #print('Gap', Gap)
    if (res < Interval):
        for i in range(1, res + 1):
            HourTime.append(i)
    else:
        for i in range(0, Interval):
            sum = sum + Gap
            HourTime.append(sum)
        if (sum != res):  # last index will contain remainder also
            HourTime[-1] += res- sum
    #print(HourTime)
    return HourTime,dates[0]

def OrderOperationGetter(orderno):

    strOrderNO = "'" + str(orderno) + "'"
    query = '''  SELECT count(Distinct(operationAutoID)) as 'Counter'
      FROM [cfl_spts].[cfl_spts].[stylebulletin]
      WHERE opsequence IS NOT NULL AND [orderID]=''' + strOrderNO + ''' '''
    df = pd.read_sql_query(query, engine)
    MaxOps=int(df['Counter'])
    #print("Ops",MaxOps)
    arr=[]
    Gap=m.ceil(MaxOps/Interval)
    if(MaxOps<Interval):
        for i in range(1,MaxOps+1):
            arr.append(i)
    else:
        for i in range(1,Interval+1):
            arr.append(i+Gap)
    return arr,MaxOps

def DateTimeConverter(x,minDate):
    arr=[]
    minDate = arrow.get(minDate)  # datetime.strptime(str(dates[-1]), date_format)
    for each in x:
        maxDate = arrow.get(each.date())  # datetime.strptime(str(dates[-1]), date_format)
        delta = maxDate - minDate
        totaldays = delta.days + 1  # 1 is added to including last date
        totalhours = totaldays * 24
        str1=str(each.time())
        totalhours=totalhours+int(str1[0:2])
        arr.append(totalhours)
    #print('Total Hours', arr)
    return arr
def OperationConverter(x,MaxOps):
    arr=[]
    #print('xx',x)
    Gap=m.ceil(MaxOps/Interval)
    if(MaxOps<Interval):
        for i in range(1,len(x)+1):
            arr.append(i)
    else:
        for i in range(1,Interval):
            arr.append(i+Gap)
    return arr
def order_plotter(orderno):
    strOrderNO = "'" + str(orderno) + "'"
    entries = 500
    strentries= str(entries)
    query = '''SELECT DISTINCT [bundleID]
    FROM [cfl_spts].[cfl_spts].[progresscomplete]
    where [orderID] = ''' + strOrderNO + ''' '''
    df = pd.read_sql_query(query, engine)
    AllBundle = list(df['bundleID'])
    plt.xlabel("Operation Number")
    plt.ylabel("Time in Hours")
    #print(AllBundle)
    HourInterval,minDate = OrderDateGetter(orderno)
    OperationInterval, MaxOps = OrderOperationGetter(orderno)

    plt.xticks(OperationInterval)
    plt.yticks(HourInterval)
    for index, bundle in enumerate(AllBundle):

        strBundle = "'" + bundle + "'"
        query = '''SELECT hourSlot
        ,[orderID]
        ,[bundleID]
        ,[opsequence]
        ,[operationAutoID]
        FROM [cfl_spts].[cfl_spts].[progresscomplete]
        where [orderID] = ''' + strOrderNO + ''' and [bundleID]= ''' + strBundle + '''
        order by [opsequence]'''
        df1 = pd.read_sql_query(query, engine)
        #print(df1)
        x = OperationConverter(list(df1['operationAutoID']),MaxOps)
        y = DateTimeConverter(list(df1['hourSlot']),minDate)
        #print('x',x)
        #print('y',y)
        smoothing(x, y,  2.5)
    #smoothing(xSum / entries, ySum / entries ,9.0)
    plt.gcf().set_size_inches(20, 15)
    plt.show()
try:
    plt.figure()
    orderno=18814
    order_plotter(orderno)

except Exception as e:
    #print('Exception Occured ' + str(e))
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #print(exc_type, fname, exc_tb.tb_lineno)

# connect database with pandas library

# clean the data using pandas

# make a linear regression model on y=target operational efficiency , X= SMV, quantity, loginTime

# look up the layouts of workers
