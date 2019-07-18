import sys,os
import sqlalchemy as db
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math as m
import arrow
import datetime
import prettytable as pt
engine = db.create_engine(
    "mssql+pyodbc://dev:spts@3311@192.168.4.114,50003/cfl_spts?driver=SQL+Server+Native+Client+11.0")
Interval=26


def Query_Runner(query):
   df = pd.read_sql_query(query, engine)
   return df

def smoothing(x,y,width):
   poly=np.polyfit(x,y,3)
   poly_y = np.poly1d(poly)(x)
   plt.plot(x,poly_y,marker='o',linewidth=width)

def  OrderDateGetter(orderno):
   strOrderNO = "'" + str(orderno) + "'"
   query=''' select distinct p_date
   from cfl_spts.progresscomplete
   where [orderID] =  ''' + strOrderNO + '''
   order by p_date
    '''
   df = Query_Runner(query)
   dates=list(df['p_date'].values)
   HourTime = []
   sum = 0
   minDate = arrow.get(str(dates[0]))  # datetime.strptime(str(dates[0]), date_format)
   maxDate = arrow.get(str(dates[-1]))  # datetime.strptime(str(dates[-1]), date_format)
   delta = maxDate - minDate
   totaldays = delta.days + 1  # 1 is added to including last date
   totalhours = totaldays * 24
   Gap =totalhours/Interval
   Gap = m.ceil(Gap)
   if (totalhours < Interval):
       for i in range(1, totalhours + 1):
           HourTime.append(i)
   else:
       for i in range(0, Interval):
           sum = sum + Gap
           HourTime.append(sum)
       if (sum != totalhours):  # last index will contain remainder also
           HourTime[-1] += totalhours- sum
   return HourTime,dates[0]

def OrderOperationGetter(orderno):

   strOrderNO = "'" + str(orderno) + "'"
   query = '''  SELECT count(Distinct(operationAutoID)) as 'Counter'
     FROM [cfl_spts].[cfl_spts].[stylebulletin]
     WHERE opsequence IS NOT NULL AND [orderID]=''' + strOrderNO + ''' '''
   df = Query_Runner(query)
   MaxOps=int(df['Counter'])
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
   return arr


def OperationConverter(x,refTable):
   arr=[]
   for i in range(0, len(x)):
       index=refTable.index(x[i])
       if(index!= -1):
           arr.append(index+1)
       else:
           print("Operation Converter Function Error")
   return arr


def All_Bundle_Getter(orderno,entries):    #returns all bundle of an order
    strentries = str(entries)
    query = '''SELECT  DISTINCT TOP (''' + strentries + ''') [bundleID]
    FROM [cfl_spts].[cfl_spts].[progresscomplete]
    where [orderID] = ''' + orderno + ''' '''
    df = Query_Runner(query)
    AllBundle = list(df['bundleID'])
    return AllBundle


def Single_Bundle_Getter(orderno,bundleID):    #returns all bundle of an order
    query = '''SELECT [time],hourSlot
    ,[orderID]
    ,[bundleID]
    ,[SAM]
    ,[opsequence]
    ,[operationAutoID]
    FROM [cfl_spts].[cfl_spts].[progresscomplete]
    where [orderID] = ''' + orderno + ''' and [bundleID]= ''' + bundleID + '''
    order by [opsequence]'''
    df = Query_Runner(query)
    return df
def OperationAutoIDGetter(orderno):     #Sequence-wise all operation auto id of an order from style bulletin
    query1 = '''  SELECT operationAutoID as 'sortedOps'
          FROM [cfl_spts].[cfl_spts].[stylebulletin]
          WHERE opsequence IS NOT NULL AND [orderID]=''' + orderno + ''' order by opsequence'''
    df2 = Query_Runner(query1)
    refTable = list(df2['sortedOps'])
    return refTable
def BundleTimePlotter(orderno):
    strOrderNO = "'" + str(orderno) + "'"
    entries = 26  # top n entries
    AllBundle = All_Bundle_Getter(strOrderNO, entries)
    HourInterval, minDate = OrderDateGetter(orderno)
    plt.xlabel("Bundle Number")
    plt.ylabel("Time in Hours")
    plt.yticks(HourInterval)
    ySum = 0
    Histogram = []
    for index, bundle in enumerate(AllBundle):
        strBundle = "'" + bundle + "'"
        df1 = Single_Bundle_Getter(strOrderNO, strBundle)
        y = DateTimeConverter(list(df1['hourSlot']), minDate)
        ySum = ySum + (y[-1] - y[0])
        Histogram.append((y[-1] - y[0]))
    arr = [ySum / entries] * len(AllBundle)
    bundless = []
    for i in range(1, len(AllBundle) + 1):
        bundless.append(i)

    plt.scatter(bundless, Histogram)
    plt.xticks(bundless)
    plt.plot(bundless, arr, 9.0)
    plt.gcf().set_size_inches(20, 15)
    plt.axhspan(-50, 192, facecolor='green', alpha=0.3)
    plt.axhspan(192, 350, facecolor='yellow', alpha=0.3)
    plt.axhspan(350, 700, facecolor='red', alpha=0.4)
    plt.show()

def order_plotter(orderno):
   strOrderNO = "'" + str(orderno) + "'"
   entries = 26     # top n entries
   AllBundle = All_Bundle_Getter(strOrderNO,entries)
   refTable=OperationAutoIDGetter(strOrderNO)
   HourInterval,minDate = OrderDateGetter(orderno)
   OperationInterval, MaxOps = OrderOperationGetter(orderno)
   plt.xlabel("Bundle Number")
   plt.ylabel("Time in Hours")
   plt.yticks(HourInterval)
   plt.xticks(OperationInterval)
   plt.gcf().set_size_inches(20, 15)
   plt.axhspan(-50, 192, facecolor='green', alpha=0.3)
   plt.axhspan(192, 350, facecolor='yellow', alpha=0.3)
   plt.axhspan(350, 700, facecolor='red', alpha=0.4)
   xSum = [0] * MaxOps
   ySum = [0] * len(HourInterval)
   for index, bundle in enumerate(AllBundle):
       strBundle = "'" + bundle + "'"
       df1=Single_Bundle_Getter(strOrderNO,strBundle)
       x = OperationConverter(list(df1['operationAutoID']), refTable)
       y = DateTimeConverter(list(df1['hourSlot']),minDate)
       z=zip(x,xSum)
       zz=zip(y,ySum)
       xSum=[item2+item for item,item2 in z]
       ySum=[item2+item for item,item2 in zz]
       smoothing(x, y,  2.5)
   smoothing(np.array(xSum)/entries,np.array(ySum)/ySum , 9.0)
   plt.show()


def TodayDateConverter(arr):
    print('as')

def Today(orderno):
    strOrderNO = "'" + str(orderno) + "'"
    entries = 2  # top n entries
    AllBundle = All_Bundle_Getter(strOrderNO, entries)
    OperationInterval, MaxOps = OrderOperationGetter(orderno)
    refTable=OperationAutoIDGetter(strOrderNO)

    plt.xlabel("Operation Number")
    plt.ylabel("Time in Minutes")
    yticker=np.arange(0,501,5)
    plt.yticks(yticker)
    plt.xticks(OperationInterval)
    for index, bundle in enumerate(AllBundle):
        print(bundle)
        strBundle = "'" + bundle + "'"
        df1 = Single_Bundle_Getter(strOrderNO, strBundle)
        x = OperationConverter(list(df1['operationAutoID']), refTable)
        SAM = list(df1['SAM'])
        X= np.arange(1,MaxOps+1)
        list1=list(df1['time'])
        arr=[]
        diff_secs=0
        for i in range(0,len(list1)):
            if(i!=(len(list1)-1)):
                difference=list1[i+1]-list1[i]
                print(difference)
                diff_secs=difference.seconds
            if((i==(len(list1)-1)) or diff_secs >= 28800 or difference.days>0 or difference.days<0):
                arr.append(SAM[i])
                print('Sec ',diff_secs/60,'Sam ',SAM[i])
            else:
                arr.append(diff_secs/60)
                print('Difference ',diff_secs/60,'Sam ',SAM[i])
        plt.plot(x, arr)
        plt.plot(x,SAM,linewidth=9, linestyle="--", c="green")
    plt.gcf().set_size_inches(20, 15)
    plt.gca().legend(('Bundle', 'SAM'))
    plt.show()


try:
   plt.figure()
   orderno=18814
   # order_plotter(orderno)
   # BundleTimePlotter(orderno)
   Today(orderno)

except Exception as e:
   print('Exception Occured ' + str(e))
   exc_type, exc_obj, exc_tb = sys.exc_info()
   fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
   print(exc_type, fname, exc_tb.tb_lineno)



