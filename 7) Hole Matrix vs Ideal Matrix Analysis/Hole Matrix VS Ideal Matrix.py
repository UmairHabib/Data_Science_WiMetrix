import sys, os
import sqlalchemy as db
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import math as m
import arrow
import datetime, calendar
from random import randint
colors=[]

engine = db.create_engine(
   "mssql+pyodbc://dev:spts@3311@192.168.4.114,50003/cfl_spts?driver=SQL+Server+Native+Client+11.0")
Interval = 26
def Query_Runner(query):
   df = pd.read_sql_query(query, engine)
   return df
def smoothing(x, y, width):
   poly = np.polyfit(x, y, 3)
   poly_y = np.poly1d(poly)(x)
   plt.plot(x, poly_y, marker='o', linewidth=width)
def OrderDateGetter(orderno):
   query = ''' select distinct p_date
 from cfl_spts.progresscomplete
 where [orderID] =  ''' + orderno + '''
 order by p_date
  '''
   df = Query_Runner(query)
   dates = list(df['p_date'].values)
   HourTime = []
   sum = 0
   minDate = arrow.get(str(dates[0]))  # datetime.strptime(str(dates[0]), date_format)
   maxDate = arrow.get(str(dates[-1]))  # datetime.strptime(str(dates[-1]), date_format)
   delta = maxDate - minDate
   totaldays = delta.days + 1  # 1 is added to including last date
   totalhours = totaldays * 24
   Gap = totalhours / Interval
   Gap = m.ceil(Gap)
   if (totalhours < Interval):
       for i in range(1, totalhours + 1):
           HourTime.append(i)
   else:
       for i in range(0, Interval):
           sum = sum + Gap
           HourTime.append(sum)
       if (sum != totalhours):  # last index will contain remainder also
           HourTime[-1] += totalhours - sum
   return HourTime, dates[0]
def OrderOperationGetter(orderno):
   query = '''  SELECT count(Distinct(operationAutoID)) as 'Counter'
   FROM [cfl_spts].[cfl_spts].[stylebulletin]
   WHERE opsequence IS NOT NULL AND [orderID]=''' + orderno + ''' '''
   df = Query_Runner(query)
   MaxOps = int(df['Counter'])
   arr = []
   Gap = m.ceil(MaxOps / Interval)
   if (MaxOps < Interval):
       for i in range(1, MaxOps + 1):
           arr.append(i)
   else:
       for i in range(1, Interval + 1):
           arr.append(i + Gap)
   return arr, MaxOps
def DateTimeConverter(x, minDate):
   arr = []
   minDate = arrow.get(minDate)  # datetime.strptime(str(dates[-1]), date_format)
   for each in x:
       maxDate = arrow.get(each.date())  # datetime.strptime(str(dates[-1]), date_format)
       delta = maxDate - minDate
       totaldays = delta.days + 1  # 1 is added to including last date
       totalhours = totaldays * 24
       str1 = str(each.time())
       totalhours = totalhours + int(str1[0:2])
       arr.append(totalhours)
   return arr
def timeConverter(currentTime,endTime):   #overtime is taken as SAM
    if(len(endTime)==0):
        return -1           #for SAM

    else:
        endTime=endTime.iloc[0]
        minDate = arrow.get(currentTime)
        maxDate = arrow.get(endTime)  # datetime.strptime(str(dates[-1]), date_format)
        delta = maxDate - minDate
        minutes=delta.seconds/60
        if(minutes>540 or delta.days>0 or delta.days<0):
            return -1   #for SAM
        else:
            return minutes
def OperationConverter(x,  refTable):
   arr = []
   opsequence = list(refTable["opsequence"])
   autoid = list(refTable["sortedOps"])
   i =0
   while i != len(autoid):
       j = i+1
       arr1 =[]
       arr1.append(autoid[i])
       while j != len(autoid) and opsequence[i] == opsequence[j]:
           arr1.append(autoid[j])
           j = j+1
       i = j
       arr.append(arr1)
   i=0
   j=0
   arr3=[]
   while(j!= len(x) and i!=len(arr)):
       try:
           index = arr[i].index(x[j])
           j=j+1
           arr3.append(i+1)
       except:
           i=i+1
   return arr3
def All_Bundle_Getter(orderno, entries):  # returns all bundle of an order
   strentries = str(entries)
   query = '''SELECT  DISTINCT TOP (''' + strentries + ''') [bundleID]
   FROM [cfl_spts].[cfl_spts].[progresscomplete]
   where [orderID] = ''' + orderno + '''
  '''
   df = Query_Runner(query)
   AllBundle = list(df['bundleID'])
   return AllBundle
def Single_Bundle_Getter(orderno, bundleID):  # returns all bundle of an order
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
def Single_Bundle_Getter1(orderno, bundleID):  # returns all bundle of an order
   query = '''SELECT [time],hourSlot
  ,[orderID]
  ,[bundleID]
  ,[SAM]
  ,[opsequence]
  ,[operationAutoID]
  FROM [cfl_spts].[cfl_spts].[progresscomplete]
  where [orderID] = ''' + orderno + ''' and [bundleID]= ''' + bundleID + '''
  order by time'''
   df = Query_Runner(query)
   return df
def OperationAutoIDGetter(orderno):  # Sequence-wise all operation auto id of an order from style bulletin
   query1 = '''  SELECT operationAutoID as 'sortedOps', opsequence
        FROM [cfl_spts].[cfl_spts].[stylebulletin]
        WHERE opsequence IS NOT NULL AND [orderID]=''' + orderno + ''' order by opsequence'''
   df2 = Query_Runner(query1)
   return df2
def All_Worker_Getter(orderno):  # returns all worker of order
   query1 = '''SELECT distinct workerID
        ,[opsequence]
    FROM [cfl_spts].[cfl_spts].[progresscomplete]
    where orderid=''' + orderno + '''
    order by opsequence'''
   df2 = Query_Runner(query1)
   allWorker = list(df2['workerID'])
   return allWorker
def Single_Worker_Order_DateTime_Getter(orderno, workerId):  # this function will return all DateTime of single worker on an order
   query1 = '''SELECT [time], bundleID
    FROM [cfl_spts].[cfl_spts].[progresscomplete]
    where orderid=''' + orderno + ''' and workerID=''' + workerId + '''
    order by [time]'''
   df2 = Query_Runner(query1)
   timeOfBundles = list(df2['time'])
   bundleID = list(df2['bundleID'])
   return timeOfBundles, bundleID
def ideal_Matrix_Data_Getter(orderno):
    query1 = '''select t1.orderID, bundleID, operationAutoID, opsequence, SMV * t2.quantity as SAM
    from cfl_spts.orders as t1
    inner join cfl_spts.cutreport as t2 on t1.orderID = t2.orderID
    inner join cfl_spts.stylebulletin as t3 on t2.orderID = t3.orderID
    where t1.orderID = ''' + orderno + '''
    order by t2.bundleID, t3.opsequence
    '''
    df2 = Query_Runner(query1)
    return df2
def hole_Matrix_Data_Getter(orderno):
    query1 = '''SELECT  [time]
          ,[bundleID]
          ,[operationAutoID]
          ,[macID]
          ,[SAM]
      FROM [cfl_spts].[cfl_spts].[progresscomplete]
      where orderID=''' + orderno + '''
      order by [time]'''
    df2 = Query_Runner(query1)
    return df2
def timeRangeDataGetter(strInitial, strFinal,orderno):
   query = '''    SELECT  DISTINCT [bundleID],p_date
   FROM [cfl_spts].[cfl_spts].[progresscomplete]
   where [orderID] = ''' + orderno + ''' and p_date>= ''' + strInitial + ''' and p_date<= ''' + strFinal + '''
  order by p_date '''
   df = Query_Runner(query)
   AllBundle = list(df['bundleID'])
   return AllBundle
def BundleTimePlotter(orderno):
   entries = 26  # top n entries
   AllBundle = All_Bundle_Getter(orderno, entries)
   HourInterval, minDate = OrderDateGetter(orderno)
   plt.xlabel("Bundle Number")
   plt.ylabel("Time in Hours")
   plt.yticks(HourInterval)
   ySum = 0
   Histogram = []
   for index, bundle in enumerate(AllBundle):
       strBundle = "'" + bundle + "'"
       df1 = Single_Bundle_Getter(orderno, strBundle)
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
   entries = 26  # top n entries
   AllBundle = All_Bundle_Getter(orderno, entries)
   refTable = OperationAutoIDGetter(orderno)
   HourInterval, minDate = OrderDateGetter(orderno)
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
       df1 = Single_Bundle_Getter(orderno, strBundle)
       x = OperationConverter(list(df1['operationAutoID']), refTable)
       y = DateTimeConverter(list(df1['hourSlot']), minDate)
       z = zip(x, xSum)
       zz = zip(y, ySum)
       xSum = [item2 + item for item, item2 in z]
       ySum = [item2 + item for item, item2 in zz]
       smoothing(x, y, 2.5)
   smoothing(np.array(xSum) / entries, np.array(ySum) / ySum, 9.0)
   plt.show()
def Today(orderno):
   entries = 2  # top n entries
   AllBundle = All_Bundle_Getter(orderno, entries)
   OperationInterval, MaxOps = OrderOperationGetter(orderno)
   refTable = OperationAutoIDGetter(orderno)

   plt.xlabel("Operation Number")
   plt.ylabel("Time in Minutes")
   yticker = np.arange(0, 501, 5)
   plt.yticks(yticker)
   plt.xticks(OperationInterval)
   print('Op table ', refTable, 'len', len(refTable))
   for index, bundle in enumerate(AllBundle):
       print(bundle)
       bundle = str(53)
       strBundle = "'" + bundle + "'"
       df1 = Single_Bundle_Getter(orderno, strBundle)
       x = OperationConverter(list(df1['operationAutoID']), refTable)
       SAM = list(df1['SAM'])
       X = np.arange(1, MaxOps + 1)
       list1 = list(df1['time'])
       arr = []
       diff_secs = 0
       for i in range(0, len(list1)):
           print(i, ' ', len(list1))
           if (i != (len(list1) - 1)):
               difference = list1[i + 1] - list1[i]
               print(difference)
               diff_secs = difference.seconds
           if ((i == (len(list1) - 1)) or diff_secs >= 28800 or difference.days > 0 or difference.days < 0):
               arr.append(SAM[i])
               print('Sec ', diff_secs / 60, 'Sam ', SAM[i])
           else:
               arr.append(diff_secs / 60)
               print('Difference ', diff_secs / 60, 'Sam ', SAM[i])
       print(arr)
       print(x)
       plt.plot(x, arr)
       plt.plot(x, SAM, linewidth=9, linestyle="--", c="green")
   plt.gcf().set_size_inches(20, 15)
   plt.gca().legend(('Bundle', 'SAM'))
   plt.show()
def WorkerWiseBundleFlow(orderno):
   entries = 6  # top n entries
   AllBundle = All_Bundle_Getter(orderno, entries)
   OperationInterval, MaxOps = OrderOperationGetter(orderno)
   refTable = OperationAutoIDGetter(orderno)
   workerTable = All_Worker_Getter(orderno)
   print(len(workerTable))
   columnWiseTime = {}

   for i in workerTable:
       timeOfBundles, bundleID = Single_Worker_Order_DateTime_Getter(orderno, i)
       columnWiseTime.update({str(i): timeOfBundles})

   for i in dict.keys(columnWiseTime):
       print(i, '  ', columnWiseTime[i])

   plt.xlabel("Operation Number")
   plt.ylabel("Time in Minutes")
   yticker = np.arange(0, 501, 5)
   plt.yticks(yticker)
   plt.xticks(OperationInterval)
   for index, bundle in enumerate(AllBundle):
       print(bundle)
       strBundle = "'" + bundle + "'"
       df1 = Single_Bundle_Getter(orderno, strBundle)
       x = OperationConverter(list(df1['operationAutoID']), refTable)
       print(x)
       SAM = list(df1['SAM'])
       X = np.arange(1, MaxOps + 1)
       list1 = list(df1['time'])
       arr = []
       diff_secs = 0

       for i in range(0, len(list1)):

           if (i != (len(list1) - 1)):
               difference = list1[i + 1] - list1[i]
               print(difference)
               diff_secs = difference.seconds
           if ((i == (len(list1) - 1)) or diff_secs >= 28800 or difference.days > 0 or difference.days < 0):
               arr.append(SAM[i])
               print('Sec ', diff_secs / 60, 'Sam ', SAM[i])
           else:
               arr.append(diff_secs / 60)
               print('Difference ', diff_secs / 60, 'Sam ', SAM[i])
       plt.plot(x, arr)
       plt.plot(x, SAM, linewidth=5, linestyle="--", c="green")
   plt.gcf().set_size_inches(20, 15)
   plt.gca().legend(('Bundle', 'SAM'))
   plt.show()
def inductionProduction(orderno,initial,final):
   strInitial="'" + str(initial) + "'"
   strFinal="'" + str(final) + "'"
   AllBundle=timeRangeDataGetter(strInitial,strFinal,orderno)
   OperationInterval, MaxOps = OrderOperationGetter(orderno)
   refTable = OperationAutoIDGetter(orderno)
   plt.xlabel("Operation Number")
   plt.ylabel("Time in Minutes")
   yticker = np.arange(0, 501, 5)
   plt.yticks(yticker)
   plt.xticks(OperationInterval)
   X=[]
   SAM=[]
   for index, bundle in enumerate(AllBundle):
       print(bundle)
       strBundle = "'" + bundle + "'"
       df1 = Single_Bundle_Getter1(orderno, strBundle)
       x = OperationConverter(list(df1['operationAutoID']), refTable)
       print(x)
       SAM = list(df1['SAM'])
       print('SAM ',SAM)
       X = np.arange(1, len(x) + 1)
       print(df1[1,:])

   arr=[]
   plt.plot(X, arr)
   plt.plot(X, SAM, linewidth=9, linestyle="--", c="green")
   plt.gcf().set_size_inches(20, 15)
   plt.gca().legend(('Bundle', 'SAM'))
   plt.show()
def idealMatrix(orderno):
    print('Ideal Matrix')
    df=ideal_Matrix_Data_Getter(orderno)
    AllBundle=df['bundleID'].unique()
    allOperationAutoID=df['operationAutoID'].unique()
    matrixFrame = pd.DataFrame(columns=allOperationAutoID, index=AllBundle)
    OperationInterval, MaxOps = OrderOperationGetter(orderno)
    refTable = OperationAutoIDGetter(orderno)
    x = OperationConverter(list(df['operationAutoID']), refTable)
    for index, bundle in enumerate(AllBundle):
        bundleFrame=df[df["bundleID"]==bundle]
        y=list(bundleFrame['SAM'])
        matrixFrame.loc[bundle] = y
    return AllBundle,allOperationAutoID,OperationInterval,refTable,x, matrixFrame



def HoleMatrix(orderno):
    print('Hole Matrix')

    AllBundle, allOperationAutoID, OperationInterval, refTable, x, idealMatrixFrame=idealMatrix(orderno)
    matrixFrame = pd.DataFrame(columns=allOperationAutoID, index=AllBundle)
    plt.xlabel("Operation Number")
    plt.ylabel("Time in Minutes")
    plt.xticks(OperationInterval)
    df2=hole_Matrix_Data_Getter(orderno)
    for i in range(len(AllBundle)):
        colors.append('%06X'% randint(0,0xFFFFFF))
    for index, row in df2.iterrows():
        bundleId=row['bundleID']
        macId=row['macID']
        operationAutoId=row['operationAutoID']
        currentTime=row['time']
        filteredMachines=df2[df2['macID']==macId]     #filtered on same machine ID
        filteredTime=filteredMachines[filteredMachines['time']>currentTime]
        endtime=filteredTime['time'].head(1)
        y=timeConverter(currentTime,endtime)
        if y == -1:
             matrixFrame.set_value(bundleId,operationAutoId,row['SAM'])

        else:
            matrixFrame.set_value(bundleId, operationAutoId, y)

        yAxis=matrixFrame.loc[bundleId]

        #plt.plot(x, yAxis)

    bundles=5
    counter=0
    for index,row in matrixFrame.iterrows():
        if(counter<bundles and index != str(5004)  ):
            yAxis=matrixFrame.loc[index]
            plt.plot(x, yAxis,c='#'+colors[counter])
            counter=counter+1
            yAxis2 = idealMatrixFrame.loc[index]
            plt.plot(x, yAxis2,  linestyle="--",c='#'+colors[counter])
    plt.gcf().set_size_inches(20, 15)    #yticker = np.arange(-10.0,max(list(matrixFrame.max()))+2, (max(list(matrixFrame.max()))+2)/Interval)   # time divided into 26 intervals
    #plt.yticks(yticker)
    plt.show()




try:
   plt.figure()
   order = 18528
   orderno = "'" + str(order) + "'"
   # order_plotter(orderno)
   # BundleTimePlotter(orderno)
   # Today(orderno)
   #WorkerWiseBundleFlow(orderno)
   #inductionProduction(orderno,'2018-07-01','2018-07-13')
   #idealMatrix(orderno)
   HoleMatrix(orderno)




except Exception as e:
   print('Exception Occured ' + str(e))
   exc_type, exc_obj, exc_tb = sys.exc_info()
   fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
   print(exc_type, fname, exc_tb.tb_lineno)







