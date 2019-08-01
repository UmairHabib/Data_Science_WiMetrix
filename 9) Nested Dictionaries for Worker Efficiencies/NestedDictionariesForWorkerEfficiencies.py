import sys, os
import sqlalchemy as db
import pandas as pd
import numpy as np
import csv
import arrow
colors=[]
engine = db.create_engine("mssql+pyodbc://sa:123456@localhost/us2_spts?driver=SQL+Server+Native+Client+11.0")
Interval = 26
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
   query1 = ''' SELECT  [time]
         ,[bundleID]
         ,[operationAutoID]
         ,[macID]
         ,[workerID]
         ,[SAM]
     FROM us2_spts.dbo.[progresscomplete]
     where orderID=''' + orderno + '''
     order by [time]'''
   df2 = Query_Runner(query1)
   return df2
def OperationAutoIDGetter(orderno):  # Sequence-wise all operation auto id of an order from style bulletin
   query1 = '''  SELECT operationAutoID as 'sortedOps', opsequence, styleID
       FROM [us2_spts].[wim_spts].[stylebulletin]
       where [orderID]=''' + orderno + ''' order by opsequence'''
   df2 = Query_Runner(query1)
   return df2
def allOperationAutoIDGetter():
 query="Select distinct operationautoid from [us2_spts].[wim_spts].stylebulletin"
 df= Query_Runner(query)
 return df["operationautoid"]
def fault_operation_getter():
   query=" SELECT* from [us2_spts].[traffic].[Temp]"
   return Query_Runner(query)
def Query_Runner(query):
   df = pd.read_sql_query(query, engine)
   return df
def timeConverter(currentTime,endTime):   #overtime is taken as SAM
   minDate = arrow.get(currentTime)
   maxDate = arrow.get(endTime)  # datetime.strptime(str(dates[-1]), date_format)
   delta = maxDate - minDate
   minutes=delta.seconds/60
   if(minutes>540 or delta.days>0 or delta.days<0):
       return -1   #for SAM
   else:
       return minutes
def differenceColumn(df):
   allTime=list(df['time'])
   arr=[]
   for index in range(0,len(allTime)-1):
       y=timeConverter(allTime[index],allTime[index+1])
       if y==-1:
           row=df.iloc[index]
           arr.append(row['SAM'])
       else:
           arr.append(y)
   if(len(allTime)!=0):
       row = df.iloc[len(allTime)-1]
       arr.append(row['SAM'])
   return arr
def MedianTaker(allOperationAutoID , df2,df):
   actual={}
   ideal={}
   efficiency={}
   for index,each in enumerate(allOperationAutoID):
       x=df2[df2['operationAutoID']==each]
       y=df[df["operationAutoID"]==each]
       if(len(x)!=0):
           actual.update({each:(sum(x['timeDifference']))})
           ideal.update({each:(sum(x['SAM']))})
           if len(y)>0:
            efficiency = {key: np.array([ideal[key] - actual.get(key, "#"), y["numOfFaults"].iloc[0]]) for key in ideal.keys()}
           else:
            efficiency = {key: np.array([ideal[key] - actual.get(key, "#"), 0.0]) for key in
                             ideal.keys()}
   return efficiency,ideal,actual
def workerMatrix(orderno,faultsFrame):
   print('Worker Matrix',orderno)
   df2=hole_Matrix_Data_Getter(orderno)
   df=OperationAutoIDGetter(orderno)
   workerEfficiency={}
   idealSam = {}
   actualSam = {}
   workers=df2['workerID'].unique()
   for workerID in workers:
       df3=df2[df2['workerID'] == workerID]
       df3['timeDifference'] = differenceColumn(df3)
       Eff,ideal,actual=MedianTaker(df['sortedOps'].values , df3,faultsFrame[faultsFrame["workerID"]==workerID])
       workerEfficiency.update({workerID:Eff})
       idealSam.update({workerID:ideal})
       actualSam.update({workerID:actual})
   return workerEfficiency,idealSam,actualSam
def efficiencyMatrix():
    #allOrderList=list(allOrderFrame['orderID'].values)
    allOrderList=['1120180661/8','1120180939/6','1120180940/6','1120180940/7','1120180940/8']
    allWorkerEfficiencies={}
    allIdealSam={}
    allActualSam={}
    alloperations=list(allOperationAutoIDGetter().values)
    alloperations.insert(0, "workerID")
    faultsFrame=fault_operation_getter()
    for order in allOrderList:
        orderno = "'" + str(order) + "'"
        efficiency,ideal,actual=workerMatrix(orderno, faultsFrame[faultsFrame["orderID"]==order])
        for each in efficiency.keys():
            if each in allWorkerEfficiencies.keys():                                                            #means that element is already added so we have to sum up
                allWorkerEfficiencies.update({each:{key: allWorkerEfficiencies[each].get(key, 0) + efficiency[each].get(key, 0)   #adding corresponding worker efficiency
                                         for key in set(allWorkerEfficiencies[each]) | set(efficiency[each])}})
                allIdealSam.update({each:{key: allIdealSam[each].get(key, 0) + ideal[each].get(key, 0)   #adding corresponding worker efficiency
                                         for key in set(allIdealSam[each]) | set(ideal[each])}})
                allActualSam.update({each:{key: allActualSam[each].get(key, 0) + actual[each].get(key, 0)   #adding corresponding worker efficiency
                                         for key in set(allActualSam[each]) | set(actual[each])}})
            else:
                allWorkerEfficiencies.update({each:efficiency.get(each, 'Not Found')})    #adding worker efficiency in bigger adjacency list
                allIdealSam.update({each:ideal.get(each, 'Not Found')})
                allActualSam.update({each:actual.get(each, 'Not Found')})

    print('Ideal ',len(allIdealSam),allIdealSam)
    print('Actual ',len(allActualSam),allActualSam)
    print('Efficiency ',len(allWorkerEfficiencies),allWorkerEfficiencies)
    with open(r'C:\Users\Lucky\Desktop\employee_efficiency.csv', "w") as f:
        w = csv.DictWriter(f, alloperations)
        w.writeheader()
        for key, val in (allWorkerEfficiencies.items()):
            row = {'workerID': key}
            row.update(val)
            w.writerow(row)
try:
  efficiencyMatrix()

except Exception as e:
  print('Exception Occured ' + str(e))
  exc_type, exc_obj, exc_tb = sys.exc_info()
  fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
  print(exc_type, fname, exc_tb.tb_lineno)



# select t2.*, t3.*
#  INTO [us2_spts].[traffic].[Temp]
#  from traffic.qualitylog as t1
#  inner join (
#   SELECT pC.workerID, max(qualityLogID) as qualityLogID,
#       [qualityFault_operationID]
#       ,sum([defectsNo]) as numOfFaults
#   FROM [us2_spts].[traffic].[qualitylog] as qL
#   LEFT JOIN us2_spts.dbo.[progresscomplete] as pC
#   ON qL.qualityFault_operationID = pC.styleID and qL.itemID=pC.itemID
#   where qL.qualityFaultID < 1000 and qL.rework = 0
#   group by workerID, [qualityFault_operationID]
# ) as t2 on t1.qualityLogID = t2.qualityLogID
# inner join wim_spts.stylebulletin as t3 on t1.qualityFault_operationID = t3.styleID
# where t2.workerID IS NOT NULL
#  --having qL.qualityFaultID < 1000
















