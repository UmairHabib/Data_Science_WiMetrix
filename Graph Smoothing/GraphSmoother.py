import sqlalchemy as db
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as plticker
import numpy as np
engine = db.create_engine(
    "mssql+pyodbc://dev:spts@3311@192.168.4.114,50003/cfl_spts?driver=SQL+Server+Native+Client+11.0")

def smoothing(x,y,width):
    poly=np.polyfit(x,y,5)
    poly_y = np.poly1d(poly)(x)
    plt.plot(x,poly_y,linewidth=width)


def order_plotter(orderno):
    strOrderNO = "'" + str(orderno) + "'"
    entries = 600
    query = '''SELECT top (600)[bundleID], count(*) as numOfOps
   FROM [cfl_spts].[cfl_spts].[progresscomplete]

   where [orderID] = ''' + strOrderNO + '''
   group by [bundleID]
   having count(*)=15
   order by  count(*) desc'''
    df = pd.read_sql_query(query, engine)
    print(df)
    AllBundle = list(df['bundleID'])
    print(AllBundle)
    xSum = 0
    ySum = 0

    for bundle in AllBundle:
        strBundle = "'" + bundle + "'"
        query = '''SELECT  hourSlot
             ,[orderID]
             ,[bundleID]
             ,[operationAutoID]
         FROM [cfl_spts].[cfl_spts].[progresscomplete]

         where [orderID] = ''' + strOrderNO + ''' and  [bundleID]= ''' + strBundle + '''
         order by hourSlot'''
        df1 = pd.read_sql_query(query, engine)
        x = mdates.date2num(df1["hourSlot"])
        y = df1[['operationAutoID']].index + 1
        xSum = x + xSum
        ySum = y + ySum
        smoothing(x, y, 2.5)

    ax = plt.gca()
    xfmt = mdates.DateFormatter('%Y-%m-%d %H:%M:%S')
    ax.xaxis.set_major_formatter(xfmt)
    plt.gcf().autofmt_xdate()
    plt.ylabel("Operation Number")
    plt.xlabel("DateTime")
    plt.tight_layout()
    plt.yticks(np.arange(1, 16))
    loc = plticker.MultipleLocator(base=0.25)  # this locator puts ticks at regular intervals
    ax.xaxis.set_major_locator(loc)
    plt.gcf().set_size_inches(30, 20)
    smoothing(xSum / entries ,ySum / entries,9.0)
    plt.show()


try:
    plt.figure()
    order_plotter(18814)
except Exception as e:
    print('Exception Occured ' + str(e))

# connect database with pandas library

# clean the data using pandas

# make a linear regression model on y=target operational efficiency , X= SMV, quantity, loginTime

# look up the layouts of workers
