# %%
import arrow
import argparse            # Construct the argument parser
import logging
import numpy as np
import pandas as pd
import pyodbc
import time
import warnings

warnings.filterwarnings('ignore')

from datetime import datetime
from humanfriendly import format_timespan
from sqlalchemy import create_engine
from tqdm import tqdm

# %%
from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient
from osisoft.pidevclub.piwebapi.models import PIStreamValues, PITimedValue

from PiHelper import *

# %%
server = '192.168.5.191'
database = 'Runtime'
username = 'pi'
password = '1m4dm1n'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

# %%
piwebapi_url = 'https://192.168.5.74/piwebapi'
client = PIWebApiClient(piwebapi_url, useKerberos=False, username="administrator", password="Spc12345", verifySsl=False) 
piHelper = PiHelper(client)

# %%
# Read model configuration file
df_conf = pd.read_csv('AF_PTAR.csv')

# Filter model configuration based on object type
object_type = "ObjectType=='Attribute'"
df_conf = df_conf.query(object_type)

# %%
# Create start date and end date table for datestamp selection
start = arrow.get("20190701 00:00:00")
end = arrow.get("20220315 23:59:59")
start_list = []
end_list = []

for r in arrow.Arrow.span_range('days', start, end):
    sdate = r[0].floor('day').format('YYYYMMDD HH:mm:ss')
    start_list.append(sdate)
    edate = r[1].floor('second').shift(days=+7).format('YYYYMMDD HH:mm:ss')
    end_list.append(edate)

df_date = pd.DataFrame(list(zip(start_list, end_list)), columns =['start_date', 'end_date'])
#df_date['start_date'] = pd.to_datetime(df_date['start_date'])
#df_date['end_date'] = pd.to_datetime(df_date['end_date'])

l = list(np.arange(0,len(df_date),8))
df_date = df_date.iloc[l].sort_values('start_date',ascending=False).reset_index()
df_date = df_date.drop(columns=['index'])

# %%
# For test
tagname = df_conf['Name'][1]            # tag_path = '\\\\PISERVER\\Database1\\PCS0210PN001_Crushing|MAR.M0210_FE001_MI_sPV'
af_path = df_conf['AFPath'][1]
#startdate = df_date['start_date'][0] #"20220330 00:00:00"
#enddate = df_date['end_date'][0] # "20220315 23:59:59"
logging.basicConfig(filename=tagname+'.log', filemode='a', level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

# %%
t0_total = time.time()
# Iterate through rows
for index, row in tqdm(df_date.iterrows(), total=df_date.shape[0]):
     # Create start date, and end date for data training
    startdate = row['start_date']
    enddate = row['end_date']

    # Query to WW
    sql = "SET NOCOUNT ON \
    DECLARE @StartDate DateTime \
    DECLARE @EndDate DateTime \
    SET @StartDate = '{}' \
    SET @EndDate = '{}' \
    SET NOCOUNT OFF \
    SELECT  * FROM ( \
    SELECT DateTime, Value \
         FROM History \
         WHERE History.TagName = '{}' \
         AND wwRetrievalMode = 'Cyclic' \
         AND wwResolution = 1000 \
         AND wwQualityRule = 'Extended' \
         AND Quality = 0 \
         AND wwVersion = 'Latest' \
         AND DateTime >= @StartDate \
         AND DateTime <= @EndDate) temp ".format(startdate, enddate, tagname)
    try:
        # Get data from WW Historian
        logging.info("Reading data {} from WW Historian".format(tagname))
        logging.info("From start time = {} to end time = {}".format(startdate, enddate))
        t0 = time.time()
        
        # Execute SQL
        df = pd.read_sql(sql, cnxn)
        t1 = time.time()
        
        execution_time = t1 - t0
        logging.info("Execution time: {}".format(format_timespan(execution_time, True)))

        # Length of database record
        logging.info("Row count = {}".format(len(df)))
        
        if (len(df) > 0):
            # Get values
            values = df['Value'].values.tolist()

            # Get timestamp
            timestamps = df['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S").tolist()

            # Write data to PI tag
            logging.info("Insert data to PI Data Archive {}".format(tagname))
            t0 = time.time()
            response = piHelper.insertTimeSeriesValues(af_path, values, timestamps)
            t1 = time.time()
            execution_time = t1 - t0
            logging.info("Execution time: {}".format(format_timespan(execution_time, True)))

            # Log response
            logging.info(str(response))
        else:
            logging.warning("No data for {} at {} to {}".format(tagname,startdate,enddate))
        
        logging.info("")
                     
    except Exception as exception:
        logging.error("Error for {} at {} to {}".format(tagname,startdate,enddate))
        logging.error(str(exception), exc_info=True) 

t1_total = time.time()
total_execution_time = t1_total - t0_total
logging.info("Total execution time: {}".format(format_timespan(total_execution_time, True)))                     

