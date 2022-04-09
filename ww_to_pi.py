"""     python ww_to_pi.py --conf=[json_conf_file] --startdate=[start_date] --enddate=[start_date] 

Where:

json_conf_file = JSON Configuration File (complete path)
start_date     = Start date of learning data (format: YYYY-MM-DD HH:mm:ss)
end_date       = End date of learning data (format: YYYY-MM-DD HH:mm:ss) """

# %%
import arrow
import argparse            # Construct the argument parser
import json
import logging
import numpy as np
import os
import pandas as pd
import pyodbc
import time
import warnings

warnings.filterwarnings('ignore')

from datetime import datetime
from humanfriendly import format_timespan
from sqlalchemy import create_engine
from tqdm.contrib.telegram import tqdm, trange

# %%
from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient
from osisoft.pidevclub.piwebapi.models import PIStreamValues, PITimedValue

from PiHelper import *

# %%
# Add the arguments to the parser
ap = argparse.ArgumentParser()
ap.add_argument("-c", "--conf", required=True, help="Configuration file (JSON format)")
ap.add_argument("-s", "--startdate", required=True, help="Start date data")
ap.add_argument("-e", "--enddate", required=True, help="End date data")
ap.add_argument("-d", "--days", required=True, help="Split start and end date into x days range")

args = vars(ap.parse_args())
fileConf  = str(args['conf'])
startDate = str(args['startdate'])
endDate = str(args['enddate'])
days = int(args['days'])

# %%
###################################################################################
## Open and read configuration file
###################################################################################
# Opening JSON file
print("Reading config file ...\n")

with open(fileConf, 'r') as f:
    # returns JSON object as  a dictionary 
    conf = json.load(f) 

server = conf['server_ww']
database = conf['database_ww']
username = conf['username_ww']
password = conf['password_ww']

piwebapi_url = conf['piwebapi_url']
piwebapi_username = conf['piwebapi_username']
piwebapi_password = conf['piwebapi_password']


# %%
#server = '192.168.5.191'
#database = 'Runtime'
#username = 'pi'
#password = '1m4dm1n'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
#cnxn = pyodbc.connect('DRIVER={SQL Server}; SERVER='+server+'; DATABASE='+database+'; UID='+username+'; PWD='+ password)
cursor = cnxn.cursor()

# %%
#piwebapi_url = 'https://192.168.5.74/piwebapi'
#client = PIWebApiClient(piwebapi_url, useKerberos=False, username="administrator", password="Spc12345", verifySsl=False) 
client = PIWebApiClient(piwebapi_url, useKerberos=False, username=piwebapi_username, password=piwebapi_password, verifySsl=False) 

piHelper = PiHelper(client)

# %%
# Read model configuration file
df_conf = pd.read_csv('agincourt_resources.csv')

# Filter model configuration based on object type
#object_type = "ObjectType=='Attribute'"
#df_conf = df_conf.query(object_type)
df_conf = df_conf.reset_index(drop=True)

# %%
# Create start date and end date table for datestamp selection
#start = arrow.get("20220101 00:00:00")
#end = arrow.get("20220315 23:59:59")

start = arrow.get(startDate)
end = arrow.get(endDate)
start_list = []
end_list = []

for r in arrow.Arrow.interval('days', start, end, interval=days):
    start_list.append(r[0].format('YYYYMMDD HH:mm:ss'))
    end_list.append(r[1].format('YYYYMMDD HH:mm:ss'))

df_date = pd.DataFrame(list(zip(start_list, end_list)), columns =['start_date', 'end_date'])
#df_date['start_date'] = pd.to_datetime(df_date['start_date'])
#df_date['end_date'] = pd.to_datetime(df_date['end_date'])

df_date = df_date.sort_values('start_date',ascending=False).reset_index().drop(columns=['index'])
# %%
# For test
#tagname = df_conf['Name'][2]            # tag_path = '\\\\PISERVER\\Database1\\PCS0210PN001_Crushing|MAR.M0210_FE001_MI_sPV'
#af_path = df_conf['AFPath'][2]
#startdate = df_date['start_date'][0] #"20220330 00:00:00"
#enddate = df_date['end_date'][0] # "20220315 23:59:59"
# %%
list_error_tag = []
list_success_tag = []

# Create directory to store logging, based on startdate
dirName = "migration_logs"

# Create target Directory if don't exist
if not os.path.exists(dirName):
    os.mkdir(dirName)

# Check file processed_tag, if exist, delete it
processed_tag_path = dirName+"/list_success_tag.csv"
if os.path.exists(processed_tag_path):
    os.remove(processed_tag_path)

# Send tqdm progress update to telegram
telegram_bot_token = '5287136655:AAEj6qGOblO7MmyqQ9G72XJnHXM7b3gYNTs'
telegram_chat_id = '30724671'

# Iterate through tagname rows
with tqdm(df_conf.iterrows(), total=df_conf.shape[0], ascii =" #", colour='green', token=telegram_bot_token, chat_id=telegram_chat_id) as t_tag:
    for index_tag, row_tag in t_tag:

        tagname = row_tag['Name']
        af_path = row_tag['AFPath']

        # Put tagname to progress bar
        t_tag.set_description('Processing ' + tagname + ' ...')

        # Init processing time measurement
        t0_total = time.time()

        filename = dirName+"/"+ tagname+'.log'
        logging.basicConfig(force=True, filename=filename, filemode='w', level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

        #with tqdm(df_date.iterrows(), total=df_date.shape[0], ascii=" *", colour='yellow', token=telegram_bot_token, chat_id=telegram_chat_id) as t_datetime:
        with tqdm(df_date.iterrows(), total=df_date.shape[0], ascii=" *", colour='yellow') as t_datetime:
            for index, row in t_datetime:
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
                    AND wwRetrievalMode = 'Delta' \
                    AND wwResolution = 1000 \
                    AND wwQualityRule = 'Extended' \
                    AND Quality = 0 \
                    AND wwVersion = 'Latest' \
                    AND DateTime >= @StartDate \
                    AND DateTime <= @EndDate) temp ".format(startdate, enddate, tagname)
                try:
                     # Put start time  to progress bar
                    t_datetime.set_description('Datetime:  ' + startdate + ' to ' + enddate)

                    # Get data from WW Historian
                    logging.info("Reading data {} from WW Historian ...;".format(tagname))
                    logging.info("From start time = {} to end time = {}".format(startdate, enddate))
                    t0 = time.time()
                    
                    # Execute SQL
                    df = pd.read_sql(sql, cnxn)
                    df['DateTime'] = pd.to_datetime(df['DateTime'])
                    t1 = time.time()
                    ww_query_time = t1-t0

                    ww_execution_time = format_timespan(ww_query_time, True)
                    logging.info("Execution time: {}".format(ww_execution_time))

                    # Length of database record
                    row_count = len(df)
                    logging.info("Row count = {}".format(row_count))
                    
                    # Init value
                    pi_insert_time = 0
                    str_response = ""

                    if (row_count > 0):
                        # Write data to PI tag
                        logging.info("Insert data to PI Data Archive {} ...".format(tagname))

                        t0 = time.time()
                        # Get values
                        #values = df['Value'].values.tolist()
                        values = df['Value']

                        # Get timestamp
                        #timestamps = df['DateTime'].dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
                        timestamps = df['DateTime']

                        response = piHelper.insertTimeSeriesValues(af_path, values, timestamps)
                        str_response = str(response)
                        t1 = time.time()
                        pi_insert_time = t1-t0

                        pi_execution_time = format_timespan(pi_insert_time, True)
                        logging.info("Execution time: {}".format(pi_execution_time))

                        # Log response
                        logging.info(str_response)
                    else:
                        logging.warning("No data for {} at {} to {}".format(tagname,startdate,enddate))
                    
                    logging.info("")

                    current_time = arrow.now().strftime("%Y-%m-%d %H:%M:%S")
                    dict_success = {"tagname":tagname, "start_date":startdate, "end_date":enddate, "created_date":current_time, \
                                    "ww_query_time":ww_query_time, "ww_query_time_str":ww_execution_time, "ww_query_record":row_count, \
                                    "pi_insert_time":pi_insert_time, "pi_insert_time_str":pi_execution_time, "response":str_response}
                    list_success_tag.append(dict_success)
                                
                except Exception as exception:
                    str_expt = str(exception)
                    # Create dictionary consist of tag, start_date, end_date, and created_date which has an error
                    current_time = arrow.now().strftime("%Y-%m-%d %H:%M:%S")
                    dict_error = {"tagname":tagname, "start_date":startdate, "end_date":enddate, "created_date":current_time, "error_msg":str_expt}
                    # Add to list of error
                    list_error_tag.append(dict_error)
                    logging.error("Error for {} at {} to {}".format(tagname,startdate,enddate))
                    logging.error(str_expt, exc_info=True) 

        t1_total = time.time()
        total_execution_time = format_timespan(t1_total - t0_total, True, max_units=3)
        logging.info("Total Execution time: {}".format(total_execution_time)) 

        # Create file to track tagnames that have been processed, so if the migration suddenly stop, 
        # and then restart again, we can skip processed tag.
        with open(dirName+'/processed_tag.txt', 'a') as f:
            f.write(tagname)
            f.write('\n')                   

# Create data frame list of tag success
df_success = pd.DataFrame(list_success_tag)
df_success.to_csv(processed_tag_path)

# Create data frame list of error
df_error = pd.DataFrame(list_error_tag)
# Export list of error as csv
if (len(df_error) > 0) :
    df_error.to_csv(dirName + "/list_of_error_tagname.csv")