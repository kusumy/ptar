
import datetime
import dateutil.tz as tz
import pandas as pd
import arrow

from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient
from osisoft.pidevclub.piwebapi.models import PIStreamValues, PITimedValue
from functools import reduce

class PiHelper:
    def __init__(self, pi_client):
        self.pi_client = pi_client
    

    
    def character_indexes(self, string, match):
        #string = "Hello World! This is an example sentence with no meaning."
        #match = "i"
        return [index for index, character in enumerate(string) if character == match]
    
    
    def getRecordedData(self, path, start_time, end_time, timezone = 'Asia/Jakarta'):
        try:
            dfx = self.pi_client.data.get_recorded_values(path, start_time=start_time, end_time=end_time, max_count=10000)
            dfx['Timestamp'] = pd.to_datetime(dfx['Timestamp'])
            dfx['Value'] = pd.to_numeric(dfx['Value'], errors='coerce')
            dfx.set_index('Timestamp', inplace=True)
            dfx = dfx.tz_convert(tz=timezone)
            #dfx = dfx.reset_index()
        except:
            dfx = None
        return dfx
    
    def getInterpolatedData(self, path, start_time, end_time, interval, timezone = 'Asia/Jakarta'):
        try:
            dfx = self.pi_client.data.get_interpolated_values(path, start_time=start_time, end_time=end_time, interval=interval)
            dfx['Timestamp'] = pd.to_datetime(dfx['Timestamp'], utc=True)
            dfx['Value'] = pd.to_numeric(dfx['Value'], errors='coerce')
            dfx = dfx.set_index('Timestamp')
            dfx = dfx.tz_convert(timezone)
            #dfx = dfx.reset_index()
        except:
            # Set dfx to Null or Nan
            dfx = None
        return dfx
    
    
    def getMultipleInterpolatedData(self, paths, start_time, end_time, interval, timezone = 'Asia/Jakarta'):

        # Loop through all path
        list_df = []
        for i in paths:  
            #print('af:'+i)
            dfx = self.pi_client.data.get_interpolated_values(i, start_time=start_time, end_time=end_time, interval=interval)
            dfx['Timestamp'] = pd.to_datetime(dfx['Timestamp'], utc=True)
            dfx['Value'] = pd.to_numeric(dfx['Value'], errors='coerce')

            # Only get value and timestamp column
            dfx = dfx[['Timestamp', 'Value']].copy()

            # Get attribute name from path
            list_pos = self.character_indexes(i, "\\")

            # Get last position of "\\" character in string and add with 1 (because the indexes of character start with 0)
            last_post = list_pos[-1] + 1

            # Get attribute name and replace '|' with underscore
            attr_name = i[last_post:]
            #attr_name = attr_name.replace("|","_")       

            # Rename value column to attribute name
            dfx = dfx.rename(columns={"Value":attr_name})

            # Add dataframe to list dataframe
            list_df.append(dfx)

            # Merge dataframes based on timestamp
            df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['Timestamp']), list_df)

            df_merged = df_merged.set_index('Timestamp')
            df_merged = df_merged.tz_convert(timezone)
            #df_merged = df_merged.reset_index()
            
        return df_merged


    # Set attribute value
    def setValue(self, path, value, timestamp):
        try:
            #webid_next_maint = self.pi_client.attribute.get_by_path(path).web_id
            point1 = self.pi_client.point.get_by_path(path)
            # Update value
            val = PITimedValue()
            val.good = True
            val.value = value
            val.timestamp = self.convertToUTC(timestamp);
            self.pi_client.stream.update_value(point1.web_id, val, update_option="Replace")
        except:
            pass


    def setValues(self, path, values, timestamps):
        for i, val in enumerate(values):
            # Update value
            self.setValue(path, val, timestamps[i])


    # %% Insert timeseries values
    def insertTimeSeriesValues(self, path, values, timestamps, update_option="Replace"):
        web_id = self.pi_client.attribute.get_by_path(path).web_id
        streamValues = PIStreamValues()
        pi_values = list()

        for i, val in enumerate(values):
            pi_val = PITimedValue()
            pi_val.value, pi_val.timestamp, pi_val.good = val, self.convertToUTC(timestamps[i]), True
            #pi_val.timestamp = self.convertToUTC(timestamps[i])
            #pi_val.good = True
            pi_values.append(pi_val)
        
        #pi_values = []

        streamValues.web_id = web_id
        streamValues.items = pi_values
        listStreamValues = list()
        listStreamValues.append(streamValues)
        response = self.pi_client.streamSet.update_values_ad_hoc_with_http_info(listStreamValues, update_option=update_option)

        return response

    def convertToUTC(self, datestamp):
    
        # Auto-detect zones:
        utc_zone = tz.tzutc()
        local_zone = tz.tzlocal()

        # Convert time string to datetime
        local_time = datetime.datetime.strptime(datestamp, '%Y-%m-%d %H:%M:%S')

        # Tell the datetime object that it's in local time zone since 
        # datetime objects are 'naive' by default
        local_time = local_time.replace(tzinfo=local_zone)
        # Convert time to UTC
        utc_time = local_time.astimezone(utc_zone)
        # Generate UTC time string
        utc_string = utc_time.strftime('%Y-%m-%dT%H:%M:%SZ')

        return utc_string

    def add_minutes(self, start_date, minutes = 0):
        start_date = str(start_date)
        st = arrow.get(start_date, 'YYYY-MM-DD HH:mm:ss')
        end_date = st.shift(minutes=minutes).strftime('%Y-%m-%d %H:%M:%S')
        return end_date
    
    def add_seconds(self, start_date, seconds = 0):
        start_date = str(start_date)
        st = arrow.get(start_date, 'YYYY-MM-DD HH:mm:ss')
        end_date = st.shift(seconds=seconds).strftime('%Y-%m-%d %H:%M:%S')
        return end_date
    
    def add_hours(self, start_date, hours = 0):
        start_date = str(start_date)
        st = arrow.get(start_date, 'YYYY-MM-DD HH:mm:ss')
        end_date = st.shift(hours=hours).strftime('%Y-%m-%d %H:%M:%S')
        return end_date
    
    def add_days(self, start_date, days = 0):
        start_date = str(start_date)
        st = arrow.get(start_date, 'YYYY-MM-DD HH:mm:ss')
        end_date = st.shift(days=days).strftime('%Y-%m-%d %H:%M:%S')
        return end_date
    
    def create_dates(self, start, end, freq):
        v = pd.date_range(start=start, end=end, freq=freq, closed=None)
        datetime_forecast = pd.DataFrame(index=v)
        return datetime_forecast
        