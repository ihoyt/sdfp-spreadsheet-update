from select import select
import pandas as pd
import numpy as np
import datetime
import warnings
import os
import json
import gspread
import statsmodels.api as sm
from sqlalchemy import create_engine
import pytz
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

def update_tracking_spreadsheet(data, flood_cutoff = 0):
    x=data.copy()
    
    # current_time = pd.Timestamp('now', tz= "UTC") + pd.offsets.Hour(-172) # 7 days + 4 hours
    current_time = pd.Timestamp('now', tz= "UTC") + pd.offsets.Hour(-4)
    
    flooding_measurements = x.reset_index().query("road_water_level_adj > @flood_cutoff").copy()
    
    n_flooding_measurements = flooding_measurements.shape[0]
  
    if(n_flooding_measurements == 0):
        return "No flooding to update spreadsheet"
    
    flooding_measurements = flooding_measurements.reset_index()
    flooding_measurements["min_date"] = flooding_measurements.date - datetime.timedelta(minutes = 1)
    flooding_measurements["max_date"] = flooding_measurements.date + datetime.timedelta(minutes = 1)
    
    flooding_measurements = flooding_measurements[["place", "sensor_ID", "date", "road_water_level_adj", "road_water_level", "voltage", "min_date", "max_date"]]
 
    # Download existing flood events from Google Sheets
    json_secret = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
    google_sheet_id = os.environ.get('GOOGLE_SHEET_ID')
    scope = ["https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict=json_secret, scopes=scope)

    gc = gspread.service_account_from_dict(json_secret)
    sh = gc.open_by_key(google_sheet_id)
    worksheet = sh.get_worksheet(0)
        
    sheet_data_df = pd.DataFrame(worksheet.get_all_records())

    min_dates = sheet_data_df.groupby(["place", "sensor_ID", "flood_event"])[["date"]].min() 
    max_dates = sheet_data_df.groupby(["place", "sensor_ID", "flood_event"])[["date"]].max()

    flood_start_stop = pd.merge(min_dates, max_dates, on = ["place", "sensor_ID","flood_event"])
    flood_start_stop["min_date"] = pd.to_datetime(flood_start_stop.date_x, utc=True) - datetime.timedelta(minutes = 30)
    flood_start_stop["max_date"] = pd.to_datetime(flood_start_stop.date_y, utc=True) + datetime.timedelta(minutes = 30)
    
    # Iterate through each place, compare overlap of each flood event in our new data and the existing data in the spreadsheet
    # If there is no overlap, collect the flood event data to then write to spreadsheet
    places = list(flooding_measurements["place"].unique())
    
    new_site_data_df = pd.DataFrame()
    
    for selected_place in places:
        print(selected_place)
        site_data = flooding_measurements.query("place == @selected_place").copy()
        site_existing_data = flood_start_stop.query("place == @selected_place").copy().reset_index()
        
        last_flood_number = pd.to_numeric(site_existing_data.flood_event).max()
        if (pd.isna(last_flood_number)):
            last_flood_number = 0
            
        site_data["flood_event"] = flood_counter(site_data.date, start_number = 0, lag_hrs = 2)
        
        flood_events_occuring = site_data.groupby("flood_event").max_date.max() > current_time
        flood_events_occuring = flood_events_occuring.reset_index()
        flood_events_to_select = flood_events_occuring[flood_events_occuring.max_date == False].flood_event.tolist()
        
        site_data = site_data.query("flood_event in @flood_events_to_select")

        site_min_dates = site_data.groupby(["flood_event"])[["date"]].min() 
        site_max_dates = site_data.groupby(["flood_event"])[["date"]].max() 
        site_flood_start_stop = pd.merge(site_min_dates, site_max_dates, on = ["flood_event"])
        site_flood_start_stop["min_date"] = pd.to_datetime(site_flood_start_stop.date_x) 
        site_flood_start_stop["max_date"] = pd.to_datetime(site_flood_start_stop.date_y)
        
        site_keep_list = list()
        
        for (i, v) in site_flood_start_stop.iterrows():
            # need to collect values to keep to track when there is overlap. keep = not overlap
            internal_overlap_list = list()
            
            # create an interval using the new data (from the row being iterated on)
            new_interval = pd.Interval(v.min_date, v.max_date)
            
            # for each row in the existing data, check for overlap with our new data
            for (existing_i, existing_v) in site_existing_data.iterrows():
                
                existing_interval = pd.Interval(existing_v.min_date, existing_v.max_date)
                overlaps = new_interval.overlaps(existing_interval)
                internal_overlap_list.append(overlaps)
            
            if sum(internal_overlap_list) > 0:
                site_keep = False
            else:
                site_keep = True
            
            site_keep_list.append(site_keep)
            
        if sum(site_keep_list) == 0:
            print("No new flood events")
            pass
        
        new_flood_events = site_flood_start_stop[site_keep_list].reset_index()
        
        new_site_data = site_data.query("flood_event in @new_flood_events.flood_event")
        new_site_data.flood_event = flood_counter(new_site_data.date, start_number = last_flood_number, lag_hrs = 2)
        new_site_data["drift"] = new_site_data.road_water_level - new_site_data.road_water_level_adj
        new_site_data = new_site_data.loc[:,['place','sensor_ID','flood_event', 'date', 'road_water_level_adj', 'road_water_level', 'drift', 'voltage']]
        new_site_data_df = pd.concat([new_site_data_df,new_site_data])
    
    if (new_site_data_df.size == 0):
        print("No new flood events to write to spreadsheet")
        return

    # Get pictures that align
    # new_site_data_df_w_pics = get_pictures_for_flooding(new_site_data_df)

    new_site_data_df['pic_links'] = ''
    new_site_data_df['date_added'] = pd.to_datetime(datetime.datetime.utcnow())
    
    # Convert full df of new flood events to string so we can write them to a google spreadsheet
    # new_site_data_df_w_pics = new_site_data_df_w_pics.astype('str')
    new_site_data_df = new_site_data_df.astype('str')
    
    # Append new values the google sheet!
    try:
        # write_to_sheet = worksheet.append_rows(values = new_site_data_df_w_pics.values.tolist(), value_input_option="USER_ENTERED")
        write_to_sheet = worksheet.append_rows(values = new_site_data_df.values.tolist(), value_input_option="USER_ENTERED")
        print("Wrote new flood events to spreadsheet")
    except:
        print("Whoops! An error writing flood events to spreadsheet")
        
    return

def get_pictures_for_flooding(data):
    json_secret = json.loads(os.environ.get('GOOGLE_JSON_KEY'))
    google_drive_folder_id = os.environ.get('GOOGLE_DRIVE_FOLDER_ID')
    scope = ["https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict=json_secret, scopes=scope)

    drive = build('drive', 'v3', credentials=credentials)
    
    images_folder_id = os.environ.get('GOOGLE_IMAGES_ID')
    
    x = data.copy()
    sensor_ids = x["sensor_ID"].unique().tolist()
    x_cols = x.columns.tolist()
    x_cols.append("pic_links")
    
    x["day"] = x.date.dt.strftime("%Y-%m-%d")
    
    rows_with_pics = pd.DataFrame()
    
    for selected_sensor_id in sensor_ids:
        selected_sensor_data = x.query("sensor_ID == @selected_sensor_id").copy()
        days_of_flooding = selected_sensor_data.day.unique().tolist()
        
        # Search for the camera's folder within
        camera_image_folder_info = drive.files().list(
            corpora="drive",
            driveId=google_drive_folder_id,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            q="name='CAM_" + selected_sensor_id + "' and mimeType='application/vnd.google-apps.folder' and '" + images_folder_id + "' in parents and trashed = false"
        ).execute().get('files', [])
        
        if len(camera_image_folder_info) > 0:
            camera_image_folder_id = camera_image_folder_info[0].get('id')
            
            for day in days_of_flooding:
                
                selected_day_data = selected_sensor_data.query("day == @day").copy()
                
                # Within the camera's folder, see if there is a folder for the specific date of interest (date_label)
                date_folder_info = drive.files().list(
                    corpora="drive",
                    driveId=google_drive_folder_id,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    q="'" + camera_image_folder_id + "'" + " in parents and trashed = false and name='" + day + "' and mimeType='application/vnd.google-apps.folder'"
                ).execute().get('files', [])

                # If there is a folder for the date within the camera's folder, get the ID
                if len(date_folder_info) > 0:
                    date_folder_id = date_folder_info[0].get('id')
                    
                    picture_info = pd.DataFrame(drive.files().list(
                        corpora="drive",
                        driveId=google_drive_folder_id,
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                        q="'" + date_folder_id + "'" + " in parents and trashed = false"
                    ).execute().get('files', []))
                    
                    picture_info["time"] = picture_info.name.apply(lambda x: pd.Interval(pd.to_datetime(x.split("_")[-1].split(".")[0], utc=True),pd.to_datetime(x.split("_")[-1].split(".")[0], utc=True)))
                    selected_day_data["interval"] = selected_day_data.date.apply(lambda x: pd.Interval(x - datetime.timedelta(minutes=5), x + datetime.timedelta(minutes=5)))
                    
                    selected_day_data["pic_links"] = pd.NA
                    
                    for (i, v) in selected_day_data.iterrows():
                        overlaps = picture_info.time.apply(lambda x: v.interval.overlaps(x))
                        
                        if sum(overlaps > 0):
                            selected_day_data.loc[i, "pic_links"] = "https://drive.google.com/open?id=" + str(picture_info.id[overlaps].values.tolist()[0]) 
                    
                rows_with_pics = pd.concat([rows_with_pics, selected_day_data])
        else:
            print("No camera folder for this site: CAM_" + selected_sensor_id)
        
    return x.merge(rows_with_pics.loc[:,["place","sensor_ID","date","pic_links"]], on = ["place","sensor_ID","date"], how="left").loc[:,x_cols]
        
    
def flood_counter(dates, start_number = 0, lag_hrs = 8):
    dates = dates.copy().reset_index().date
    lagged_time = dates - dates.shift(1)
    lagged_time = lagged_time.fillna(pd.Timedelta('0 days'))
    
    group_change_vector = list()
    
    for i,v in enumerate(dates):
        x = 0

        if abs(lagged_time[i]) > datetime.timedelta(hours = lag_hrs):
            x = 1
    
        group_change_vector.append(x)
    
    group_vector = np.cumsum(group_change_vector) + 1 + start_number
    
    return group_vector



def main():

    ########################
    # Establish DB engine  #
    ########################

    SQLALCHEMY_DATABASE_URL = "postgresql://" + os.environ.get('POSTGRESQL_USER') + ":" + os.environ.get(
        'POSTGRESQL_PASSWORD') + "@" + os.environ.get('POSTGRESQL_HOSTNAME') + "/" + os.environ.get('POSTGRESQL_DATABASE')

    engine = create_engine(SQLALCHEMY_DATABASE_URL)

    #####################
    # Process data  #
    #####################

    end_date = pd.to_datetime(datetime.datetime.utcnow())
    start_date = end_date - datetime.timedelta(days=21)
    # query = f"SELECT * FROM data_for_display WHERE date >= '2023-10-01' AND date <= '2023-11-01'"
    query = f"SELECT * FROM data_for_display WHERE date >= '{start_date}' AND date <= '{end_date}'"
    print(query)
    drift_corrected_df = pd.read_sql_query(query, engine).sort_values(['place','date']).drop_duplicates()

    #######################################
    #  Update flood tracking spreadsheet  #
    #######################################
    
    update_tracking_spreadsheet(data = drift_corrected_df, flood_cutoff = 0)
    
    #############################
    # Cleanup the DB connection #
    #############################
    
    engine.dispose()
    
    # Create requirements.txt using this commange on local machine - "pip list --format=freeze > requirements.txt"

if __name__ == "__main__":
    main()