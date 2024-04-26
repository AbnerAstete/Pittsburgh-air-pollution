import os, pytz
from datetime import datetime
  
import pandas as pd
import numpy as np

DATA_DIR_ESDR = 'data/esdr/'
DATA_DIR_ESDR_CLEAN = 'data/esdr/clean/'
DATA_DIR_SMELL_REPORT = 'data/smell_report/'

def clean_esdr_1():
    Feed_1_Avalon_ACHD_PM25 = pd.read_csv(DATA_DIR_ESDR + 'Feed_1_Avalon_ACHD_PM25.csv')
    Feed_1_Avalon_ACHD_PM25 = Feed_1_Avalon_ACHD_PM25.rename(columns={'3.feed_1.PM25B_UG_M3..3.feed_1.PM25T_UG_M3..3.feed_1.PM25_640_UG_M3':'PM25B_UG_M3.PM25T_UG_M3.PM25_640_UG_M3'})


    Feed_1_Avalon_ACHD = pd.read_csv(DATA_DIR_ESDR + 'Feed_1_Avalon_ACHD.csv')
    Feed_1_Avalon_ACHD = Feed_1_Avalon_ACHD.rename(columns={'3.feed_1.SO2_PPM': 'SO2_PPM','3.feed_1.H2S_PPM':'H2S_PPM','3.feed_1.SIGTHETA_DEG':'SIGTHETA_DEG','3.feed_1.SONICWD_DEG':'SONICWD_DEG','3.feed_1.SONICWS_MPH':'SONICWS_MPH'})


    esdr_1 = pd.merge(Feed_1_Avalon_ACHD_PM25, Feed_1_Avalon_ACHD, on='EpochTime', how='left')
    esdr_1['datetime'] = pd.to_datetime(esdr_1['EpochTime'], unit='s')
    esdr_1['nameZipcode'] = 'Avalon ACHD'
    esdr_1['zipcode'] = 15202

    esdr_1.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_1.csv', index=False)

def clean_esdr_3():
    Feed_3_North_Braddock_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR + 'Feed_3_North_Braddock_ACHD_PM10.csv')
    Feed_3_North_Braddock_ACHD_PM10 = Feed_3_North_Braddock_ACHD_PM10.rename(columns={'3.feed_3.PM10B_UG_M3..3.feed_3.PM10_640_UG_M3':'PM10B_UG_M3.PM10_640_UG_M3'})


    Feed_3_North_Braddock_ACHD = pd.read_csv(DATA_DIR_ESDR + 'Feed_3_North_Braddock_ACHD.csv')
    Feed_3_North_Braddock_ACHD = Feed_3_North_Braddock_ACHD.rename(columns={'3.feed_3.SO2_PPM': 'SO2_PPM','3.feed_3.SONICWD_DEG':'SONICWD_DEG','3.feed_3.SONICWS_MPH':'SONICWS_MPH','3.feed_3.SIGTHETA_DEG':'SIGTHETA_DEG'})

    esdr_3 = pd.merge(Feed_3_North_Braddock_ACHD, Feed_3_North_Braddock_ACHD_PM10, on='EpochTime', how='left')
    esdr_3['datetime'] = pd.to_datetime(esdr_3['EpochTime'], unit='s')
    esdr_3['nameZipcode'] = 'North Braddock ACHD'
    esdr_3['zipcode'] = 15104

    esdr_3.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_3.csv', index=False)

def clean_esdr_23():
    Feed_23_Flag_Plaza_ACHD_CO = pd.read_csv(DATA_DIR_ESDR + 'Feed_23_Flag_Plaza_ACHD_CO.csv')
    Feed_23_Flag_Plaza_ACHD_CO = Feed_23_Flag_Plaza_ACHD_CO.rename(columns={'3.feed_23.CO_PPM..3.feed_23.CO_PPB':'CO_PPM.CO_PPB'})


    Feed_23_Flag_Plaza_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR + 'Feed_23_Flag_Plaza_ACHD_PM10.csv')
    Feed_23_Flag_Plaza_ACHD_PM10 = Feed_23_Flag_Plaza_ACHD_PM10.rename(columns={'3.feed_23.PM10_UG_M3':'PM10_UG_M3'})

    esdr_23 = pd.merge(Feed_23_Flag_Plaza_ACHD_PM10, Feed_23_Flag_Plaza_ACHD_CO, on='EpochTime' , how='left')
    esdr_23['datetime'] = pd.to_datetime(esdr_23['EpochTime'], unit='s')
    esdr_23['nameZipcode'] = 'Flag Plaza ACHD'
    esdr_23['zipcode'] = 15219
    esdr_23.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_23.csv', index=False)

def clean_esdr_24():
    Feed_24_Glassport_High_Street_ACHD = pd.read_csv(DATA_DIR_ESDR + 'Feed_24_Glassport_High_Street_ACHD.csv')
    esdr_24 = Feed_24_Glassport_High_Street_ACHD.rename(columns={'3.feed_24.PM10_UG_M3':'PM10_UG_M3'})

    esdr_24['datetime'] = pd.to_datetime(esdr_24['EpochTime'], unit='s')
    esdr_24['nameZipcode'] = 'Glassport High Street ACHD'
    esdr_24['zipcode'] = 15045
    esdr_24.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_24.csv', index=False)

def clean_esdr_26():
    Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR + 'Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10.csv')
    Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10 = Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10.rename(columns={'3.feed_26.PM10B_UG_M3..3.feed_59665.PM10_640_UG_M3':'PM10B_UG_M3.PM10_640_UG_M3'})


    Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25 = pd.read_csv(DATA_DIR_ESDR + 'Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25.csv')
    Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25 = Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25.rename(columns={'3.feed_26.PM25B_UG_M3..3.feed_26.PM25T_UG_M3..3.feed_59665.PM25_640_UG_M3':'PM25B_UG_M3.PM25T_UG_M3.PM25_640_UG_M3'})


    Feed_26_Lawrenceville_ACHD = pd.read_csv(DATA_DIR_ESDR + 'Feed_26_Lawrenceville_ACHD.csv')
    Feed_26_Lawrenceville_ACHD = Feed_26_Lawrenceville_ACHD.rename(columns={'3.feed_26.OZONE_PPM':'OZONE_PPM','3.feed_26.SONICWS_MPH':'SONICWS_MPH','3.feed_26.SONICWD_DEG':'SONICWD_DEG','3.feed_26.SIGTHETA_DEG':'SIGTHETA_DEG'})

    esdr_26 = pd.merge(Feed_26_Lawrenceville_ACHD , Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25, on='EpochTime' , how='left')
    esdr_26 = pd.merge(esdr_26, Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10, on='EpochTime', how='left')
    esdr_26['datetime'] = pd.to_datetime(esdr_26['EpochTime'], unit='s')
    esdr_26['nameZipcode'] = 'Lawrenceville ACHD'
    esdr_26['zipcode'] = 15201

    esdr_26.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_26.csv', index=False)

def clean_esdr_27():
    Feed_27_Lawrenceville_2_ACHD = pd.read_csv(DATA_DIR_ESDR + 'Feed_27_Lawrenceville_2_ACHD.csv')
    esdr_27 = Feed_27_Lawrenceville_2_ACHD.rename(columns={'3.feed_27.NO_PPB':'NO_PPB','3.feed_27.NOY_PPB':'NOY_PPB','3.feed_27.CO_PPB':'CO_PPB','3.feed_27.SO2_PPB':'SO2_PPB'})

    esdr_27['datetime'] = pd.to_datetime(esdr_27['EpochTime'], unit='s')
    esdr_27['nameZipcode'] = 'Lawrenceville 2 ACHD'
    esdr_27['zipcode'] = 15201
    esdr_27.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_27.csv', index=False)

def clean_esdr_28():
    Feed_28_Liberty_ACHD = pd.read_csv(DATA_DIR_ESDR + 'Feed_28_Liberty_ACHD.csv')
    esdr_28 = Feed_28_Liberty_ACHD.rename(columns={'3.feed_28.H2S_PPM': 'H2S_PPM','3.feed_28.SO2_PPM':'SO2_PPM','3.feed_28.SIGTHETA_DEG':'SIGTHETA_DEG','3.feed_28.SONICWD_DEG':'SONICWD_DEG','3.feed_28.SONICWS_MPH':'SONICWS_MPH'})
    esdr_28['datetime'] = pd.to_datetime(esdr_28['EpochTime'], unit='s')
    esdr_28['nameZipcode'] = 'Liberty ACHD'
    esdr_28['zipcode'] = 15133


    esdr_28.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_28.csv', index=False)

def clean_esdr_29():
    Feed_29_Liberty_2_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR + 'Feed_29_Liberty_2_ACHD_PM10.csv')
    Feed_29_Liberty_2_ACHD_PM10 = Feed_29_Liberty_2_ACHD_PM10.rename(columns={'3.feed_29.PM10_UG_M3': 'PM10_UG_M3'})

    Feed_29_Liberty_2_ACHD_PM25 = pd.read_csv(DATA_DIR_ESDR + 'Feed_29_Liberty_2_ACHD_PM25.csv')
    Feed_29_Liberty_2_ACHD_PM25 = Feed_29_Liberty_2_ACHD_PM25.rename(columns={'3.feed_29.PM25_UG_M3..3.feed_29.PM25T_UG_M3': 'PM25_UG_M3.PM25T_UG_M3'})

    esdr_29 = pd.merge(Feed_29_Liberty_2_ACHD_PM10, Feed_29_Liberty_2_ACHD_PM25, on='EpochTime', how='left')
    esdr_29['datetime'] = pd.to_datetime(esdr_29['EpochTime'], unit='s')
    esdr_29['nameZipcode'] = 'Liberty 2 ACHD'
    esdr_29['zipcode'] = 15133
    esdr_29.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_29.csv', index=False)

def clean_esdr_43():
    Feed_43_and_Feed_11067_Parkway_East_ACHD= pd.read_csv(DATA_DIR_ESDR + 'Feed_43_and_Feed_11067_Parkway_East_ACHD.csv')
    esdr_43 = Feed_43_and_Feed_11067_Parkway_East_ACHD.rename(columns={'3.feed_11067.CO_PPB..3.feed_43.CO_PPB': 'CO_PPB','3.feed_11067.NO2_PPB..3.feed_43.NO2_PPB':'NO2_PPB','3.feed_11067.NOX_PPB..3.feed_43.NOX_PPB':'NOX_PPB','3.feed_11067.NO_PPB..3.feed_43.NO_PPB':'NO_PPB','3.feed_11067.PM25T_UG_M3..3.feed_43.PM25T_UG_M3':'PM25T_UG_M3','3.feed_11067.SIGTHETA_DEG..3.feed_43.SIGTHETA_DEG':'SIGTHETA_DEG','3.feed_11067.SONICWD_DEG..3.feed_43.SONICWD_DEG':'SONICWD_DEG','3.feed_11067.SONICWS_MPH..3.feed_43.SONICWS_MPH':'SONICWS_MPH'})
    esdr_43['datetime'] = pd.to_datetime(esdr_43['EpochTime'], unit='s')
    esdr_43['nameZipcode'] = 'Parkway East Near Road ACHD'
    esdr_43['zipcode'] = 15221
    esdr_43.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_43.csv', index=False)

def clean_esdr_3506():
    Feed_3506_BAPC_301_39TH_STREET_BLDG_AirNow = pd.read_csv(DATA_DIR_ESDR + 'Feed_3506_BAPC_301_39TH_STREET_BLDG_AirNow.csv')
    esdr_3506 = Feed_3506_BAPC_301_39TH_STREET_BLDG_AirNow.rename(columns={'3.feed_3506.PM2_5': 'PM2_5','3.feed_3506.OZONE':'OZONE'})
    esdr_3506['datetime'] = pd.to_datetime(esdr_3506['EpochTime'], unit='s')
    esdr_3506['nameZipcode'] = 'BAPC 301 39TH STREET BLDG #7 AirNow'
    esdr_3506['zipcode'] = 15201
    esdr_3506.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_3506.csv', index=False)

def clean_esdr_3508():
    Feed_3508_South_Allegheny_High_School_AirNow = pd.read_csv(DATA_DIR_ESDR + 'Feed_3508_South_Allegheny_High_School_AirNow.csv')
    esdr_3508 = Feed_3508_South_Allegheny_High_School_AirNow.rename(columns={'3.feed_3508.PM2_5': 'PM2_5'})

    esdr_3508['datetime'] = pd.to_datetime(esdr_3508['EpochTime'], unit='s')
    esdr_3508['nameZipcode'] ='South Allegheny High School AirNow'
    esdr_3508['zipcode'] = 15133
    esdr_3508.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_3508.csv', index=False)

def clean_esdr_5975():
    Feed_5975_Parkway_East_AirNow = pd.read_csv(DATA_DIR_ESDR + 'Feed_5975_Parkway_East_AirNow.csv')
    esdr_5975 = Feed_5975_Parkway_East_AirNow.rename(columns={'3.feed_5975.PM2_5': 'PM2_5'})
    esdr_5975['datetime'] = pd.to_datetime(esdr_5975['EpochTime'], unit='s')
    esdr_5975['nameZipcode'] = 'Parkway East (Near Road) AirNow'
    esdr_5975['zipcode'] = 15221
    esdr_5975.to_csv(DATA_DIR_ESDR_CLEAN + 'esdr_5975.csv', index=False)


def clean_smell_report():
    smell_report = pd.read_csv(DATA_DIR_SMELL_REPORT + 'smell_report_raw.csv')
    smell_report['date'] = pd.to_datetime(smell_report['EpochTime'], unit='s').dt.date
    smell_report['datetime'] = pd.to_datetime(smell_report['EpochTime'], unit='s')

    # Save smell_report as JSON # Save smell_report as JSON
    smell_report.to_csv(DATA_DIR_SMELL_REPORT + 'smell_report.csv', index=False)