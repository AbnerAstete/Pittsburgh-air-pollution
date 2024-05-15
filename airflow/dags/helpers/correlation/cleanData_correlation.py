import os
from datetime import datetime
  
import pandas as pd

DATA_DIR_COR = 'airflow/data/correlation_analisys/'

def cleanData_correlation():
    df = pd.read_csv(DATA_DIR_COR+'correlation_merge.csv')
    df = df.rename(columns={
        "3.feed_1.SO2_PPM": "SO2_PPM",
        "3.feed_1.H2S_PPM": "H2S_PPM",
        "3.feed_1.SIGTHETA_DEG": "SIGTHETA_DEG",
        "3.feed_1.SONICWD_DEG": "SONICWD_DEG",
        "3.feed_1.SONICWS_MPH": "SONICWS_MPH",
        "3.feed_11067.CO_PPB..3.feed_43.CO_PPB": "CO_PPB",
        "3.feed_11067.NO2_PPB..3.feed_43.NO2_PPB": "NO2_PPB",
        "3.feed_11067.NOX_PPB..3.feed_43.NOX_PPB": "NOX_PPB",
        "3.feed_11067.NO_PPB..3.feed_43.NO_PPB": "NO_PPB",
        "3.feed_11067.PM25T_UG_M3..3.feed_43.PM25T_UG_M3": "PM2_5",
        "3.feed_11067.SIGTHETA_DEG..3.feed_43.SIGTHETA_DEG": "SIGTHETA_DEG",
        "3.feed_11067.SONICWD_DEG..3.feed_43.SONICWD_DEG": "SONICWD_DEG",
        "3.feed_11067.SONICWS_MPH..3.feed_43.SONICWS_MPH": "SONICWS_MPH",
        "3.feed_29.PM10_UG_M3": "PM10",
        "3.feed_26.PM10B_UG_M3..3.feed_59665.PM10_640_UG_M3": "PM10",
        "3.feed_3506.PM2_5": "PM2_5",
        "3.feed_3506.OZONE": "OZONE",
        "3.feed_24.PM10_UG_M3": "PM10",
        "3.feed_3508.PM2_5": "PM2_5",
        "3.feed_3.PM10B_UG_M3..3.feed_3.PM10_640_UG_M3": "PM10",
        "3.feed_23.CO_PPM..3.feed_23.CO_PPB": "",
        "3.feed_28.H2S_PPM": "H2S_PPM",
        "3.feed_28.SO2_PPM": "SO2_PPM",
        "3.feed_28.SIGTHETA_DEG": "SIGTHETA_DEG",
        "3.feed_28.SONICWD_DEG": "SONICWD_DEG",
        "3.feed_28.SONICWS_MPH": "SONICWS_MPH",
        "3.feed_1.PM25B_UG_M3..3.feed_1.PM25T_UG_M3..3.feed_1.PM25_640_UG_M3": "",
        "3.feed_26.OZONE_PPM": "OZONE_PPM",
        "3.feed_26.SONICWS_MPH": "SONICWS_MPH",
        "3.feed_26.SONICWD_DEG": "SONICWD_DEG",
        "3.feed_26.SIGTHETA_DEG": "SIGTHETA_DEG",
        "3.feed_3.SO2_PPM": "SO2_PPM",
        "3.feed_3.SONICWD_DEG": "SONICWD_DEG",
        "3.feed_3.SONICWS_MPH": "SONICWS_MPH",
        "3.feed_3.SIGTHETA_DEG": "SIGTHETA_DEG",
        "3.feed_5975.PM2_5": "PM2_5",
        "3.feed_23.PM10_UG_M3": "PM10",
        "3.feed_27.NO_PPB": "NO_PPB",
        "3.feed_27.NOY_PPB": "NOY_PPB",
        "3.feed_27.CO_PPB": "CO_PPB",
        "3.feed_27.SO2_PPB": "SO2_PPB",
        "3.feed_29.PM25_UG_M3..3.feed_29.PM25T_UG_M3": "",
        "3.feed_26.PM25B_UG_M3..3.feed_26.PM25T_UG_M3..3.feed_59665.PM25_640_UG_M3": "",


        
    })  # Cambiar nombres de columnas
    

cleanData_correlation()