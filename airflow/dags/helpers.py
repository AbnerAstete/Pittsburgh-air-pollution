import os, pytz
from datetime import datetime
  
import pandas as pd
from py2neo import Node, Relationship, Graph, NodeMatcher

BASE_DIR = '/opt/airflow/'
DATA_DIR = os.path.join(BASE_DIR, 'data')

#connection with neo4j
graph = Graph("bolt://neo:7687")
neo4j_session = graph.begin()

def extract_esdr(url):
    start_dt = datetime(2016, 10, 31, 0, tzinfo=pytz.timezone("US/Eastern"))
    end_dt = datetime(2024, 1, 31, 0, tzinfo=pytz.timezone("US/Eastern"))
    getData(url,start_dt=start_dt, end_dt=end_dt)

def getData(url,start_dt=None, end_dt=None):
    """
    Get and save smell and ESDR data

    Input:
        out_p: the path for storing ESDR and smell data (optional)
        start_dt (datetime.datetime object): starting date that you want to get the data
        end_dt (datetime.datetime object): ending date that you want to get the data
        region_setting: setting of the region that we want to get the smell reports
        logger: the python logger created by the generateLogger() function

    Output:
        df_esdr_array_raw (list of pandas.DataFrame): a list of raw ESDR data for each channel
        df_smell_raw (pandas.DataFrame): raw smell data
    """
    # Get and save ESDR data
    # Feed 26: Lawrenceville ACHD
    # Feed 28: Liberty ACHD
    # Feed 23: Flag Plaza ACHD
    # Feed 43 and 11067: Parkway East ACHD
    # Feed 1: Avalon ACHD
    # Feed 27: Lawrenceville 2 ACHD
    # Feed 29: Liberty 2 ACHD
    # Feed 3: North Braddock ACHD
    # Feed 3506: BAPC 301 39TH STREET BLDG AirNow
    # Feed 5975: Parkway East AirNow
    # Feed 3508: South Allegheny High School AirNow
    # Feed 24: Glassport High Street ACHD
    # Feed 11067: Parkway East Near Road ACHD
    # Feed 59665: Pittsburgh ACHD
    esdr_source_names = [
        "Feed_1_Avalon_ACHD_PM25",
        "Feed_1_Avalon_ACHD",
        "Feed_26_Lawrenceville_ACHD",
        "Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25",
        "Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10",
        "Feed_27_Lawrenceville_2_ACHD",
        "Feed_28_Liberty_ACHD",
        "Feed_29_Liberty_2_ACHD_PM10",
        "Feed_29_Liberty_2_ACHD_PM25",
        "Feed_3_North_Braddock_ACHD",
        "Feed_3_North_Braddock_ACHD_PM10",
        "Feed_23_Flag_Plaza_ACHD_CO",
        "Feed_23_Flag_Plaza_ACHD_PM10",
        "Feed_43_and_Feed_11067_Parkway_East_ACHD",
        "Feed_3506_BAPC_301_39TH_STREET_BLDG_AirNow",
        "Feed_5975_Parkway_East_AirNow",
        "Feed_3508_South_Allegheny_High_School_AirNow",
        "Feed_24_Glassport_High_Street_ACHD"
    ]
    # IMPORTANT: if you add more data that changes the length of the esdr_source list,
    # ...you need to also add a name to the corresponding index of the esdr_source_names list.
    esdr_source = [
        [
            {"feed": "1", "channel": "PM25B_UG_M3"},
            {"feed": "1", "channel": "PM25T_UG_M3"},
            {"feed": "1", "channel": "PM25_640_UG_M3"}
        ],
        [{"feed": "1", "channel": "SO2_PPM,H2S_PPM,SIGTHETA_DEG,SONICWD_DEG,SONICWS_MPH"}],
        [{"feed": "26", "channel": "OZONE_PPM,SONICWS_MPH,SONICWD_DEG,SIGTHETA_DEG"}],
        [
            {"feed": "26", "channel": "PM25B_UG_M3"},
            {"feed": "26", "channel": "PM25T_UG_M3"},
            {"feed": "59665", "channel": "PM25_640_UG_M3"}
        ],
        [
            {"feed": "26", "channel": "PM10B_UG_M3"},
            {"feed": "59665", "channel": "PM10_640_UG_M3"}
        ],
        [{"feed": "27", "channel": "NO_PPB,NOY_PPB,CO_PPB,SO2_PPB"}],
        [{"feed": "28", "channel": "H2S_PPM,SO2_PPM,SIGTHETA_DEG,SONICWD_DEG,SONICWS_MPH"}],
        [{"feed": "29", "channel": "PM10_UG_M3"}],
        [
            {"feed": "29", "channel": "PM25_UG_M3"},
            {"feed": "29", "channel": "PM25T_UG_M3"}
        ],
        [{"feed": "3", "channel": "SO2_PPM,SONICWD_DEG,SONICWS_MPH,SIGTHETA_DEG"}],
        [
            {"feed": "3", "channel": "PM10B_UG_M3"},
            {"feed": "3", "channel": "PM10_640_UG_M3"}
        ],
        [
            {"feed": "23", "channel": "CO_PPM"},
            {"feed": "23", "channel": "CO_PPB", "factor": 0.001}
        ],
        [{"feed": "23", "channel": "PM10_UG_M3"}],
        [
            {"feed": "11067", "channel": "CO_PPB,NO2_PPB,NOX_PPB,NO_PPB,PM25T_UG_M3,SIGTHETA_DEG,SONICWD_DEG,SONICWS_MPH"},
            {"feed": "43", "channel": "CO_PPB,NO2_PPB,NOX_PPB,NO_PPB,PM25T_UG_M3,SIGTHETA_DEG,SONICWD_DEG,SONICWS_MPH"}
        ],
        [{"feed": "3506", "channel": "PM2_5,OZONE"}],
        [{"feed": "5975", "channel": "PM2_5"}],
        [{"feed": "3508", "channel": "PM2_5"}],
        [{"feed": "24", "channel": "PM10_UG_M3"}]
    ]
    start_time = datetimeToEpochtime(start_dt) / 1000 # ESDR uses seconds
    end_time = datetimeToEpochtime(end_dt) / 1000 # ESDR uses seconds
    df_esdr_array_raw = getEsdrData(url,esdr_source, start_time=start_time, end_time=end_time)
    #print('EL LARGO ES:'+ str(len(df_esdr_array_raw)))

    # Save ESDR data
    for i in range(len(df_esdr_array_raw)):
            df_esdr_array_raw[i].to_csv(os.path.join(DATA_DIR, esdr_source_names[i] + ".csv"))

def datetimeToEpochtime(dt):
    """Convert a datetime object to epoch time"""
    if dt.tzinfo is None:
        dt_utc = dt
    else:
        dt_utc = dt.astimezone(pytz.utc).replace(tzinfo=None)
    epoch_utc = datetime.utcfromtimestamp(0)
    return int((dt_utc - epoch_utc).total_seconds() * 1000)

def getEsdrData(url,source, **options):
    """
    Get data from ESDR
    source = [
        [{"feed": 27, "channel": "NO_PPB"}],
        [{"feed": 1, "channel": "PM25B_UG_M3"}, {"feed": 1, "channel": "PM25T_UG_M3"}]
    ]
    if source = [[A,B],[C]], this means that A and B will be merged
    start_time: starting epochtime in seconds
    end_time: ending epochtime in seconds
    """
    print("Get ESDR data...")

    # Url parts
    api_url = url + "api/v1/"
    export_para = "/export?format=csv"
    if "start_time" in options:
        export_para += "&from=" + str(options["start_time"])
    if "end_time" in options:
        export_para += "&to=" + str(options["end_time"])

    # Loop each source
    data = []
    for s_all in source:
        df = None
        for s in s_all:
            # Read data
            feed_para = "feeds/" + s["feed"]
            channel_para = "/channels/" + s["channel"]
            df_s = pd.read_csv(api_url + feed_para + channel_para + export_para)
            df_s.set_index("EpochTime", inplace=True)
            if "factor" in s:
                df_s = df_s * s["factor"]
            if df is None:
                df = df_s
            else:
                # Merge column names
                c = []
                for k in zip(df.columns, df_s.columns):
                    if k[0] != k[1]:
                        c.append(k[0] + ".." + k[1])
                    else:
                        c.append(k[0])
                df.columns = c
                df_s.columns = c
                df = pd.concat([df[~df.index.isin(df_s.index)], df_s])
        df = df.apply(pd.to_numeric, errors="coerce") # To numeric values
        data.append(df)
    # Return
    return data