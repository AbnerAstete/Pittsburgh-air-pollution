import os, pytz
from datetime import datetime
  
import pandas as pd
import numpy as np
from py2neo import Node, Relationship, Graph, NodeMatcher

BASE_DIR = '/opt/airflow/'
DATA_DIR = os.path.join(BASE_DIR, 'data')
DATA_DIR_ESDR = 'data/esdr/'
DATA_DIR_ESDR_RAW = 'data/esdr_raw/'
DATA_DIR_SMELL_REPORT = 'data/smell_report/'
DATA_DIR_SMELL_REPORT_RAW = 'data/smell_report_raw/'




#connection with neo4j
graph = Graph("bolt://neo:7687")
neo4j_session = graph.begin()

def extract_esdr(url):
    start_dt = datetime(2016, 10, 31, 0, tzinfo=pytz.timezone("US/Eastern"))
    end_dt = datetime(2024, 1, 31, 0, tzinfo=pytz.timezone("US/Eastern"))
    getData_esdr(url,start_dt=start_dt, end_dt=end_dt)

def getData_esdr(url,start_dt=None, end_dt=None):
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

    # Save ESDR data
    for i in range(len(df_esdr_array_raw)):
            df_esdr_array_raw[i].to_csv( DATA_DIR_ESDR_RAW + esdr_source_names[i] + ".csv")


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

def extract_smell(url):
    start_dt = datetime(2016, 10, 31, 0, tzinfo=pytz.timezone("US/Eastern"))
    end_dt = datetime(2024, 1, 31, 0, tzinfo=pytz.timezone("US/Eastern"))
    df_smell_raw = getSmellReports(url,start_dt=start_dt, end_dt=end_dt,allegheny_county=True)
    df_smell_raw.to_csv( DATA_DIR_SMELL_REPORT_RAW + 'smell_report_raw.csv')


def getSmellReports(url,**options):
    """Get smell reports data from SmellPGH"""
    print("Get smell reports from V2 API...")

    # Url
    api_url = url + "api/v2/"
    api_para = "smell_reports?"
    if "allegheny_county" in options and options["allegheny_county"] == True:
        # This is for general dataset usage in the Allegheny County in Pittsburgh
        api_para += "zipcodes=15202,15104,15219,15045,15201,15133,15221"
    if "start_time" in options:
        api_para += "&start_time=" + str(options["start_time"])
    if "end_time" in options:
        api_para += "&end_time=" + str(options["end_time"])

    # Load smell reports
    df = pd.read_json(api_url + api_para, convert_dates=False)

    # If empty, return None
    if df.empty:
        return None

    # Wrangle text
    df["smell_description"] = df["smell_description"].replace(np.nan, "").map(removeNonAsciiChars)
    df["feelings_symptoms"] = df["feelings_symptoms"].replace(np.nan, "").map(removeNonAsciiChars)
    df["additional_comments"] = df["additional_comments"].replace(np.nan, "").map(removeNonAsciiChars)

    # Set index and drop columns
    df.set_index("observed_at", inplace=True)
    df.index.names = ["EpochTime"]
    df.rename(columns={"latitude": "skewed_latitude", "longitude": "skewed_longitude"}, inplace=True)
    df.drop(["zip_code_id"], axis=1, inplace=True)

    # Return
    return df
    
def getSmellReports_complete(url,**options):
    """Get smell reports data from SmellPGH"""
    print("Get smell reports from V2 API...")

    # Url
    api_url = url + "api/v2/"
    api_para = "smell_reports?"
    if "allegheny_county" in options and options["allegheny_county"] == True:
        # This is for general dataset usage in the Allegheny County in Pittsburgh
        api_para += "zipcodes=15006,15007,15014,15015,15017,15018,15020,15024,15025,15028,15030,15031,15032,15034,15035,15037,15044,15045,15046,15047,15049,15051,15056,15064,15065,15071,15075,15076,15082,15084,15086,15088,15090,15091,15095,15096,15101,15102,15104,15106,15108,15110,15112,15116,15120,15122,15123,15126,15127,15129,15131,15132,15133,15134,15135,15136,15137,15139,15140,15142,15143,15144,15145,15146,15147,15148,15201,15202,15203,15204,15205,15206,15207,15208,15209,15210,15211,15212,15213,15214,15215,15216,15217,15218,15219,15220,15221,15222,15223,15224,15225,15226,15227,15228,15229,15230,15231,15232,15233,15234,15235,15236,15237,15238,15239,15240,15241,15242,15243,15244,15250,15251,15252,15253,15254,15255,15257,15258,15259,15260,15261,15262,15264,15265,15267,15268,15270,15272,15274,15275,15276,15277,15278,15279,15281,15282,15283,15286,15289,15290,15295"
    else:
        # This is for our smell pgh paper
        api_para += "zipcodes=15221,15218,15222,15219,15201,15224,15213,15232,15206,15208,15217,15207,15260,15104"
    if "start_time" in options:
        api_para += "&start_time=" + str(options["start_time"])
    if "end_time" in options:
        api_para += "&end_time=" + str(options["end_time"])

    # Load smell reports
    df = pd.read_json(api_url + api_para, convert_dates=False)

    # If empty, return None
    if df.empty:
        return None

    # Wrangle text
    df["smell_description"] = df["smell_description"].replace(np.nan, "").map(removeNonAsciiChars)
    df["feelings_symptoms"] = df["feelings_symptoms"].replace(np.nan, "").map(removeNonAsciiChars)
    df["additional_comments"] = df["additional_comments"].replace(np.nan, "").map(removeNonAsciiChars)

    # Set index and drop columns
    df.set_index("observed_at", inplace=True)
    df.index.names = ["EpochTime"]
    df.rename(columns={"latitude": "skewed_latitude", "longitude": "skewed_longitude"}, inplace=True)
    df.drop(["zip_code_id"], axis=1, inplace=True)

    # Return
    return df

def removeNonAsciiChars(str_in):
    """Remove all non-ascii characters in the string"""
    if str_in is None:
        return ""
    return str_in.encode("ascii", "ignore").decode()


def clean_esdr_1():
    Feed_1_Avalon_ACHD_PM25 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_1_Avalon_ACHD_PM25.csv')
    Feed_1_Avalon_ACHD_PM25 = Feed_1_Avalon_ACHD_PM25.rename(columns={'3.feed_1.PM25B_UG_M3..3.feed_1.PM25T_UG_M3..3.feed_1.PM25_640_UG_M3':'PM25B_UG_M3.PM25T_UG_M3.PM25_640_UG_M3'})


    Feed_1_Avalon_ACHD = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_1_Avalon_ACHD.csv')
    Feed_1_Avalon_ACHD = Feed_1_Avalon_ACHD.rename(columns={'3.feed_1.SO2_PPM': 'SO2_PPM','3.feed_1.H2S_PPM':'H2S_PPM','3.feed_1.SIGTHETA_DEG':'SIGTHETA_DEG','3.feed_1.SONICWD_DEG':'SONICWD_DEG','3.feed_1.SONICWS_MPH':'SONICWS_MPH'})


    esdr_1 = pd.merge(Feed_1_Avalon_ACHD_PM25, Feed_1_Avalon_ACHD, on='EpochTime')
    esdr_1['datetime'] = pd.to_datetime(esdr_1['EpochTime'], unit='s')
    esdr_1['nameZipcode'] = 'Avalon ACHD'
    esdr_1['zipcode'] = 15202

    esdr_1.to_csv(DATA_DIR_ESDR + 'esdr_1.csv', index=False)

def clean_esdr_3():
    Feed_3_North_Braddock_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_3_North_Braddock_ACHD_PM10.csv')
    Feed_3_North_Braddock_ACHD_PM10 = Feed_3_North_Braddock_ACHD_PM10.rename(columns={'3.feed_3.PM10B_UG_M3..3.feed_3.PM10_640_UG_M3':'PM10B_UG_M3.PM10_640_UG_M3'})


    Feed_3_North_Braddock_ACHD = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_3_North_Braddock_ACHD.csv')
    Feed_3_North_Braddock_ACHD = Feed_3_North_Braddock_ACHD.rename(columns={'3.feed_3.SO2_PPM': 'SO2_PPM','3.feed_3.SONICWD_DEG':'SONICWD_DEG','3.feed_3.SONICWS_MPH':'SONICWS_MPH','3.feed_3.SIGTHETA_DEG':'SIGTHETA_DEG'})

    esdr_3 = pd.merge(Feed_3_North_Braddock_ACHD_PM10, Feed_3_North_Braddock_ACHD, on='EpochTime')
    esdr_3['datetime'] = pd.to_datetime(esdr_3['EpochTime'], unit='s')
    esdr_3['nameZipcode'] = 'North Braddock ACHD'
    esdr_3['zipcode'] = 15104

    esdr_3.to_csv(DATA_DIR_ESDR + 'esdr_3.csv', index=False)

def clean_esdr_23():
    Feed_23_Flag_Plaza_ACHD_CO = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_23_Flag_Plaza_ACHD_CO.csv')
    Feed_23_Flag_Plaza_ACHD_CO = Feed_23_Flag_Plaza_ACHD_CO.rename(columns={'3.feed_23.CO_PPM..3.feed_23.CO_PPB':'CO_PPM.CO_PPB'})


    Feed_23_Flag_Plaza_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_23_Flag_Plaza_ACHD_PM10.csv')
    Feed_23_Flag_Plaza_ACHD_PM10 = Feed_23_Flag_Plaza_ACHD_PM10.rename(columns={'3.feed_23.PM10_UG_M3':'PM10_UG_M3'})

    esdr_23 = pd.merge(Feed_23_Flag_Plaza_ACHD_PM10, Feed_23_Flag_Plaza_ACHD_CO, on='EpochTime')
    esdr_23['datetime'] = pd.to_datetime(esdr_23['EpochTime'], unit='s')
    esdr_23['nameZipcode'] = 'Flag Plaza ACHD'
    esdr_23['zipcode'] = 15219
    esdr_23.to_csv(DATA_DIR_ESDR + 'esdr_23.csv', index=False)

def clean_esdr_24():
    Feed_24_Glassport_High_Street_ACHD = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_24_Glassport_High_Street_ACHD.csv')
    esdr_24 = Feed_24_Glassport_High_Street_ACHD.rename(columns={'3.feed_24.PM10_UG_M3':'PM10_UG_M3'})

    esdr_24['datetime'] = pd.to_datetime(esdr_24['EpochTime'], unit='s')
    esdr_24['nameZipcode'] = 'Glassport High Street ACHD'
    esdr_24['zipcode'] = 15045
    esdr_24.to_csv(DATA_DIR_ESDR + 'esdr_24.csv', index=False)

def clean_esdr_26():
    Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10.csv')
    Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10 = Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10.rename(columns={'3.feed_26.PM10B_UG_M3..3.feed_59665.PM10_640_UG_M3':'PM10B_UG_M3.PM10_640_UG_M3'})


    Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25.csv')
    Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25 = Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25.rename(columns={'3.feed_26.PM25B_UG_M3..3.feed_26.PM25T_UG_M3..3.feed_59665.PM25_640_UG_M3':'PM25B_UG_M3.PM25T_UG_M3.PM25_640_UG_M3'})


    Feed_26_Lawrenceville_ACHD = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_26_Lawrenceville_ACHD.csv')
    Feed_26_Lawrenceville_ACHD = Feed_26_Lawrenceville_ACHD.rename(columns={'3.feed_26.OZONE_PPM':'OZONE_PPM','3.feed_26.SONICWS_MPH':'SONICWS_MPH','3.feed_26.SONICWD_DEG':'SONICWD_DEG','3.feed_26.SIGTHETA_DEG':'SIGTHETA_DEG'})

    esdr_26 = pd.merge(Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM10, Feed_26_and_Feed_59665_Lawrenceville_ACHD_PM25, on='EpochTime')
    esdr_26 = pd.merge(esdr_26, Feed_26_Lawrenceville_ACHD, on='EpochTime')
    esdr_26['datetime'] = pd.to_datetime(esdr_26['EpochTime'], unit='s')
    esdr_26['nameZipcode'] = 'Lawrenceville ACHD'
    esdr_26['zipcode'] = 15201

    esdr_26.to_csv(DATA_DIR_ESDR + 'esdr_26.csv', index=False)

def clean_esdr_27():
    Feed_27_Lawrenceville_2_ACHD = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_27_Lawrenceville_2_ACHD.csv')
    esdr_27 = Feed_27_Lawrenceville_2_ACHD.rename(columns={'3.feed_27.NO_PPB':'NO_PPB','3.feed_27.NOY_PPB':'NOY_PPB','3.feed_27.CO_PPB':'CO_PPB','3.feed_27.SO2_PPB':'SO2_PPB'})

    esdr_27['datetime'] = pd.to_datetime(esdr_27['EpochTime'], unit='s')
    esdr_27['nameZipcode'] = 'Lawrenceville 2 ACHD'
    esdr_27['zipcode'] = 15201
    esdr_27.to_csv(DATA_DIR_ESDR + 'esdr_27.csv', index=False)

def clean_esdr_28():
    Feed_28_Liberty_ACHD = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_28_Liberty_ACHD.csv')
    esdr_28 = Feed_28_Liberty_ACHD.rename(columns={'3.feed_28.H2S_PPM': 'H2S_PPM','3.feed_28.SO2_PPM':'SO2_PPM','3.feed_28.SIGTHETA_DEG':'SIGTHETA_DEG','3.feed_28.SONICWD_DEG':'SONICWD_DEG','3.feed_28.SONICWS_MPH':'SONICWS_MPH'})
    esdr_28['datetime'] = pd.to_datetime(esdr_28['EpochTime'], unit='s')
    esdr_28['nameZipcode'] = 'Liberty ACHD'
    esdr_28['zipcode'] = 15133


    esdr_28.to_csv(DATA_DIR_ESDR + 'esdr_28.csv', index=False)

def clean_esdr_29():
    Feed_29_Liberty_2_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_29_Liberty_2_ACHD_PM10.csv')
    Feed_29_Liberty_2_ACHD_PM10 = Feed_29_Liberty_2_ACHD_PM10.rename(columns={'3.feed_29.PM10_UG_M3': 'PM10_UG_M3'})

    Feed_29_Liberty_2_ACHD_PM25 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_29_Liberty_2_ACHD_PM25.csv')
    Feed_29_Liberty_2_ACHD_PM25 = Feed_29_Liberty_2_ACHD_PM25.rename(columns={'3.feed_29.PM25_UG_M3..3.feed_29.PM25T_UG_M3': 'PM25_UG_M3.PM25T_UG_M3'})

    esdr_29 = pd.merge(Feed_29_Liberty_2_ACHD_PM10, Feed_29_Liberty_2_ACHD_PM25, on='EpochTime')
    esdr_29['datetime'] = pd.to_datetime(esdr_29['EpochTime'], unit='s')
    esdr_29['nameZipcode'] = 'Liberty 2 ACHD'
    esdr_29['zipcode'] = 15133
    esdr_29.to_csv(DATA_DIR_ESDR + 'esdr_29.csv', index=False)

def clean_esdr_43():
    Feed_43_and_Feed_11067_Parkway_East_ACHD= pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_43_and_Feed_11067_Parkway_East_ACHD.csv')
    esdr_43 = Feed_43_and_Feed_11067_Parkway_East_ACHD.rename(columns={'3.feed_11067.CO_PPB..3.feed_43.CO_PPB': 'CO_PPB','3.feed_11067.NO2_PPB..3.feed_43.NO2_PPB':'NO2_PPB','3.feed_11067.NOX_PPB..3.feed_43.NOX_PPB':'NOX_PPB','3.feed_11067.NO_PPB..3.feed_43.NO_PPB':'NO_PPB','3.feed_11067.PM25T_UG_M3..3.feed_43.PM25T_UG_M3':'PM25T_UG_M3','3.feed_11067.SIGTHETA_DEG..3.feed_43.SIGTHETA_DEG':'SIGTHETA_DEG','3.feed_11067.SONICWD_DEG..3.feed_43.SONICWD_DEG':'SONICWD_DEG','3.feed_11067.SONICWS_MPH..3.feed_43.SONICWS_MPH':'SONICWS_MPH'})
    esdr_43['datetime'] = pd.to_datetime(esdr_43['EpochTime'], unit='s')
    esdr_43['nameZipcode'] = 'Parkway East Near Road ACHD'
    esdr_43['zipcode'] = 15221
    esdr_43.to_csv(DATA_DIR_ESDR + 'esdr_43.csv', index=False)

def clean_esdr_3506():
    Feed_3506_BAPC_301_39TH_STREET_BLDG_AirNow = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_3506_BAPC_301_39TH_STREET_BLDG_AirNow.csv')
    esdr_3506 = Feed_3506_BAPC_301_39TH_STREET_BLDG_AirNow.rename(columns={'3.feed_3506.PM2_5': 'PM2_5','3.feed_3506.OZONE':'OZONE'})
    esdr_3506['datetime'] = pd.to_datetime(esdr_3506['EpochTime'], unit='s')
    esdr_3506['nameZipcode'] = 'BAPC 301 39TH STREET BLDG #7 AirNow'
    esdr_3506['zipcode'] = 15201
    esdr_3506.to_csv(DATA_DIR_ESDR + 'esdr_3506.csv', index=False)

def clean_esdr_3508():
    Feed_3508_South_Allegheny_High_School_AirNow = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_3508_South_Allegheny_High_School_AirNow.csv')
    esdr_3508 = Feed_3508_South_Allegheny_High_School_AirNow.rename(columns={'3.feed_3508.PM2_5': 'PM2_5'})

    esdr_3508['datetime'] = pd.to_datetime(esdr_3508['EpochTime'], unit='s')
    esdr_3508['nameZipcode'] ='South Allegheny High School AirNow'
    esdr_3508['zipcode'] = 15133
    esdr_3508.to_csv(DATA_DIR_ESDR + 'esdr_3508.csv', index=False)

def clean_esdr_5975():
    Feed_5975_Parkway_East_AirNow = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_5975_Parkway_East_AirNow.csv')
    esdr_5975 = Feed_5975_Parkway_East_AirNow.rename(columns={'3.feed_5975.PM2_5': 'PM2_5'})
    esdr_5975['datetime'] = pd.to_datetime(esdr_5975['EpochTime'], unit='s')
    esdr_5975['nameZipcode'] = 'Parkway East (Near Road) AirNow'
    esdr_5975['zipcode'] = 15221
    esdr_5975.to_csv(DATA_DIR_ESDR + 'esdr_5975.csv', index=False)


def clean_smell_report():
    smell_report = pd.read_csv(DATA_DIR_SMELL_REPORT_RAW + 'smell_report_raw.csv')
    smell_report['date'] = pd.to_datetime(smell_report['EpochTime'], unit='s').dt.date
    smell_report['datetime'] = pd.to_datetime(smell_report['EpochTime'], unit='s')

    # Save smell_report as JSON # Save smell_report as JSON
    smell_report.to_csv(DATA_DIR_SMELL_REPORT + 'smell_report.csv', index=False)

def insert_smell_data_in_neo4j():
    graph.delete_all()

    # Read the data as DataFrame
    smell_report = pd.read_csv(DATA_DIR_SMELL_REPORT + 'smell_report.csv')

    for index, report in smell_report.iterrows():
        #date = report['date'] 
        datetime = report['datetime']
        zipcode = report['zipcode']
        lat = report['skewed_latitude']
        lon = report['skewed_longitude']
        smell_value = report['smell_value']
        smell_description = report['smell_description']
        feelings_symptoms = report['feelings_symptoms']
        additional_comments = report['additional_comments']

        # Create the "Day" node
        # date_node = Node("Day", date=date)
        # graph.merge(date_node, "Day", "date")

        # Create the "Smell" node
        smell_node = Node("Smell", datetime=datetime ,smell_value=smell_value, smell_description=smell_description, feelings_symptoms=feelings_symptoms, additional_comments=additional_comments, lat=lat, lon=lon)
        graph.merge(smell_node, "Smell", "datetime")
        # Create the relationship between "Day" and "Smell"
        # relation_smell = Relationship(smell_node, "REPORTED_ON", date_node)
        # graph.create(relation_smell)

        # Create the "Zipcode" node
        zipcode_node = Node("Zipcode", value=zipcode)
        graph.merge(zipcode_node, "Zipcode", "value")
        # Create the relationship between "Smell" and "Zipcode"
        relation_zipcode = Relationship(smell_node, "REPORTED_IN", zipcode_node)
        graph.create(relation_zipcode)

def insert_esdr_1_neo4j():
    esdr_1 = pd.read_csv(DATA_DIR_ESDR + 'esdr_1.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name= 'Avalon ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15202)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_1.iterrows():
        datetime = report['datetime']
        PM25B = report['PM25B_UG_M3.PM25T_UG_M3.PM25_640_UG_M3']
        SO2_PPM = report['SO2_PPM']
        H2S_PPM = report['H2S_PPM']
        SIGTHETA_DEG = report['SIGTHETA_DEG']
        SONICWD_DEG = report['SONICWD_DEG']
        SONICWS_MPH = report['SONICWS_MPH']

        # Create the "PM2_5" node
        PM2_5_node = Node("PM2_5", value=PM25B, datetime= datetime)
        graph.create(PM2_5_node)
        # Create the "SO2" node
        SO2_node = Node("SO2", PPM=SO2_PPM, datetime= datetime)
        graph.create(SO2_node)
        # Create the "H2S" node
        H2S_node = Node("H2S", PPM=H2S_PPM, datetime= datetime)
        graph.create(H2S_node)

        # Create the "WIND" node
        WIND_node = Node("WIND", dev_standar_direction=SIGTHETA_DEG, direction=SONICWD_DEG, speed_mph=SONICWS_MPH, datetime= datetime) 
        graph.create(WIND_node)

        
        # Create the relationship between "PM2_5" and "Sensor"
        relation_PM25B = Relationship(sensor_node, "MEASURED", PM2_5_node)
        graph.create(relation_PM25B)
        # Create the relationship between "SO_2" and "Sensor"
        relation_SO2_PPM = Relationship(sensor_node, "MEASURED", SO2_node)
        graph.create(relation_SO2_PPM)
        # Create the relationship between "H2_S" and "Sensor"
        relation_H2S_PPM = Relationship(sensor_node, "MEASURED", H2S_node)
        graph.create(relation_H2S_PPM)
        # Create the relationship between "WIND" and "Zipcode"
        relation_WIND = Relationship(sensor_node, "MEASURED", WIND_node)
        graph.create(relation_WIND)

        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)
        
def insert_esdr_3_neo4j():
    esdr_3 = pd.read_csv(DATA_DIR_ESDR + 'esdr_3.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='North Braddock ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15104)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_3.iterrows():
        datetime = report['datetime']
        PM10B = report['PM10B_UG_M3.PM10_640_UG_M3'] 
        SO2_PPM = report['SO2_PPM']
        SONICWD_DEG = report['SONICWD_DEG']
        SONICWS_MPH = report['SONICWS_MPH']
        SIGTHETA_DEG = report['SIGTHETA_DEG']

        # Create the "PM10B" node
        PM10_node = Node("PM10", value=PM10B, datetime= datetime)
        graph.create(PM10_node)
        # Create the "SO2_PPM" node
        SO2_node = Node("SO2", PPM=SO2_PPM, datetime= datetime)
        graph.create(SO2_node)
        # Create the "WIND" node
        WIND_node = Node("WIND", dev_standar_direction=SIGTHETA_DEG, direction=SONICWD_DEG, speed_mph=SONICWS_MPH, datetime= datetime) 
        graph.create(WIND_node)


        # Create the relationship between "PM10B" and "Sensor"
        relation_PM10B = Relationship(sensor_node, "MEASURED", PM10_node)
        graph.create(relation_PM10B)
        # Create the relationship between "SO2_PPM" and "Sensor"
        relation_SO2_PPM = Relationship(sensor_node, "MEASURED", SO2_node)
        graph.create(relation_SO2_PPM)
        # Create the relationship between "WIND" and "Zipcode"
        relation_WIND = Relationship(sensor_node, "MEASURED", WIND_node)
        graph.create(relation_WIND)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)

def insert_esdr_23_neo4j():
    esdr_23 = pd.read_csv(DATA_DIR_ESDR + 'esdr_23.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Flag Plaza ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15219)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_23.iterrows():
        datetime = report['datetime']
        PM10 = report['PM10_UG_M3']
        CO = report['CO_PPM.CO_PPB']

        # Create the "PM10" node
        PM10_node = Node("PM10", value=PM10, datetime= datetime)
        graph.create(PM10_node)

        # Create the "CO" node
        CO_node = Node("CO", PPM_PPB=CO, datetime= datetime)
        graph.create(CO_node)

        # Create the relationship between "PM10" and "Sensor"
        relation_PM10 = Relationship(sensor_node, "MEASURED", PM10_node)
        graph.create(relation_PM10)
        # Create the relationship between "CO" and "Sensor"
        relation_CO = Relationship(sensor_node, "MEASURED", CO_node)
        graph.create(relation_CO)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)

def insert_esdr_24_neo4j():
    esdr_24 = pd.read_csv(DATA_DIR_ESDR + 'esdr_24.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Glassport High Street ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15045)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_24.iterrows():
        datetime = report['datetime']
        PM10 = report['PM10_UG_M3']

        # Create the "PM10" node
        PM10_node = Node("PM10", PM10=PM10, datetime= datetime)
        graph.create(PM10_node)

        # Create the relationship between "PM10" and "Sensor"
        relation_PM10 = Relationship(sensor_node, "MEASURED", PM10_node)
        graph.create(relation_PM10)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)
      
def insert_esdr_26_neo4j():
    esdr_26 = pd.read_csv(DATA_DIR_ESDR + 'esdr_26.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Lawrenceville ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15201)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_26.iterrows():
        datetime = report['datetime']
        PM10 = report['PM10B_UG_M3.PM10_640_UG_M3']
        PM25 = report['PM25B_UG_M3.PM25T_UG_M3.PM25_640_UG_M3']
        OZONE_PPM = report['OZONE_PPM']
        SONICWS_MPH = report['SONICWS_MPH']
        SONICWD_DEG = report['SONICWD_DEG']
        SIGTHETA_DEG = report['SIGTHETA_DEG']
        
        # Create the "PM10B" node
        PM10_node = Node("PM10", value=PM10, datetime= datetime)
        graph.create(PM10_node)
        # Create the "PM25B" node
        PM2_5_node = Node("PM2_5", value=PM25, datetime= datetime)
        graph.create(PM2_5_node)
        # Create the "OZONE_PPM" node
        OZONE_node = Node("OZONE", PPM=OZONE_PPM, datetime= datetime)
        graph.create(OZONE_node)

        # Create the "WIND" node
        WIND_node = Node("WIND", dev_standar_direction=SIGTHETA_DEG, direction=SONICWD_DEG, speed_mph=SONICWS_MPH, datetime= datetime) 
        graph.create(WIND_node)

        # Create the relationship between "PM10B" and "Sensor"
        relation_PM10B = Relationship(sensor_node, "MEASURED", PM10_node)
        graph.create(relation_PM10B)
        # Create the relationship between "PM25B" and "Sensor"
        relation_PM25B = Relationship(sensor_node, "MEASURED", PM2_5_node)
        graph.create(relation_PM25B)
        # Create the relationship between "OZONE_PPM" and "Sensor"
        relation_OZONE_PPM = Relationship(sensor_node, "MEASURED", OZONE_node)
        graph.create(relation_OZONE_PPM)
        # Create the relationship between "WIND" and "Zipcode"
        relation_WIND = Relationship(sensor_node, "MEASURED", WIND_node)
        graph.create(relation_WIND)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)
        
def insert_esdr_27_neo4j():
    esdr_27 = pd.read_csv(DATA_DIR_ESDR + 'esdr_27.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Lawrenceville 2 ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15201)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_27.iterrows():
        datetime = report['datetime']
        NO_PPB = report['NO_PPB']
        NOY_PPB = report['NOY_PPB']
        CO_PPB = report['CO_PPB']
        SO2_PPB = report['SO2_PPB']
        
        # Create the "NO_PPB" node
        NO_node = Node("NO", PPB=NO_PPB, NOY_PPB_value=NOY_PPB, datetime= datetime)
        graph.create(NO_node)

        # Create the "CO" node
        CO_node = Node("CO", PPB=CO_PPB, datetime= datetime)
        graph.create(CO_node)
        # Create the "SO2" node
        SO2_node = Node("SO2", PPB=SO2_PPB, datetime= datetime)
        graph.create(SO2_node)



        # Create the relationship between "NO_PPB" and "Sensor"
        relation_NO_PPB = Relationship(sensor_node, "MEASURED", NO_node)
        graph.create(relation_NO_PPB)

        # Create the relationship between "CO_PPB" and "Sensor"
        relation_CO_PPB = Relationship(sensor_node, "MEASURED", CO_node)
        graph.create(relation_CO_PPB)
        # Create the relationship between "SO2_PPB" and "Sensor"
        relation_SO2_PPB = Relationship(sensor_node, "MEASURED", SO2_node)
        graph.create(relation_SO2_PPB)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)

def insert_esdr_28_neo4j():
    esdr_28 = pd.read_csv(DATA_DIR_ESDR + 'esdr_28.csv')

    # Create the "Sensor" node 
    sensor_node = Node("Sensor", name='Liberty ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15133 )
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_28.iterrows():
        datetime = report['datetime']
        H2S_PPM = report['H2S_PPM']
        SO2_PPM = report['SO2_PPM']
        SIGTHETA_DEG = report['SIGTHETA_DEG']
        SONICWD_DEG = report['SONICWD_DEG']
        SONICWS_MPH = report['SONICWS_MPH']

        # Create the "H2S" node
        H2S_node = Node("H2S", PPM=H2S_PPM, datetime = datetime)
        graph.create(H2S_node)
        # Create the "SO2" node
        SO2_node = Node("SO2", PPM=SO2_PPM, datetime= datetime)
        graph.create(SO2_node)

        # Create the "WIND" node
        WIND_node = Node("WIND", dev_standar_direction=SIGTHETA_DEG, direction=SONICWD_DEG, speed_mph=SONICWS_MPH, datetime= datetime) 
        graph.create(WIND_node)

        # Create the relationship between "H2S" and "Sensor"
        relation_H2S_PPM = Relationship(sensor_node, "MEASURED", H2S_node)
        graph.create(relation_H2S_PPM)
        # Create the relationship between "SO2" and "Zipcode"
        relation_SO2_PPM = Relationship(sensor_node, "MEASURED",SO2_node )
        graph.create(relation_SO2_PPM)
        # Create the relationship between "WIND" and "Zipcode"
        relation_WIND = Relationship(sensor_node, "MEASURED", WIND_node)
        graph.create(relation_WIND)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)

def insert_esdr_29_neo4j():
    esdr_29 = pd.read_csv(DATA_DIR_ESDR + 'esdr_29.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Liberty 2 ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15133)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_29.iterrows():
        datetime = report['datetime']
        PM10 = report['PM10_UG_M3']
        PM25 = report['PM25_UG_M3.PM25T_UG_M3']


        # Create the "PM10" node
        PM10_node = Node("PM10", value=PM10, datetime= datetime)
        graph.create(PM10_node)
        # Create the "PM25" node
        PM2_5_node = Node("PM2_5", value=PM25, datetime= datetime)
        graph.create(PM2_5_node)


        # Create the relationship between "PM10" and "Sensor"
        relation_PM10 = Relationship(sensor_node, "MEASURED", PM10_node)
        graph.create(relation_PM10)
        # Create the relationship between "PM25" and "Sensor"
        relation_PM25 = Relationship(sensor_node, "MEASURED", PM2_5_node)
        graph.create(relation_PM25)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)

def insert_esdr_43_neo4j():
    esdr_43 = pd.read_csv(DATA_DIR_ESDR + 'esdr_43.csv')
    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Parkway East Near Road ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15221)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_43.iterrows():
        datetime = report['datetime']
        CO_PPB = report['CO_PPB']
        NO2_PPB = report['NO2_PPB']
        NOX_PPB = report['NOX_PPB']
        NO_PPB = report['NO_PPB']
        PM25T_UG_M3 = report['PM25T_UG_M3']
        SIGTHETA_DEG = report['SIGTHETA_DEG']
        SONICWD_DEG = report['SONICWD_DEG']
        SONICWS_MPH = report['SONICWS_MPH']

        # Create the "CO_PPB" node
        CO_node = Node("CO", PPB=CO_PPB, datetime= datetime)
        graph.create(CO_node)

        # Create the "NO2_PPB" node
        NO_node = Node("NO", NO_PPB=NO_PPB, NO2_PPB=NO2_PPB, NOX_PPB=NOX_PPB, datetime= datetime)
        graph.create(NO_node)

        # Create the "PM25T_UG_M3" node
        PM2_5_node = Node("PM2_5", value=PM25T_UG_M3, datetime= datetime)
        graph.create(PM2_5_node)


        # Create the "WIND" node
        WIND_node = Node("WIND", dev_standar_direction=SIGTHETA_DEG, direction=SONICWD_DEG, speed_mph=SONICWS_MPH, datetime= datetime) 
        graph.create(WIND_node)
        
        # Create the relationship between "CO_PPB" and "Sensor"
        relation_CO_PPB = Relationship(sensor_node, "MEASURED", CO_node)
        graph.create(relation_CO_PPB)

        # Create the relationship between "NO" and "Sensor"
        relation_NO = Relationship(sensor_node, "MEASURED", NO_node)
        graph.create(relation_NO)
        # Create the relationship between "PM25T_UG_M3" and "Sensor"
        relation_PM25T_UG_M3 = Relationship(sensor_node, "MEASURED", PM2_5_node)
        graph.create(relation_PM25T_UG_M3)


        # Create the relationship between "WIND" and "Zipcode"
        relation_WIND = Relationship(sensor_node, "MEASURED", WIND_node)
        graph.create(relation_WIND)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)

def insert_esdr_3506_neo4j():
    esdr_3506 = pd.read_csv(DATA_DIR_ESDR + 'esdr_3506.csv')
    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='BAPC 301 39TH STREET BLDG #7 AirNow')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15201)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_3506.iterrows():
        datetime = report['datetime']
        PM2_5 = report['PM2_5']

        # Create the "PM2_5" node
        PM2_5_node = Node("PM2_5", value=PM2_5, datetime= datetime)
        graph.create(PM2_5_node)
        
        # Create the relationship between "PM2_5" and "Sensor"
        relation_PM2_5 = Relationship(sensor_node, "MEASURED", PM2_5_node)
        graph.create(relation_PM2_5)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)

def insert_esdr_3508_neo4j():
    esdr_3508 = pd.read_csv(DATA_DIR_ESDR + 'esdr_3508.csv')
    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='South Allegheny High School AirNow')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15133)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_3508.iterrows():
        datetime = report['datetime']
        PM2_5 = report['PM2_5']

        # Create the "PM2_5" node
        PM2_5_node = Node("PM2_5", PM2_5=PM2_5, datetime= datetime)
        graph.create(PM2_5_node)


        # Create the relationship between "PM2_5" and "Sensor"
        relation_PM2_5 = Relationship(sensor_node, "MEASURED", PM2_5_node)
        graph.create(relation_PM2_5)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)

def insert_esdr_5975_neo4j():
    esdr_5975 = pd.read_csv(DATA_DIR_ESDR + 'esdr_5975.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Parkway East (Near Road) AirNow')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15221)
    graph.merge(zipcode_node, "Zipcode", "value")
    
    for index, report in esdr_5975.iterrows():
        datetime = report['datetime']
        PM2_5 = report['PM2_5']

        # Create the "PM2_5" node
        PM2_5_node = Node("PM2_5", PM2_5=PM2_5, datetime= datetime)
        graph.create(PM2_5_node)

        # Create the relationship between "PM2_5" and "Sensor"
        relation_PM2_5 = Relationship(sensor_node, "MEASURED", PM2_5_node)
        graph.create(relation_PM2_5)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)

