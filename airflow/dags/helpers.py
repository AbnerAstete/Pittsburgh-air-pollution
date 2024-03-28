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
    df_smell_raw = getSmellReports(url,start_dt=start_dt, end_dt=end_dt)
    df_smell_raw.to_csv( DATA_DIR_SMELL_REPORT_RAW + 'smell_report.csv')
    
def getSmellReports(url,**options):
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
    esdr_1['zipcode'] = 15202

    esdr_1.to_csv(DATA_DIR_ESDR + 'esdr_1.csv', index=False)

def clean_esdr_3():
    Feed_3_North_Braddock_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_3_North_Braddock_ACHD_PM10.csv')
    Feed_3_North_Braddock_ACHD_PM10 = Feed_3_North_Braddock_ACHD_PM10.rename(columns={'3.feed_3.PM10B_UG_M3..3.feed_3.PM10_640_UG_M3':'PM10B_UG_M3.PM10_640_UG_M3'})


    Feed_3_North_Braddock_ACHD = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_3_North_Braddock_ACHD.csv')
    Feed_3_North_Braddock_ACHD = Feed_3_North_Braddock_ACHD.rename(columns={'3.feed_3.SO2_PPM': 'SO2_PPM','3.feed_3.SONICWD_DEG':'SONICWD_DEG','3.feed_3.SONICWS_MPH':'SONICWS_MPH','3.feed_3.SIGTHETA_DEG':'SIGTHETA_DEG'})

    esdr_3 = pd.merge(Feed_3_North_Braddock_ACHD_PM10, Feed_3_North_Braddock_ACHD, on='EpochTime')
    esdr_3['zipcode'] = 15104

    esdr_3.to_csv(DATA_DIR_ESDR + 'esdr_3.csv', index=False)

def clean_esdr_23():
    Feed_23_Flag_Plaza_ACHD_CO = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_23_Flag_Plaza_ACHD_CO.csv')
    Feed_23_Flag_Plaza_ACHD_CO = Feed_23_Flag_Plaza_ACHD_CO.rename(columns={'3.feed_23.CO_PPM..3.feed_23.CO_PPB':'CO_PPM.CO_PPB'})


    Feed_23_Flag_Plaza_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_23_Flag_Plaza_ACHD_PM10.csv')
    Feed_23_Flag_Plaza_ACHD_PM10 = Feed_23_Flag_Plaza_ACHD_PM10.rename(columns={'3.feed_23.PM10_UG_M3':'PM10_UG_M3'})

    esdr_23 = pd.merge(Feed_23_Flag_Plaza_ACHD_PM10, Feed_23_Flag_Plaza_ACHD_CO, on='EpochTime')
    esdr_23['zipcode'] = 15219
    esdr_23.to_csv(DATA_DIR_ESDR + 'esdr_23.csv', index=False)

def clean_esdr_24():
    Feed_24_Glassport_High_Street_ACHD = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_24_Glassport_High_Street_ACHD.csv')
    esdr_24 = Feed_24_Glassport_High_Street_ACHD.rename(columns={'3.feed_24.PM10_UG_M3':'PM10_UG_M3'})

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
    esdr_26['zipcode'] = 15201

    esdr_26.to_csv(DATA_DIR_ESDR + 'esdr_26.csv', index=False)

def clean_esdr_27():
    Feed_27_Lawrenceville_2_ACHD = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_27_Lawrenceville_2_ACHD.csv')
    esdr_27 = Feed_27_Lawrenceville_2_ACHD.rename(columns={'3.feed_27.NO_PPB':'NO_PPB','3.feed_27.NOY_PPB':'NOY_PPB','3.feed_27.CO_PPB':'CO_PPB','3.feed_27.SO2_PPB':'SO2_PPB'})

    esdr_27['zipcode'] = 15201
    esdr_27.to_csv(DATA_DIR_ESDR + 'esdr_27.csv', index=False)

def clean_esdr_28():
    Feed_28_Liberty_ACHD = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_28_Liberty_ACHD.csv')
    esdr_28 = Feed_28_Liberty_ACHD.rename(columns={'3.feed_28.H2S_PPM': 'H2S_PPM','3.feed_28.SO2_PPM':'SO2_PPM','3.feed_28.SIGTHETA_DEG':'SIGTHETA_DEG','3.feed_28.SONICWD_DEG':'SONICWD_DEG','3.feed_28.SONICWS_MPH':'SONICWS_MPH'})

    esdr_28['zipcode'] = 15133
    esdr_28.to_csv(DATA_DIR_ESDR + 'esdr_28.csv', index=False)

def clean_esdr_29():
    Feed_29_Liberty_2_ACHD_PM10 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_29_Liberty_2_ACHD_PM10.csv')
    Feed_29_Liberty_2_ACHD_PM10 = Feed_29_Liberty_2_ACHD_PM10.rename(columns={'3.feed_29.PM10_UG_M3': 'PM10_UG_M3'})

    Feed_29_Liberty_2_ACHD_PM25 = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_29_Liberty_2_ACHD_PM25.csv')
    Feed_29_Liberty_2_ACHD_PM25 = Feed_29_Liberty_2_ACHD_PM25.rename(columns={'3.feed_29.PM25_UG_M3..3.feed_29.PM25T_UG_M3': 'PM25_UG_M3.PM25T_UG_M3'})

    esdr_29 = pd.merge(Feed_29_Liberty_2_ACHD_PM10, Feed_29_Liberty_2_ACHD_PM25, on='EpochTime')
    esdr_29['zipcode'] = 15133
    esdr_29.to_csv(DATA_DIR_ESDR + 'esdr_29.csv', index=False)

def clean_esdr_43():
    Feed_43_and_Feed_11067_Parkway_East_ACHD= pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_43_and_Feed_11067_Parkway_East_ACHD.csv')
    esdr_43 = Feed_43_and_Feed_11067_Parkway_East_ACHD.rename(columns={'3.feed_11067.CO_PPB..3.feed_43.CO_PPB': 'CO_PPB','3.feed_11067.NO2_PPB..3.feed_43.NO2_PPB':'NO2_PPB','3.feed_11067.NOX_PPB..3.feed_43.NOX_PPB':'NOX_PPB','3.feed_11067.NO_PPB..3.feed_43.NO_PPB':'NO_PPB','3.feed_11067.PM25T_UG_M3..3.feed_43.PM25T_UG_M3':'PM25T_UG_M3','3.feed_11067.SIGTHETA_DEG..3.feed_43.SIGTHETA_DEG':'SIGTHETA_DEG','3.feed_11067.SONICWD_DEG..3.feed_43.SONICWD_DEG':'SONICWD_DEG','3.feed_11067.SONICWS_MPH..3.feed_43.SONICWS_MPH':'SONICWS_MPH'})

    esdr_43['zipcode'] = 15221
    esdr_43.to_csv(DATA_DIR_ESDR + 'esdr_43.csv', index=False)

def clean_esdr_3506():
    Feed_3506_BAPC_301_39TH_STREET_BLDG_AirNow = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_3506_BAPC_301_39TH_STREET_BLDG_AirNow.csv')
    esdr_3506 = Feed_3506_BAPC_301_39TH_STREET_BLDG_AirNow.rename(columns={'3.feed_3506.PM2_5': 'PM2_5','3.feed_3506.OZONE':'OZONE'})

    esdr_3506['zipcode'] = 15201
    esdr_3506.to_csv(DATA_DIR_ESDR + 'esdr_3506.csv', index=False)

def clean_esdr_3508():
    Feed_3508_South_Allegheny_High_School_AirNow = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_3508_South_Allegheny_High_School_AirNow.csv')
    esdr_3508 = Feed_3508_South_Allegheny_High_School_AirNow.rename(columns={'3.feed_3508.PM2_5': 'PM2_5'})

    esdr_3508['zipcode'] = 15133
    esdr_3508.to_csv(DATA_DIR_ESDR + 'esdr_3508.csv', index=False)

def clean_esdr_5975():
    Feed_5975_Parkway_East_AirNow = pd.read_csv(DATA_DIR_ESDR_RAW + 'Feed_5975_Parkway_East_AirNow.csv')
    esdr_5975 = Feed_5975_Parkway_East_AirNow.rename(columns={'3.feed_5975.PM2_5': 'PM2_5'})
    
    esdr_5975['zipcode'] = 15221
    esdr_5975.to_csv(DATA_DIR_ESDR + 'esdr_5975.csv', index=False)


def clean_smell_report():
    info_zipcode = pd.read_json('data/json/esdr_metadata.json')
    smell_report = pd.read_csv(DATA_DIR_SMELL_REPORT_RAW + 'smell_report.csv')

    # Iterar sobre cada fila del DataFrame info_zipcode
    for index, row in info_zipcode.iterrows():
        # Verificar si el código postal está presente en smell_report
        if row['zipcode'] in smell_report['zipcode'].values:
            # Actualizar el valor de 'name' en smell_report con el valor correspondiente de 'name' en info_zipcode
            smell_report.loc[smell_report['zipcode'] == row['zipcode'], 'name'] = row['name']

    # Convertir EpochTime a datetime
    smell_report['DateTime'] = pd.to_datetime(smell_report['EpochTime'], unit='s').dt.strftime('%d-%m-%Y')

    # Guardar smell_report como JSON
    smell_report.to_json(DATA_DIR_SMELL_REPORT + 'smell_report.json', orient='records')

def insert_smell_data_in_neo4j():
    graph.delete_all()

    # Leer los datos como DataFrame
    smell_report = pd.read_json(DATA_DIR_SMELL_REPORT + 'smell_report.json')

    for index, report in smell_report.iterrows():
        date = report['DateTime'].strftime('%d-%m-%Y')  # Formato YYYY-MM-DD
        epoch = str(report['EpochTime'])
        zipcode = report['zipcode']
        name_zipcode = report['name']
        # lat = report['skewed_latitude']
        # lon = report['skewed_longitude']
        smell_value = report['smell_value']
        smell_description = report['smell_description']
        feelings_symptoms = report['feelings_symptoms']
        additional_comments = report['additional_comments']

        # # Crear el nodo "Day"
        date_node = Node("Day", date=date)
        graph.merge(date_node, "Day", "date")

        # # Crear el nodo "Smell"
        smell_node = Node("Smell", epoch=epoch ,smell_value=smell_value, smell_description=smell_description, feelings_symptoms=feelings_symptoms, additional_comments=additional_comments)
        graph.merge(smell_node, "Smell", "epoch")
        
        # # Crear la relación entre "Day" y "Smell"
        relation_smell = Relationship(date_node, "REPORTED_ON", smell_node)
        graph.create(relation_smell)

        # Crear el nodo "Zipcode"
        zipcode_node = Node("Zipcode",name_zipcode=name_zipcode, zipcode=zipcode)
        graph.merge(zipcode_node, "Zipcode", "zipcode")
        
        # # # Crear la relación entre "Smell" y "Zipcode"
        relation_zipcode = Relationship(smell_node, "REPORTED_IN", zipcode_node)
        graph.create(relation_zipcode)

