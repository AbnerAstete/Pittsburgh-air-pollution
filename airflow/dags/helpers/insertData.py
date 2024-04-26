import os
from datetime import datetime
  
import pandas as pd
import json
import nltk 
from nltk.tokenize import word_tokenize


from py2neo import Node, Relationship, Graph, NodeMatcher

BASE_DIR = '/opt/airflow/'
DATA_DIR = os.path.join(BASE_DIR, 'data')
DATA_DIR_META = 'datqwwa/metadata/'
DATA_DIR_ESDR = 'data/esdr/clean/'
DATA_DIR_SMELL_REPORT = 'data/smell_report/'

#connection with neo4j
graph = Graph("bolt://neo:7687")
neo4j_session = graph.begin()

def delete_all():
    graph.delete_all()

def find_keywords_odor(description):
    keywords = ['industrial', 'sulphur','diesel', 'egg', 'burning plastic', 'smoke', 'woodsmoke', 'acrid', 'coke', 'tar', 'smog', 'chemical', 'sewage', 'gas', 'trash', 'coal']
    found_keywords = []

    # Tokenizar la descripción
    tokens = word_tokenize(description.lower())
    
    # Buscar coincidencias con las palabras clave
    for keyword in keywords:
        if keyword in tokens:
            found_keywords.append(keyword)
    
    return found_keywords

def find_keywords_symptoms(description):
    keywords = ['headache', 'throat', 'irritat','eye' ,'cough', 'nausea', 'asthma', 'anxiety']
    found_keywords = []
    tokens = word_tokenize(description.lower())
    
    # Buscar coincidencias con las palabras clave
    for keyword in keywords:
        if keyword in tokens:
            found_keywords.append(keyword)
    
    return found_keywords

def insert_smell_data_in_neo4j():
    nltk.download('punkt')
    smell_report = pd.read_csv(DATA_DIR_SMELL_REPORT + 'smell_report.csv')

    for index, report in smell_report.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        zipcode = report['zipcode']
        lat = report['skewed_latitude']
        lon = report['skewed_longitude']
        smell_value = report['smell_value']
        smell_description = report['smell_description']
        feelings_symptoms = report['feelings_symptoms']
        additional_comments = report['additional_comments']

        # Create the "Smell" node
        smell_node = Node("Smell", datetime=report_datetime ,smell_value=smell_value,smell_description=smell_description, feelings_symptoms=feelings_symptoms, additional_comments=additional_comments, lat=lat, lon=lon)
        graph.merge(smell_node, "Smell", "datetime")

        if pd.notnull(smell_description):
            # Create the "Description" nodes and relationships
            description_keywords = find_keywords_odor(smell_description)
            for keyword in description_keywords:
                odor = Node("Odor", keyword=keyword)
                graph.merge(odor, "Odor", "keyword")
                relation_description = Relationship(smell_node, "HAS_ODOR", odor)
                graph.create(relation_description)
        
        if pd.notnull(feelings_symptoms):
            symptom_keywords = find_keywords_symptoms(feelings_symptoms)
            for keyword in symptom_keywords:
                symptom = Node("Symptom", keyword=keyword)
                graph.merge(symptom, "Symptom", "keyword")
                relation_symptom = Relationship(smell_node, "HAS_SYMPTOM", symptom)
                graph.create(relation_symptom)

        # Create the "Zipcode" node
        zipcode_node = Node("Zipcode", value=zipcode)
        graph.merge(zipcode_node, "Zipcode", "value")
        # Create the relationship between "Smell" and "Zipcode"
        relation_zipcode = Relationship(smell_node, "REPORTED_IN", zipcode_node)
        graph.create(relation_zipcode)

def inster_nearby_zipcodes():

    with open('data/metadata/zipcode_neighbors.json') as f:
        zipcode_neighbors = json.load(f)

    for zipcode, neighbors in zipcode_neighbors.items():
        
        zipcode = int(zipcode)
        # Create or update the current zipcode node
        zipcode_node = Node("Zipcode", value=zipcode)
        graph.merge(zipcode_node, "Zipcode", "value")
        
        for direction, neighbor_list in neighbors.items():
            for neighbor in neighbor_list:
                if neighbor:
                    neighbor = int(neighbor)
                    # Create or update the neighboring zipcode node
                    neighbor_node = Node("Zipcode", value=neighbor)
                    graph.merge(neighbor_node, "Zipcode", "value")
                    
                    # Create a CLOSE_TO relationship between the current zipcode and its neighbor
                    relation_close_to = Relationship(zipcode_node, "CLOSE_TO", neighbor_node, direction=direction)
                    graph.create(relation_close_to)


def insert_esdr_1_neo4j():
    esdr_1 = pd.read_csv(DATA_DIR_ESDR + 'esdr_1.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name= 'Avalon ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15202)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_1.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        PM25B = report['PM25B_UG_M3.PM25T_UG_M3.PM25_640_UG_M3']
        SO2_PPM = report['SO2_PPM']
        H2S_PPM = report['H2S_PPM']
        SIGTHETA_DEG = report['SIGTHETA_DEG']
        SONICWD_DEG = report['SONICWD_DEG']
        SONICWS_MPH = report['SONICWS_MPH']

        # Create the "PM2_5" node
        PM2_5_node = Node("PM2_5", value=PM25B, datetime= report_datetime)
        graph.create(PM2_5_node)
        # Create the "SO2" node
        SO2_node = Node("SO2", PPM=SO2_PPM, datetime= report_datetime)
        graph.create(SO2_node)
        # Create the "H2S" node
        H2S_node = Node("H2S", PPM=H2S_PPM, datetime= report_datetime)
        graph.create(H2S_node)

        # Create the "WIND" node
        WIND_node = Node("WIND", dev_standar_direction=SIGTHETA_DEG, direction=SONICWD_DEG, speed_mph=SONICWS_MPH, datetime= report_datetime) 
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
    
    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_1', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    
    # Save intermediate DataFrame
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)

def insert_esdr_3_neo4j():
    esdr_3 = pd.read_csv(DATA_DIR_ESDR + 'esdr_3.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='North Braddock ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15104)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_3.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        PM10B = report['PM10B_UG_M3.PM10_640_UG_M3'] 
        SO2_PPM = report['SO2_PPM']
        SONICWD_DEG = report['SONICWD_DEG']
        SONICWS_MPH = report['SONICWS_MPH']
        SIGTHETA_DEG = report['SIGTHETA_DEG']

        # Create the "PM10B" node
        PM10_node = Node("PM10", value=PM10B, datetime= report_datetime)
        graph.create(PM10_node)
        # Create the "SO2_PPM" node
        SO2_node = Node("SO2", PPM=SO2_PPM, datetime= report_datetime)
        graph.create(SO2_node)
        # Create the "WIND" node
        WIND_node = Node("WIND", dev_standar_direction=SIGTHETA_DEG, direction=SONICWD_DEG, speed_mph=SONICWS_MPH, datetime= report_datetime) 
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
    
    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_3', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)

def insert_esdr_23_neo4j():
    esdr_23 = pd.read_csv(DATA_DIR_ESDR + 'esdr_23.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Flag Plaza ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15219)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_23.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        PM10 = report['PM10_UG_M3']
        CO = report['CO_PPM.CO_PPB']

        # Create the "PM10" node
        PM10_node = Node("PM10", value=PM10, datetime= report_datetime)
        graph.create(PM10_node)

        # Create the "CO" node
        CO_node = Node("CO", PPM_PPB=CO, datetime= report_datetime)
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
    
    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_23', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)

def insert_esdr_24_neo4j():
    esdr_24 = pd.read_csv(DATA_DIR_ESDR + 'esdr_24.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Glassport High Street ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15045)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_24.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        PM10 = report['PM10_UG_M3']

        # Create the "PM10" node
        PM10_node = Node("PM10", value=PM10, datetime= report_datetime)
        graph.create(PM10_node)

        # Create the relationship between "PM10" and "Sensor"
        relation_PM10 = Relationship(sensor_node, "MEASURED", PM10_node)
        graph.create(relation_PM10)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)
    
    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_24', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)
      
def insert_esdr_26_neo4j():
    esdr_26 = pd.read_csv(DATA_DIR_ESDR + 'esdr_26.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Lawrenceville ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15201)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_26.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        PM10 = report['PM10B_UG_M3.PM10_640_UG_M3']
        PM25 = report['PM25B_UG_M3.PM25T_UG_M3.PM25_640_UG_M3']
        OZONE_PPM = report['OZONE_PPM']
        SONICWS_MPH = report['SONICWS_MPH']
        SONICWD_DEG = report['SONICWD_DEG']
        SIGTHETA_DEG = report['SIGTHETA_DEG']
        
        # Create the "PM10B" node
        PM10_node = Node("PM10", value=PM10, datetime= report_datetime)
        graph.create(PM10_node)
        # Create the "PM25B" node
        PM2_5_node = Node("PM2_5", value=PM25, datetime= report_datetime)
        graph.create(PM2_5_node)
        # Create the "OZONE_PPM" node
        OZONE_node = Node("OZONE", PPM=OZONE_PPM, datetime= report_datetime)
        graph.create(OZONE_node)

        # Create the "WIND" node
        WIND_node = Node("WIND", dev_standar_direction=SIGTHETA_DEG, direction=SONICWD_DEG, speed_mph=SONICWS_MPH, datetime= report_datetime) 
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

    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_26', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)
        
def insert_esdr_27_neo4j():
    esdr_27 = pd.read_csv(DATA_DIR_ESDR + 'esdr_27.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Lawrenceville 2 ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15201)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_27.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        NO_PPB = report['NO_PPB']
        NOY_PPB = report['NOY_PPB']
        CO_PPB = report['CO_PPB']
        SO2_PPB = report['SO2_PPB']
        
        # Create the "NO_PPB" node
        NO_node = Node("NO", PPB=NO_PPB, NOY_PPB_value=NOY_PPB, datetime= report_datetime)
        graph.create(NO_node)

        # Create the "CO" node
        CO_node = Node("CO", PPB=CO_PPB, datetime= report_datetime)
        graph.create(CO_node)

        # Create the "SO2" node
        SO2_node = Node("SO2", PPB=SO2_PPB, datetime= report_datetime)
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

    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_27', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)

def insert_esdr_28_neo4j():
    esdr_28 = pd.read_csv(DATA_DIR_ESDR + 'esdr_28.csv')

    # Create the "Sensor" node 
    sensor_node = Node("Sensor", name='Liberty ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15133 )
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_28.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        H2S_PPM = report['H2S_PPM']
        SO2_PPM = report['SO2_PPM']
        SIGTHETA_DEG = report['SIGTHETA_DEG']
        SONICWD_DEG = report['SONICWD_DEG']
        SONICWS_MPH = report['SONICWS_MPH']

        # Create the "H2S" node
        H2S_node = Node("H2S", PPM=H2S_PPM, datetime = report_datetime)
        graph.create(H2S_node)
        # Create the "SO2" node
        SO2_node = Node("SO2", PPM=SO2_PPM, datetime= report_datetime)
        graph.create(SO2_node)
        # Create the "WIND" node
        WIND_node = Node("WIND", dev_standar_direction=SIGTHETA_DEG, direction=SONICWD_DEG, speed_mph=SONICWS_MPH, datetime= report_datetime) 
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
    
    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_28', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)

def insert_esdr_29_neo4j():
    esdr_29 = pd.read_csv(DATA_DIR_ESDR + 'esdr_29.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Liberty 2 ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15133)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_29.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        PM10 = report['PM10_UG_M3']
        PM25 = report['PM25_UG_M3.PM25T_UG_M3']


        # Create the "PM10" node
        PM10_node = Node("PM10", value=PM10, datetime= report_datetime)
        graph.create(PM10_node)
        # Create the "PM25" node
        PM2_5_node = Node("PM2_5", value=PM25, datetime= report_datetime)
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

    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_29', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)

def insert_esdr_43_neo4j():
    esdr_43 = pd.read_csv(DATA_DIR_ESDR + 'esdr_43.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Parkway East Near Road ACHD')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15221)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_43.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        CO_PPB = report['CO_PPB']
        NO2_PPB = report['NO2_PPB']
        NOX_PPB = report['NOX_PPB']
        NO_PPB = report['NO_PPB']
        PM25T_UG_M3 = report['PM25T_UG_M3']
        SIGTHETA_DEG = report['SIGTHETA_DEG']
        SONICWD_DEG = report['SONICWD_DEG']
        SONICWS_MPH = report['SONICWS_MPH']

        # Create the "CO_PPB" node
        CO_node = Node("CO", PPB=CO_PPB, datetime= report_datetime)
        graph.create(CO_node)

        # Create the "NO2_PPB" node
        NO_node = Node("NO", NO_PPB=NO_PPB, NO2_PPB=NO2_PPB, NOX_PPB=NOX_PPB, datetime= report_datetime)
        graph.create(NO_node)

        # Create the "PM25T_UG_M3" node
        PM2_5_node = Node("PM2_5", value=PM25T_UG_M3, datetime= report_datetime)
        graph.create(PM2_5_node)


        # Create the "WIND" node
        WIND_node = Node("WIND", dev_standar_direction=SIGTHETA_DEG, direction=SONICWD_DEG, speed_mph=SONICWS_MPH, datetime= report_datetime) 
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

    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_43', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)

def insert_esdr_3506_neo4j():
    esdr_3506 = pd.read_csv(DATA_DIR_ESDR + 'esdr_3506.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='BAPC 301 39TH STREET BLDG #7 AirNow')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15201)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_3506.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        PM2_5 = report['PM2_5']

        # Create the "PM2_5" node
        PM2_5_node = Node("PM2_5", value=PM2_5, datetime= report_datetime)
        graph.create(PM2_5_node)
        
        # Create the relationship between "PM2_5" and "Sensor"
        relation_PM2_5 = Relationship(sensor_node, "MEASURED", PM2_5_node)
        graph.create(relation_PM2_5)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)
    
    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_3506', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_temp.csv', mode='a', header=False, index=False)

def insert_esdr_3508_neo4j():
    esdr_3508 = pd.read_csv(DATA_DIR_ESDR + 'esdr_3508.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='South Allegheny High School AirNow')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15133)
    graph.merge(zipcode_node, "Zipcode", "value")

    for index, report in esdr_3508.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        PM2_5 = report['PM2_5']

        # Create the "PM2_5" node
        PM2_5_node = Node("PM2_5", value=PM2_5, datetime= report_datetime)
        graph.create(PM2_5_node)


        # Create the relationship between "PM2_5" and "Sensor"
        relation_PM2_5 = Relationship(sensor_node, "MEASURED", PM2_5_node)
        graph.create(relation_PM2_5)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)
    
    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_3508', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)

def insert_esdr_5975_neo4j():
    esdr_5975 = pd.read_csv(DATA_DIR_ESDR + 'esdr_5975.csv')

    # Create the "Sensor" node
    sensor_node = Node("Sensor", name='Parkway East (Near Road) AirNow')
    graph.merge(sensor_node, "Sensor", "name")
    # Create the "Zipcode" node
    zipcode_node = Node("Zipcode", value= 15221)
    graph.merge(zipcode_node, "Zipcode", "value")

    
    for index, report in esdr_5975.iterrows():
        datetime_str = report['datetime']
        report_datetime = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        PM2_5 = report['PM2_5']

        # Create the "PM2_5" node
        PM2_5_node = Node("PM2_5", value=PM2_5, datetime= report_datetime)
        graph.create(PM2_5_node)

        # Create the relationship between "PM2_5" and "Sensor"
        relation_PM2_5 = Relationship(sensor_node, "MEASURED", PM2_5_node)
        graph.create(relation_PM2_5)
        # Create the relationship between "Sensor" and "Zipcode"
        relation_sensor = Relationship(sensor_node, "LOCATED_IN", zipcode_node)
        graph.create(relation_sensor)

    #df_metrics = pd.concat([df_metrics, pd.DataFrame([{'source': 'esdr_5975', 'nodes': total_nodes, 'edges': total_edges}])], ignore_index=True)
    #df_metrics.to_csv(DATA_DIR_META + 'metrics_neo4j.csv', mode='a', header=False, index=False)
