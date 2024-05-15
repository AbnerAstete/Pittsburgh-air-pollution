import os
from datetime import datetime
  
import pandas as pd
import json
import nltk 
from nltk.tokenize import word_tokenize


from py2neo import Node, Relationship, Graph, NodeMatcher

BASE_DIR = '/opt/airflow/'
DATA_DIR = os.path.join(BASE_DIR, 'data')
DATA_DIR_COR = 'data/correlation_analisys/'

#connection with neo4j
graph = Graph("bolt://neo:7687")
neo4j_session = graph.begin()


def insertData_correlation():

    df = pd.read_csv(DATA_DIR + DATA_DIR_COR + 'correlation_merge.csv')
    for index, row in df.iterrows():
        datetime = row['datetime']

