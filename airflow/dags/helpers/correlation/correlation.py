import pandas as pd
from os import listdir
from os.path import isfile, join


def mergeEsdrData(data):
    # Resample data
    df = resampleData(data.pop(0), method="mean").reset_index()
    while len(data) != 0:
        df = pd.merge_ordered(df, resampleData(data.pop(0), method="mean").reset_index(),
            on="DateTime", how="outer", fill_method=None)

    # Fill NaN with -1
    df = df.fillna(-1)
    return df

def resampleData(df, method="mean", rule="60Min"):
    df = df.copy(deep=True)
    # Because we want data from the past, so label need to be "right"
    df = epochtimeIdxToDatetime(df).resample(rule, label="right")
    if method == "sum":
        return df.sum()
    elif method == "count":
        return df.count()
    elif method == "mean":
        return df.mean()
    else:
        return df.mean()
    
def epochtimeIdxToDatetime(df):
    """Convert the epochtime index in a pandas dataframe to datetime index"""
    df = df.copy(deep=True)
    df.sort_index(inplace=True)
    df.index = pd.to_datetime(df.index, unit="s", utc=True)
    df.index.name = "DateTime"
    return df

def getAllFileNamesInFolder(path):
    """Return a list of all files in a folder"""
    return  [f for f in listdir(path) if isfile(join(path, f))]

def correlation_analysis_esdr(path):
    df_esdr_array_raw = []
    for f in getAllFileNamesInFolder(path):
        if ".csv" in f:
            df_esdr_array_raw.append(pd.read_csv(path + f, index_col="EpochTime"))

    df_esdr = mergeEsdrData(df_esdr_array_raw)
    df_esdr.to_csv('data/correlation_analisys/correlation_esdr.csv',index=False)

def aggregateSmellData(df):
    if df is None: return None

    # Select only the reports within the range of 3 and 5
    df = df[(df["smell_value"]>=3)&(df["smell_value"]<=5)]

    # If empty, return None
    if df.empty:
        return None

    # Group by zipcode and output a vector with zipcodes
    # TODO: need to merge the reports submitted by the same user in an hour with different weights
    # TODO: for example, starting from the n_th reports, give them discounted weights, like 0.25
    data = []
    for z, df_z in df.groupby("zipcode"):
        # Select only smell values
        df_z = df_z["smell_value"]
        # Resample data
        df_z = resampleData(df_z, method="sum")
        df_z.name = z
        data.append(df_z)

    # Merge all
    df = data.pop(0).reset_index()
    while len(data) != 0:
        df = pd.merge_ordered(df, data.pop(0).reset_index(), on="DateTime", how="outer", fill_method=None)

    # Fill NaN with 0
    df = df.fillna(0)

    return df

def correlation_analysis_smell(path):
    df_smell_analysis = pd.read_csv(path, index_col="EpochTime")
    df_smell_analysis = aggregateSmellData(df_smell_analysis)
    df_smell_analysis.to_csv('data/correlation_analisys/correlation_smell.csv',index=False)


def mergeDataFrames(esdr_path, smell_path):

    df_esdr = pd.read_csv(esdr_path, index_col="DateTime")
    df_smell = pd.read_csv(smell_path, index_col="DateTime")

    df_merge = pd.merge_ordered(df_esdr.reset_index(), df_smell.reset_index(), on="DateTime", how="left", fill_method=None)
    df_merge = df_merge.fillna(0)
    df_merge.to_csv('data/correlation_analisys/correlation_merge.csv',index=False)
    

