import requests
import pandas as pd
import json
import logging
import awswrangler as wr


def requisicoes(url):
    raw_data = requests.get(url).json()
    results = raw_data['results']

    while raw_data['next']:
        raw_data = requests.get(raw_data['next']).json()
        results.extend(raw_data['results'])
    
    df = _json_parse(results)
    
    return df


def _json_parse(results):
    results = json.dumps(results)
    df = pd.read_json(results)
    return df


def s3_bronze(df,table):
    wr.s3.to_json(
        df=df,
        path=f's3://how-dcb-data-lake-bronze/{table}',
        dataset=True
    )


def s3_silver(df, table):
    wr.s3.to_parquet(
        df=df,
        path=f's3://how-dcb-data-lake-silver/{table}',
        dataset=True,
        partition_cols=['table','year','month','day']
    )


def s3_gold(df, table):
    wr.s3.to_parquet(
        df=df,
        path=f's3://how-dcb-data-lake-gold/{table}',
        dataset=True,
        partition_cols=['table','year','month','day'],
        mode='overwrite_partitions'
    )


def exctract_path(records):
    bucket_name = records['Records'][0]['s3']['bucket']['name']
    object_key = records['Records'][0]['s3']['object']['key']

    path_file_s3 = f's3://{bucket_name}/{object_key}'
    table = object_key.split('/')[0]

    return table, path_file_s3


def read_file_s3(path):
    df = wr.s3.read_json(
        path = path
    )

    return df


def read_file_s3(path):
    df = wr.s3.read_json(
        path = path
    )

    return df


def read_all_files_s3(path):
    df = wr.s3.read_json(
        path = path,
        dataset = True
    )

    return df


def column_types(df, types):
    for type in types.items():
        if type[1] == int:
            df[type[0]] = df[type[0]].replace('unknown',0)
            df[type[0]] = df[type[0]].replace('n/a', 0)
            df[type[0]] = df[type[0]].replace('none', 0)
            df[type[0]] = df[type[0]].replace('indefinite', 0)
        elif type[1] == float:
            df[type[0]] = df[type[0]].replace('unknown',0)
            df[type[0]] = df[type[0]].replace(',','')
        elif type[1] == str:
            df[type[0]] = df[type[0]].replace('n/a', pd.NaT)
            df[type[0]] = df[type[0]].replace('none', pd.NaT)
            df[type[0]] = df[type[0]].replace('unknown',pd.NaT)
        elif type[1] == 'datetime64[ns]':
            df[type[0]] = df[type[0]].replace('unknown',pd.NaT)

    df = df.astype(types)

    return df
    

def create_partition(df, date):
    year = date.strftime('%Y')
    month = date.strftime('%Y-%m')
    day = date.strftime('%Y-%m-%d')

    df['year'] = year
    df['month'] = month
    df['day'] = day
    
    return df


def dedup_gold(df):
    chave = ['url']
    df.sort_values('edited').drop_duplicates(chave, keep = 'last')
    return df


def logs():
    logging.basicConfig(level=logging.INFO)
    return logging