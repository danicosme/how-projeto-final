import requests
import pandas as pd
import json
import logging
import awswrangler as wr


def requisicoes(url):
    """Função que faz a requisição na API para coletar dados.

    Args:
        url: link da api

    Returns:
        df: dataframe com dados coletados da api.
    """
    raw_data = requests.get(url).json()
    results = raw_data['results']

    while raw_data['next']:
        raw_data = requests.get(raw_data['next']).json()
        results.extend(raw_data['results'])
    
    df = _json_parse(results)
    
    return df


def _json_parse(results):
    """Função interna que realiza a transformação do json em dataframe e devolve para a função de coleta de dados da api.

    Args:
        results: 

    Returns:
        df: dataframe com dados coletados da api.
    """
    results = json.dumps(results)
    df = pd.read_json(results)
    return df


def s3_bronze(df,table):
    """Função que realiza a escrita dos dados no s3 na camada Bronze.

    Args:
        df: dataframe com dados a serem salvos no s3
        table: nome da tabela a ser salva no s3
    """
    wr.s3.to_json(
        df=df,
        path=f's3://how-dcb-data-lake-bronze/{table}',
        dataset=True
    )


def s3_silver(df, table):
    """Função que realiza a escrita dos dados no s3 na camada Silver.

    Args:
        df: dataframe com dados a serem salvos no s3
        table: nome da tabela a ser salva no s3
    """
    wr.s3.to_parquet(
        df=df,
        path=f's3://how-dcb-data-lake-silver/{table}',
        dataset=True,
        partition_cols=['table','year','month','day']
    )


def s3_gold(df, table):
    """Função que realiza a escrita dos dados no s3 na camada Gold.

    Args:
        df: dataframe com dados a serem salvos no s3
        table: nome da tabela a ser salva no s3
    """
    wr.s3.to_parquet(
        df=df,
        path=f's3://how-dcb-data-lake-gold/{table}',
        dataset=True,
        partition_cols=['table','year','month','day'],
        mode='overwrite_partitions'
    )


def exctract_path(records):
    """Função que coleta nome do bucket e objeto da mensagem sqs

    Args:
        records: mensagem sqs.

    Returns:
        table: nome da tabela correspondente ao arquivo
        path_file_s3: caminho do s3 completo do arquivo
    """
    bucket_name = records['Records'][0]['s3']['bucket']['name']
    object_key = records['Records'][0]['s3']['object']['key']

    path_file_s3 = f's3://{bucket_name}/{object_key}'
    table = object_key.split('/')[0]

    return table, path_file_s3


def read_file_s3(path):
    """Função que faz a leitura de um único arquivo do s3;

    Args:
        path: caminho do s3 onde o arquivo está localizado.

    Returns:
        df: retorna o dataframe lido do s3.
    """
    df = wr.s3.read_json(
        path = path
    )

    return df


def read_all_files_s3(path):
    """Função que faz a leitura de várias partições pertinentes a uma tabela do s3.

    Args:
        path: caminho do s3 onde onde a tabela está localizada.

    Returns:
        df: retorna o dataframe lido do s3.
    """
    df = wr.s3.read_json(
        path = path,
        dataset = True
    )

    return df


def column_types(df, types):
    """Função que retorna dataframe com colunas tratadas.

    Args:
        df: dataframe sem tratamentos
        types: tipos de dados para aplicar em cada coluna

    Returns:
        df: dataframe tratado.
    """
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
    """Função que divide a data em ano, mês e dia para criação das partições no s3.

    Args:
        df: dataframe que será salvo no s3
        date: data atual que será utilizada para particionamento

    Returns:
        df: dataframe com as colunas ano, mês e dia.
    """
    year = date.strftime('%Y')
    month = date.strftime('%Y-%m')
    day = date.strftime('%Y-%m-%d')

    df['year'] = year
    df['month'] = month
    df['day'] = day
    
    return df


def dedup_gold(df):
    """Função que realiza a deduplicação de dados para a camada gold.

    Args:
        df: dataframe com duplicidades

    Returns:
        df: dataframe deduplicado
    """
    chave = ['url']
    df.sort_values('edited').drop_duplicates(chave, keep = 'last')
    return df


def logs():
    """Função com a configuração de logs (Info ou Debug).

    Returns:
        logging: retorna configuração de nível de log.
    """
    logging.basicConfig(level=logging.INFO)
    return logging