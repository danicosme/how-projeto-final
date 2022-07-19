import pandas as pd
import logging
import awswrangler as wr


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
