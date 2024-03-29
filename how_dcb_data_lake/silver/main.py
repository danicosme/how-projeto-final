from how_dcb_data_lake.functions import logs, exctract_path, read_file_s3, column_types, s3_silver, create_partition
from how_dcb_data_lake.constants import *
import json
import datetime


def lambda_handler(event,context):
    logger = logs()

    try:
        records = json.loads(event['Message'])

        logger.info('Processamento de envio da camada Bronze para Silver iniciado')

        if 'Records' in records:
            table, path_file_s3 = exctract_path(records)
            
            logger.info(f'Iniciando leitura do arquivo {path_file_s3} da camada Bronze')
            df = read_file_s3(path_file_s3)

            logger.info(f'Criando partições de tablea, ano, mes e dia')
            df['table'] = table

            date = datetime.date.today()
            df = create_partition(df, date)

            logger.info(f'Iniciando tratamento de tipos de dados')
            if table == 'films':
                 data_type = film_types
            elif table == 'peoples':
                data_type = peoples_types
            elif table == 'planets':
                data_type = planets_types
            elif table == 'species':
                data_type = species_types
            elif table == 'starships':
                data_type = starships_types
            elif table == 'vehicles':
                data_type = vehicles_types
            else:
                logger.warning('Tabela não identificada')
            
            df = column_types(df, data_type)
            
            logger.info(f'Iniciando escrita do arquivo em formato .parquet na camada Silver')
            s3_silver(df,table)

            logger.info('Processo de ingestão nada camada Silver concluído com sucesso')
            
    except Exception as e:
        logger.error(f'Erro durante o processo de envio dos dados para a camada Silver: {e}')
        

def main(event):
    lambda_handler(event,None)


if __name__ == '__main__':
    event = {
    "Type" : "Notification",
    "MessageId" : "f2d241b9-6e23-5687-a0bf-1af5813ef285",
    "TopicArn" : "arn:aws:sns:us-east-1:130968353318:teste",
    "Subject" : "Amazon S3 Notification",
    "Message" : "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\",\"eventTime\":\"2022-07-16T16:52:44.585Z\",\"eventName\":\"ObjectCreated:Copy\",\"userIdentity\":{\"principalId\":\"A3THE6D8GC0B0G\"},\"requestParameters\":{\"sourceIPAddress\":\"189.62.149.97\"},\"responseElements\":{\"x-amz-request-id\":\"0QCP14F9Y7MBNT9A\",\"x-amz-id-2\":\"wz/C1tPl4xkJQJaWqfqNctOsiHep1Rql34mL0V2B5tXXFhRT0X8EYVH5e2d7k2M0gJZhGL0jSnoLaDYABIBXa8bAvtnr5eSD\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"teste\",\"bucket\":{\"name\":\"how-dcb-data-lake-bronze\",\"ownerIdentity\":{\"principalId\":\"A3THE6D8GC0B0G\"},\"arn\":\"arn:aws:s3:::how-dcb-data-lake-bronze\"},\"object\":{\"key\":\"films/ea7aa88c7d6f4d7189241dee02a3410d.json\",\"size\":34943,\"eTag\":\"b94397441187b3319486700e09b2b19a\",\"sequencer\":\"0062D2ECDC85F8046B\"}}}]}",
    "Timestamp" : "2022-07-16T16:52:45.410Z",
    "SignatureVersion" : "1",
    "Signature" : "Y/Gnd3BkVFeTSMUr/RPenB90IJMEOuLVuAniwimFFi9ifUfLJ1EPLEj/lY1wcZyDES9uXzSf6p9Bqn6X6fkmrFZT4E7h8GCBLxF31o9XAYztB6b6pRBUz3HBawLKUKsygrP8HR/lSEEirlR2qAtD78G+1HFAXBTafJm74px7t/9005uvhmWUfxLc39nGIhLpucl7p2R4XwedP208WGIuwZ6WLuubiZq0yTSrz8pPZd//wfxlWiWOvR21tO8GOBxfC8zBnbphBRO67WxHwdY9sfeqcIszF3ogVX9LzpoAxPoKrjDLDO37kRlTc/xpT8yrdydSRtwTKP55TT2XknC/dQ==",
    "SigningCertURL" : "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",
    "UnsubscribeURL" : "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:130968353318:teste:f5cc33b2-34f5-4dcb-a2d1-b96d7ef4d663"
    }
    main(event)