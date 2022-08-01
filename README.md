# Projeto de Engenharia de Dados da How Bootcamp

## Objetivo
O objetivo do projeto é aplicar na prática os conhecimentos adiquiridos ao longo do Bootcamp de Engenharia de Dados. Dessa forma, o projeto visa consumir dados de uma API, tratá-los e inseri-los no Data Lake, utilizando as soluções AWS.

## Métodos
Para esse projeto, haverão 3 lambdas:
- A primeira lambda será acionada por agendamento no Event Bridge e coletará os dados da API https://swapi.dev/api/, armazenará em JSON no s3 em sua forma bruta na camada Bronze do Data Lake.
- A segunda lambda será acionada por meio de mensageria (SNS/SQS) e fará o tratamento e particionamento dos dados, salvando-os como parquet na camada Silver do Data Lake.
- A terceira lambda será acionada por meio de mensageria (SNS/SQS) e fará a deduplicação dos dados, salvando-os na camada Gold do Data Lake para consumo.

Os dados poderão ser consultados via Athena e as visões e agregações poderão ser realizadas no Redshift utilizando o Spectrum.
 
## Serviços
Nesse projeto, as soluções AWS abaixo serão utilizadas:
- AWS S3
- Aws Athena
- AWS Lambda
- AWS SNS e SQS
- AWS Redshift

## Aquitetura 
![image](https://user-images.githubusercontent.com/17461174/182260327-25aee1f0-6563-4e95-a0c5-e6a6ea920c6d.png)
