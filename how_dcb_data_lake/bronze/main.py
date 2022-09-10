from how_dcb_data_lake.functions import requisicoes, logs, s3_bronze

def lambda_handler(event, context):
    logger = logs()
    try: 
        logger.info('Iniciando coleta dos dados de planetas')
        url_planets = 'https://swapi.dev/api/planets/'
        df_planets = requisicoes(url_planets)
        s3_bronze(df_planets,'planets')

        logger.info('Iniciando coleta dos dados de pessoas')
        url_peoples = 'https://swapi.dev/api/people/'
        df_peoples = requisicoes(url_peoples)
        s3_bronze(df_peoples,'peoples')

        logger.info('Iniciando coleta dos dados de filmes')
        url_films = 'https://swapi.dev/api/films/'
        df_films = requisicoes(url_films)
        s3_bronze(df_films,'films')

        logger.info('Iniciando coleta dos dados de espécies')
        url_species = 'https://swapi.dev/api/species/'
        df_species = requisicoes(url_species)
        s3_bronze(df_species,'species')

        logger.info('Iniciando coleta dos dados de naves')
        url_starships = 'https://swapi.dev/api/starships/'
        df_starships = requisicoes(url_starships)
        s3_bronze(df_starships,'starships')

        logger.info('Iniciando coleta dos dados de veículos')
        url_vehicles = 'https://swapi.dev/api/vehicles/'
        df_vehicles = requisicoes(url_vehicles)
        s3_bronze(df_vehicles,'vehicles')

        logger.info('Fim do processo de coleta dos dados')
    except Exception as e:
        logger.error(f'Erro durante o processo de coleta dos dados: {e}')
        

def main():
    lambda_handler(None, None)


if __name__ == '__main__':
    main()