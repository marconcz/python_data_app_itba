from datetime import datetime, timedelta
from requests import get
import pandas as pd
import time

URL = 'https://www.cheapshark.com/api/1.0/deals'

# Hace una llamada a la API de cheapshark y se trae
# todas las ofertas de videojuegos de Steam (StoreID = 1)
# ordenadas por las actualizadas recientemente.
def get_cheapshark_deals(page):
    url = URL
    params = {
        'storeID': '1',
        'sortBy': 'Recent',
        'pageSize': '60',
        'pageNumber': page
    }
    try:
        response = get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error al obtener los datos: {e}")
        return None
    
def api_to_parquet(start_date):
    # Son los valores que me parecieron interesantes para extraer
    # y modelar. 
    to_extract = {
                'ID': 'dealID',                     # ID unico de una oferta
                'GameID': 'steamAppID',             # ID de un juego, comparte ID con la API de Steam, util para enriquecer
                'StoreID':'storeID',                # ID de la tienda donde esta la oferta (Nosotros siempre traemos Steam = 1, se podria hardcodear)
                'Name': 'title',                    # Nombre del juego
                'Metacritic': 'metacriticScore',    # Puntaje del juego
                'Price': 'normalPrice',             # Precio regular del juego (U$D)
                'Sale': 'salePrice',                # Precio en esta oferta (U$D)
                'OnSale': 'isOnSale',               # Esta en oferta ? 1 = SI , 0 = NO
                'GameRelease': 'releaseDate',       # Fecha en la que salio al mercado el juego
                'UpdateTime': 'lastChange',         # Fecha del ultimo cambio del juego
            }

    # Lista para ir acumulando las ofertas a medida que recorro la API
    deal_list = []

    # El dia que recibo como parametro
    current_date = end_date = datetime.strptime(start_date, '%Y-%m-%d')

    # Le resto un dia, voy a hacer una carga de los datos que hay entre 
    # el parametro recibido y un dia menos, o una cantidad N de dias
    yesterday_date = current_date - timedelta(days=1)

    # La fecha en la API viene en formato UNIX TIME
    # por lo tanto va a ser mejor pasar mis fechas a ese formato
    # para hacer menos conversiones que transformando fecha a fecha
    # recibida por la API.
    end_date = int(end_date .timestamp())
    timestamp = int(yesterday_date.timestamp())

    # Arranco leyendo la primer pagina
    page = 0

    # timestamp -> La fecha recibida - 1 dia (N dias en caso de recibir parametro)
    # end_date -> La fecha recibida como parametro

    # end_date en inicio es mas reciente que timestamp
    # a medida que recorro los datos leidos de la API actualizo el end_date
    # hasta que en cierto momento los datos leidos de la API son menos novedosos
    # que mi fecha limite 'timestamp', ahi corto, sino al quedarme sin paginas.
    while (timestamp <= end_date):
        data = get_cheapshark_deals(page)
        if data is not None:
            for deal in data:
                # Para cada json que representa una oferta, aplico la funcion get()
                # para obtener los campos que estan en el array 'to_extact'
                deal_list.append({key: deal.get(value) for key, value in to_extract.items()})
            end_date = deal_list[-1].get('UpdateTime') # Al final de cada lectura, actualizo end_date por el dato menos novedoso, osea el ultimo en la pagina leida
            page += 1 # Paso a la siguiente pagina
            print('Recorriendo pagina: ' + str(page))
            time.sleep(1)
        else:  
            end_date = 0 # Si fallo, la fecha = 0 -> Como estan en formato UNIX TIME es la fecha minima posible y rompe el WHILE

    # Crear un DataFrame de pandas con las novedades
    new_data = pd.DataFrame(deal_list)

    # Ahora si transformo las fechas en formato YYYY-MM-DD
    new_data['GameRelease'] = pd.to_datetime(new_data['GameRelease'], unit='s').dt.strftime('%Y-%m-%d')
    new_data['UpdateTime'] = pd.to_datetime(new_data['UpdateTime'], unit='s').dt.strftime('%Y-%m-%d')

    # Convertir las columnas a los tipos correctos
    new_data['GameID'] = pd.to_numeric(new_data['GameID'], errors='coerce').fillna(0).astype(int)
    new_data['StoreID'] = pd.to_numeric(new_data['StoreID'], errors='coerce').fillna(0).astype(int)
    new_data['Metacritic'] = pd.to_numeric(new_data['Metacritic'], errors='coerce').fillna(0).astype(float)
    new_data['Price'] = pd.to_numeric(new_data['Price'], errors='coerce').fillna(0).astype(float)
    new_data['Sale'] = pd.to_numeric(new_data['Sale'], errors='coerce').fillna(0).astype(float)
    new_data['OnSale'] = pd.to_numeric(new_data['OnSale'], errors='coerce').fillna(0).astype(int)

    # Leo los datos que ya tengo cargados de antes
    #latest = pd.read_parquet('games_data.parquet')

    # Junto ambos conjuntos de datos, concateno los dataframes y luego borro duplicados
    # conservando los nuevos por ID que actua como la PK de mi tabla
    #updated_df = pd.concat([latest , new_data]).drop_duplicates(subset = ['ID'], keep = 'last')

    # Ordeno por fechas (no es necesario pero lo hacia para chequear luego en excel si actualizo bien)
    new_data.sort_values(by = 'UpdateTime', ascending = False)

    # Guardar el DataFrame en un archivo CSV
    new_data.to_csv('games_data.csv', index=False)
    new_data.to_parquet('games_data.parquet', index=False)
    
