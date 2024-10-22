from sqlalchemy import create_engine, String, Integer, Column, Float, ForeignKey, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from urllib.parse import quote_plus
import pandas as pd

# Parámetros de conexión
database = 'pda'
host = ''
port = 5439
user = ''
password = ''

# Crear el URL de conexión
connection_string = f"redshift+psycopg2://{user}:{quote_plus(password)}@{host}:{port}/{database}"

# Engine y echo = true para poder ver la salida 
# del motor de base de datos por la consola, util para debug
engine = create_engine(connection_string, echo=True)

# Voy a crear 2 clases para utilizar el ORM de sqlalchemy
Base = declarative_base()

class Games(Base):
    __tablename__ = 'games'
    __table_args__ = {'schema': '2024_patricio_nahuel_perrone_schema'}
    
    id = Column('GameID', Integer, primary_key=True)
    name = Column('Name', String(400), nullable=True)
    metacritic_score = Column('Metacritic', Float, nullable=True)
    game_realease_date = Column('GameRelease', String(10), nullable=True)

    # Relación con Deal
    deals = relationship("Deal", back_populates="game")

class Deal(Base):
    __tablename__ = 'cheapshark_deals'
    __table_args__ = {'schema': '2024_patricio_nahuel_perrone_schema'}
    
    id = Column("ID", String(200), primary_key=True)
    steam_id = Column('GameID', Integer, ForeignKey('2024_patricio_nahuel_perrone_schema.games.GameID'), nullable=True)
    store_id = Column('StoreID', Integer, nullable=True)         
    sale_price = Column('Sale', Float, nullable=True)        
    is_on_sale = Column('OnSale', Integer, nullable=True)        
    deal_update_date = Column('UpdateTime', String(10), nullable=True)   

    # Relación inversa
    game = relationship("Games", back_populates="deals")

# Esto crea las tablas (solamente si no existen previamente)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

# Aca le doy forma a los objetos que voy a insertar
def prepare_game(row):
    return Games(
        id=int(row['GameID']),
        name=row['Name'],
        metacritic_score=float(row['Metacritic']) if row['Metacritic'] else None,
        game_realease_date=row['GameRelease']
    )

def prepare_deal(row, game_id):
    return Deal(
        id=row['ID'],
        steam_id=game_id,
        store_id=int(row['StoreID']),  # Asegúrate de convertir a entero
        sale_price=float(row['Sale']) if row['Sale'] else None,
        is_on_sale=int(row['OnSale']) if row['OnSale'] else None,
        deal_update_date=row['UpdateTime']
    )

try:
    with Session() as conn:
        # Seteo el esquema proporcionado por la catedra
        conn.execute(text(f"SET search_path TO '2024_patricio_nahuel_perrone_schema';"))
        
        # Leo el stage
        df = pd.read_parquet('games_data.parquet')
        
        # Filtramos los registros con 'UpdateTime' mayor a la fecha de corte
        df = df[df['UpdateTime'] > cutoff_date]
        
        # Obtener todos los juegos y deals existentes en la base de datos
        existing_games = {game.id: game for game in conn.query(Games).all()}
        existing_deals = {deal.id: deal for deal in conn.query(Deal).all()}

        game_listings = []
        deal_listings = []

        for index, row in df.iterrows():
            game_id = int(row['GameID'])
            game = prepare_game(row)

            # Si el juego ya existe en la BBDD, lo actualizo
            if game_id in existing_games:
                existing_game = existing_games[game_id]
                existing_game.name = game.name
                existing_game.metacritic_score = game.metacritic_score
                existing_game.game_realease_date = game.game_realease_date
            else:
                # Si el juego no existe, lo agrego al lote
                game_listings.append(game)

            # Preparar el objeto Deal
            deal = prepare_deal(row, game_id)

            # Si el Deal ya existe, lo actualizo
            if deal.id in existing_deals:
                existing_deal = existing_deals[deal.id]
                existing_deal.steam_id = deal.steam_id
                existing_deal.store_id = deal.store_id
                existing_deal.sale_price = deal.sale_price
                existing_deal.is_on_sale = deal.is_on_sale
                existing_deal.deal_update_date = deal.deal_update_date
            else:
                # Si el Deal no existe, lo agrego al lote
                deal_listings.append(deal)

            # Hacer commits en lotes de 100 para evitar cargar demasiado la memoria
            if len(deal_listings) >= 100 or len(game_listings) >= 100:
                conn.bulk_save_objects(game_listings)
                conn.bulk_save_objects(deal_listings)
                deal_listings = [], game_listings = []

        # Inserción de los juegos en bulk
        if game_listings:
            conn.bulk_save_objects(game_listings)
            
        # Guardar el último lote si quedó sin insertar
        if deal_listings:
            conn.bulk_save_objects(deal_listings)


        # Commit final para confirmar los cambios
        conn.commit()     

except Exception as e:
    print(f"Error al conectar a Redshift: {e}")
