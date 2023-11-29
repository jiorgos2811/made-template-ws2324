import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()
class Trainstops(Base):
    __tablename__ = "trainstops"
    
    eva_nr = Column('EVA_NR', Integer, primary_key=True)
    ds100 = Column('DS100', String)
    ifopt = Column('IFOPT', String)
    name = Column('NAME', String)
    verkehr = Column('Verkehr', String)
    land = Column('Laenge', Float)
    breite = Column('Breite', Float)
    betreiber_name = Column('Betreiber_Name', String)
    betreiber_nr = Column('Betreiber_Nr', Integer)

def clean_data(data):
    trainstops = pd.read_csv(data, delimiter= ';')
    trainstops = trainstops[
        ['EVA_NR', 'DS100', 'IFOPT', 'NAME', 'Verkehr', 'Laenge', 'Breite',
        'Betreiber_Name', 'Betreiber_Nr']
        ] # Dropping the status collumn.

    #drop all rows with invalid values
    trainstops = trainstops.loc[trainstops['Verkehr'].isin(["FV", "RV", "nur DPN"])]
    
    trainstops['Laenge'] = trainstops['Laenge'].str.replace(',', '.').astype(float)
    trainstops['Breite'] = trainstops['Breite'].str.replace(',', '.').astype(float)
    trainstops = trainstops.loc[(trainstops['Laenge'].between(-90, 90)) & (trainstops['Breite'].between(-90, 90))]

    pattern = r'^[A-Za-z]{2}:\d+:\d+(?::\d+)?$'
    trainstops = trainstops.loc[trainstops['IFOPT'].str.contains(pattern, na = False)]
    
    trainstops = trainstops.dropna()
    
    return trainstops
    
def store_data(trainstops):
    
    # Define the SQLite database engine
    engine = create_engine('sqlite:///trainstops.sqlite', echo=True)
    Base.metadata.create_all(bind = engine)
    Session = sessionmaker(bind = engine)
    session = Session()
    
    #Write every row in the db
    for index, row in trainstops.iterrows():
            trainstop_instance = Trainstops(
                eva_nr=row['EVA_NR'],
                ds100=row['DS100'],
                ifopt=row['IFOPT'],
                name=row['NAME'],
                verkehr=row['Verkehr'],
                land=row['Laenge'],
                breite=row['Breite'],
                betreiber_name=row['Betreiber_Name'],
                betreiber_nr=row['Betreiber_Nr']
            )
            session.add(trainstop_instance)

    session.commit()

data = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df = clean_data(data)
store_data(df)
