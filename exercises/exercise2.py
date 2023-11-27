import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, MetaData, Table

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

    # Define metadata
    metadata = MetaData()

    # Define the table structure
    trainstops_table = Table('trainstops', metadata,
        Column('EVA_NR', Integer, primary_key=True),
        Column('DS100', String),
        Column('IFOPT', String),
        Column('NAME', String),
        Column('Verkehr', String),
        Column('Laenge', Float),
        Column('Breite', Float),
        Column('Betreiber_Name', String),
        Column('Betreiber_Nr', Integer)
    )

    # Create the table in the database
    metadata.create_all(engine)

    # Insert data into the table
    trainstops.to_sql('trainstops', engine, if_exists='replace', index=False)


data = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df = clean_data(data)
store_data(df)
