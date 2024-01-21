from urllib import request
from zipfile import ZipFile
import os
import pandas as pd
from sqlalchemy import create_engine

class Exercise4:
    
    def __init__(self, url):
        self.url = url
        
    #Download and unzip the data
    def donwload_and_unzip(self):
        request.urlretrieve(self.url, filename="./datafile.zip")

        with ZipFile(r".\datafile.zip", 'r') as zip_ref:
            zip_ref.extractall("./exercises")
        zip_ref.close()
        os.remove("./datafile.zip")
        os.remove("./exercises/README.pdf")

    #Reshape Data
    def reshape(self):
        with open('./exercises/data.csv', 'r') as infile, open('./exercises/data_dot.csv', 'w') as outfile:
            for line in infile:
                
                columns = line.strip().split(';')
                modified_line = ';'.join(columns[0:11]) + '\n' #Rejoin columns 0 through 11 since it has the data that intrests us
                modified_line = modified_line.replace(',', '.')
                outfile.write(modified_line)
                
        fields = ['Geraet','Hersteller','Model', 'Monat','Temperatur in °C (DWD)', 'Batterietemperatur in °C','Geraet aktiv']
        df = pd.read_csv('./exercises/data_dot.csv', delimiter=';')
        df = df[fields]
        df.rename(columns = {'Temperatur in °C (DWD)':'Temperatur', 'Batterietemperatur in °C': 'Batterietemperatur'}, inplace = True)
        
        return df

    #Transform Data
    def transform(self, df):
        df['Temperatur'] = (df['Temperatur'] * 9/5) + 32
        df['Batterietemperatur'] = (df['Batterietemperatur'] * 9/5) + 32
        return df
    
    #Validate Data
    def validate(self, df):
        # Check if Geraet is an integer and greater than 0
        df['Geraet'] = df['Geraet'].astype(int)
        if not (df['Geraet'].min() > 0):
            raise ValueError('Geraet must be an integer greater than 0')
        
        # Check if Geraet aktiv is either 'Ja' or 'Nein'
        if not (df['Geraet aktiv'].isin(['Ja', 'Nein'])).all():
            raise ValueError('Geraet aktiv must be either "Ja" or "Nein"')
        
    def load(self, df):
        engine = create_engine('sqlite:///temperatures.sqlite')
        df.to_sql('temperatures', engine, index = False, if_exists = 'replace')

    
    def run(self):
        self.donwload_and_unzip()
        df = self.reshape()
        df = self.transform(df)
        self.validate(df)
        self.load(df)
        os.remove("./exercises/data.csv")
        os.remove("./exercises/data_dot.csv")


url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
ex = Exercise4(url)
ex.run()