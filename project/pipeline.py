import requests
import gzip
import csv
import re
import os
from sqlalchemy import create_engine
from io import BytesIO
import pandas as pd

class DataPipeline:
    def __init__(self, link_traffic, link_airquality):
        self.link_traffic =  link_traffic
        self.link_airquality = link_airquality
        self.stations = [
            "mc010", "mc032", "mc042", "mc077", "mc085", "mc174"
        ] #The station codes of the messing stations we want to examine
        self.year = '2023'
        
    def extract_data(self, stations, year):
        output_folder = r".\data"
        for i in range(1,13):
            
            month = str(i).zfill(2)
            
            #Extracting traffic data
            response_traffic = requests.get(f'{self.link_traffic}/mq_hr_{year}_{month}.csv.gz')
            if response_traffic:
 
                #If the response is succesful use BytesIO to create a file-like object from the response content
                compressed_file = BytesIO(response_traffic.content)
                
                # The files traffic data is compressed in .gz format
                with gzip.open(compressed_file, 'rt') as f:

                    # Save the decompressed content to a file
                    with open(f'{output_folder}/traffic_data.csv', 'a') as traffic_data:
                        traffic_data.write(f.read())

                print(f'Traffic file downloaded and decompressed successfully for year {year} month {month}.')
            else:
                print(f'Failed to download the traffic file for year {year} month {month}. Data most likely unavailable at this time')
            
            #Extracting airquality data
            checked = 0
            for station in stations:
                next_month = str(i+1).zfill(2)
                
                #Edge case for changing the year. Opted instead of collecting data until 01.01.202X+1 to collect until 31.12.202X
                if next_month == '13':
                    next_month = 12
                    response_stations = requests.get(f'''{self.link_airquality}/{station}.csv?group=pollution&
                                                period=1h&timespan=custom&start%5Bdate%5D=01.{month}.{year}&
                                                    start%5Bhour%5D=01&end%5Bdate%5D=31.{next_month}.{year}&
                                                        end%5Bhour%5D=23''')
                else:
                    response_stations = requests.get(f'''{self.link_airquality}/{station}.csv?group=pollution&
                                                period=1h&timespan=custom&start%5Bdate%5D=01.{month}.{year}&
                                                    start%5Bhour%5D=02&end%5Bdate%5D=01.{next_month}.{year}&
                                                        end%5Bhour%5D=00''')

                if response_stations:
                    with open(f'{output_folder}/{station}.csv', 'a', encoding='utf-8') as station_data:
                        # spliting response.content into a list of lines in order not to include the first 6 lines of each file that are irrelevant
                        content_lines = response_stations.content.decode('utf-8').split('\n')
                        station_data.write('\n'.join(content_lines[6:])) #Saving the rejoined response
                        station_data.write('\n') #Add a new line between the months 
                        if len(content_lines) - 5 == 0 and checked == 0:
                            print(f'There is no data regarding the air quality for the month {month}.{year}')
                            checked = 1
                else:
                    print(f'Failed to download the file for station {station} month {month}. Status code: {response_stations.status_code}') 
                    
                    
    def transform_data(self):
        
        directory = './data'
        all_data = []

        #df for all the traffic data
        file_path = os.path.join(directory, 'traffic_data.csv')
        
        #Headers got saved multiple times. Pandas raises a dtype error.
        #We're going to delete all the duplicates (which are only headers) before we load the file into pandas.      
        lines_seen = set()  
        unique_lines = [] 
        with open(file_path, 'r') as file:
            for line in file:
                if line not in lines_seen:
                    unique_lines.append(line)
                    lines_seen.add(line) 

        with open(file_path, 'w') as file:
            file.writelines(unique_lines) # Write unique lines back to the file
            
        traffic_df = pd.read_csv(os.path.join(file_path),  delimiter= ';' , header= 0)
        
        #df for all the air quality data
        pattern = r'^mc\d{3}\.csv$' #Regex pattern to find only files starting with mc followed by 3 digits and .csv which is the naming convention we use
        for filename in os.listdir(directory):
            f = os.path.join(directory, filename)
            if re.match(pattern, os.path.basename(f)):
                
                #Formatting our data to look the way we want
                headers = ['datetime', 'PMO10', 'PMO2.5', 'NO2', 'NO', 'NOx', 'O3']
                airq_df = pd.read_csv(f, delimiter=';', header=None)
                airq_df = airq_df.iloc[:, 0:7] # Drop extra columns. I want to have the same metrics for all stations
                airq_df.columns = headers
                airq_df['station'] = filename[:-4]
                all_data.append(airq_df)
                
        merged_df = pd.concat(all_data, ignore_index=True)
        
        return traffic_df, merged_df
    
    def load_data(self, traffic_df, airq_df):
        engine = create_engine('sqlite:///./data/UrbanAirQualityInMotion.sqlite')

        # Write the DataFrame to the SQLite database
        traffic_df.to_sql('traffic', engine, index = False, if_exists = 'replace')
        airq_df.to_sql('airQuality', engine, index = False, if_exists = 'replace')
        
        if os.path.exists('./data/UrbanAirQualityInMotion.sqlite'):
            print('UrbanAirQualityInMotion.sqlite was created')
        else:
            print('An error occured when loadind the data')
            
    def run_data_pipeline(self):
        
        self.extract_data(self.stations, self.year)
        traffic_df, airq_df = self.transform_data()
        self.load_data(traffic_df, airq_df)

        print("Data pipeline completed successfully.")   

if __name__ == "__main__":
    link_traffic = "https://mdhopendata.blob.core.windows.net/verkehrsdetektion/2023/Messquerschnitte%20(fahrtrichtungsbezogen)"
    link_airquality = "https://luftdaten.berlin.de/station"
    
    pipeline = DataPipeline(link_traffic, link_airquality)
    pipeline.run_data_pipeline()

