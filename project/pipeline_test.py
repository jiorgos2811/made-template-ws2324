from pipeline import DataPipeline
from sqlalchemy import create_engine, inspect
import pytest
import glob
import os

current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
db_path = os.path.join(parent_dir, "data", "UrbanAirQualityInMotion.sqlite")

@pytest.mark.dependency()
def test_data_retrieval(): 
    data_pipeline = DataPipeline(link_traffic, link_airquality)
    data_pipeline.extract_data(data_pipeline.stations, data_pipeline.year)
    
    # Check if the expected files are created in the data folder
    assert os.path.exists('./data/traffic_data.csv')
    for station in data_pipeline.stations:
        assert os.path.exists(f'./data/{station}.csv')
    
# Only execute test for the complete datapipeline if the previous tests passed
@pytest.mark.dependency(depends=["test_data_retrieval"])
def test_data_formating(): 
    data_pipeline = DataPipeline(link_traffic, link_airquality)
    csv_files = glob.glob(os.path.join(parent_dir, '*.csv'))

    # Loop through each CSV file from the previous test and delete it 
    for csv_file in csv_files:
        try:
            os.remove(csv_file)
        except:
            print(f"Error deleting {csv_file}")
            
    if os.path.exists(db_path):
        os.remove(db_path)
        
    data_pipeline.run_data_pipeline()

    assert os.path.exists(db_path)

    engine = create_engine(f"sqlite:///{db_path}")
    inspector = inspect(engine)
    tables = inspector.get_table_names()

    assert len(tables) == 2
    assert "traffic" in tables
    assert "airQuality" in tables

# Define the links here
link_traffic = "https://mdhopendata.blob.core.windows.net/verkehrsdetektion/2023/Messquerschnitte%20(fahrtrichtungsbezogen)"
link_airquality = "https://luftdaten.berlin.de/station"
