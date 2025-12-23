from extract import extract_data
from transform import transform_data
from load import load_data

def run_pipeline():
    extract_data()
    transform_data()
    load_data()
    print("ETL pipeline executed successfully")

if __name__ == "__main__":
    run_pipeline()
