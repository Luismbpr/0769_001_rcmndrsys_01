import os
import pandas as pd
from google.cloud import storage
from config.paths_config import *
from utils.common_functions import read_yaml

from .logger import get_logger
from .custom_exception import CustomException
#from src.logger import get_logger
#from src.custom_exception import CustomException

logger = get_logger(__name__)

# #from config.yaml
# # data_ingestion:
# #   bucket_name: "mlops-project-2"
# #   bucket_file_names:
# #     - "anime.csv"
# #     - "anime_with_synopsis.csv"
# #     - "animelist.csv"

class DataIngestion:
    def __init__(self,config):
        self.config = config["data_ingestion"]
        #self.bucket_name = self.config["bucket_name"]
        self.file_names = self.config["bucket_file_names"]

        if os.path.exists(RAW_DIR):
            logger.info(f"Path already exists: {RAW_DIR}")
            os.makedirs(RAW_DIR, exist_ok=True)
        else:
            logger.info(f"Creating path: {RAW_DIR}")
            os.makedirs(RAW_DIR, exist_ok=True)
        #os.makedirs(RAW_DIR, exist_ok=True)
        logger.info("Data Ingestion Process Starting")
    
    def move_from_data_to_raw(self):
        """
        Moves Specific data files to RAW folder
        If Data File is too large it will only select a subset and save it in the RAW folder.
        """
        try:
            pass
        except Exception as e:
            logger.error("Error while transferring data from folder into RAW path")
            raise CustomException("Failed to Transfer data", e)

    def download_csv_from_gcp(self):
        """
        Downloads CSV file from Google Cloud In this case from GC Bucket

        """
        try:
            #pass
            ## Initialize Client and list the bucket
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            
            for file_name in self.file_names:
                ##RAW_DIR = "artifacts/raw/file_name"
                file_path = os.path.join(RAW_DIR, file_name)
                ## "animelist.csv" is too large and will import a percentage of the whole dataset
                if file_name == "animelist.csv":
                    ## File is stored inside this blob variable
                    blob = bucket.blob(file_name)
                    blob.download_to_filename(file_path)

                    data = pd.read_csv(file_path, nrows=5000000)
                    data.to_csv(file_path, index=False)
                    logger.info(f"Large file detected. Downloading {5000,000} rows")
                    logger.info(f"File: {file_name} downloaded successfully")
                else:
                    blob = bucket.blob(file_name)
                    blob.download_to_filename(file_path)
                    logger.info(f"File: {file_name} downloaded successfully")
        except Exception as e:
            logger.error("Error while downloading data from GCP")
            raise CustomException("Failed to download data", e)
    
    def run(self):
        try:
            logger.info("Starting Data Ingestion Process")
            self.download_csv_from_gcp()
            logger.info("Data Ingestion completed successfully")
        except CustomException as ce:
            logger.error(f"Custom Exception: {str(ce)}")
        finally:
            logger.info("Data Ingestion Process DONE")


if __name__=="__main__":
    logger.info("Data Ingestion Process Started")
    data_ingestion = DataIngestion(read_yaml(CONFIG_PATH))
    data_ingestion.run()

## Note: When running this standalone script
### Use this:
##from .logger import get_logger
##from .custom_exception import CustomException
##% python -m src.data_ingestion
## This worked