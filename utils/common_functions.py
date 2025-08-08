import os
import yaml
from src.logger import get_logger
from src.custom_exception import CustomException
import pandas as pd

## Create Functions

logger = get_logger(__name__)

def read_yaml(file_path:str):
    """
    Reads YAML file from a file path and returns the yaml file.

    Args
      file_path: str
    
    Returns
      YAML file
    
    from utils.common_functions import read_yaml
    read_yaml(file_path=)
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File is not in the given path")
        
        with open(file_path, "r") as yaml_file:
            config = yaml.safe_load(yaml_file)
            logger.info("Successfully loaded and read the YAML file")
            return config
    
    except Exception as e:
        logger.error("Error while reading YAML file")
        raise CustomException("Failed to read YAML File", e)


def load_data(file_path:str):
    """
    Reads data from a file path and returns the the data. 
    Note: It reads the data in a CSV format.

    Args
      file_path: str
    
    Returns
      data
    
    from utils.common_functions import load_data
    load_data(file_path=)
    """
    try:
        #pass
        logger.info("Loading Data")
        return pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Error loading the data {e}")
        raise CustomException("Failed to load data",e)