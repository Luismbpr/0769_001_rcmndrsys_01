import os
import sys
import pandas as pd
import numpy as np
import joblib
from sklearn.model_selection import train_test_split
#from src.logger import get_logger
#from src.custom_exception import CustomException
from .logger import get_logger
from .custom_exception import CustomException
from config.paths_config import *


logger = get_logger(__name__)## Initialize logger

## input file: all the csv
## output directory to store all the processed files
class DataProcessor:
    def __init__(self, input_file, output_dir):
        self.input_file = input_file
        self.output_dir = output_dir
        
        ## Currently there is nothing. Will store it later
        self.rating_df = None
        self.anime_df = None
        self.X_train_array = None
        self.X_test_array = None
        self.y_train = None
        self.y_test = None

        self.user2user_encoded = {}
        self.user2user_decoded = {}
        self.anime2anime_encoded = {}
        self.anime2anime_decoded = {}
        
        ## Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info("Data Processing Initialized")
    

    def load_data(self, usecols):
        """Load data

        usecols = ["user_id", "anime_id", "rating"]
        """
        try:
            #pass
            self.rating_df = pd.read_csv(self.input_file, low_memory=True, usecols=usecols)
            logger.info("Data loaded successfully for Data Processing")
        except Exception as e:
            raise CustomException("Failed to load data", sys)
    

    def filter_users(self, min_rating=400):
        """
        Filters, selects and returns users who have n-amount of ratings.

        Args
          min_rating: int - Minimum amount of ratings that user needs to have. If user < min_rating it will not be retrieved. = 400 (Default)

        Returns
          .
        
        filter_users(min_rating=400)
        """
        try:
            #pass
            n_ratings = self.rating_df["user_id"].value_counts()
            self.rating_df = self.rating_df[self.rating_df["user_id"].isin(n_ratings[n_ratings>=min_rating].index)].copy()
            logger.info("Filtered users successfully")
        except Exception as e:
            raise CustomException("Failed to filter data", sys)
    
    def scale_rating(self):
        """
        - Performs Scaling by using Min Max Scaling on the 'rating' column.
        - Removes duplicated values

        Args
          min_rating: int - Minimum amount of ratings that user needs to have. If user < min_rating it will not be retrieved. = 400 (Default)

        Returns
          .
        
        scale_rating(min_rating=400)
        """
        try:
            #pass
            min_rating = min(self.rating_df["rating"])
            max_rating = max(self.rating_df["rating"])
            self.rating_df["rating"] = self.rating_df["rating"].apply(lambda x: (x - min_rating)/(max_rating - min_rating)).values.astype(np.float64)
            logger.info("Scaling Successful - Data Preprocessing")
        except Exception as e:
            raise CustomException("Failed to load data", sys)
    
    def remove_duplicates(self):
        """This dataset does not contain duplicates but this code will remove if there are some"""
        try:
            #pass
            #self.rating_df = self.rating_df.drop_duplicates()
            if self.rating_df.duplicated().sum() is None or self.rating_df.duplicated().sum() == 0:
                logger.info("Dataset does not contain duplicated value on following columns: ['user_id', 'anime_id']")
                pass
            else:
                self.rating_df = self.rating_df.drop_duplicates(subset=['user_id', 'anime_id'])
                logger.info(f"Removed duplicated values on 'user_id' and 'anime_id'")
            logger.info("Remove Duplicates Function Completed Successfully")
        except Exception as e:
            raise CustomException("Failed to remove duplicate values", sys)
    

    def null_values(self):
        """
        Removes null values
        - Removes duplicated values

        Args
          min_rating: int - Minimum amount of ratings that user needs to have. If user < min_rating it will not be retrieved. = 400 (Default)

        Returns
          .
        
        scale_rating(min_rating=400)
        """
        try:
            #pass
            self.rating_df.dropna(subset=['user_id', 'anime_id'], inplace=True)
            fill_values = {'user_id':'MISSING_VALUE', 'anime_id':'MISSING_VALUE'}
            self.rating_df.fillna()
            logger.info("Successfully Removed null values")
        except Exception as e:
            raise CustomException("Failed to remove duplicate values", sys)
    
    
    def encode_data(self):
        try:
            #pass
            ## User encoding
            user_ids = self.rating_df["user_id"].unique().tolist()
            self.user2user_encoded = {x:i for i,x in enumerate(user_ids)}
            self.user2user_decoded = {i:x for i,x in enumerate(user_ids)}
            self.rating_df["user"] = self.rating_df["user_id"].map(self.user2user_encoded)
            ## Anime encoding
            anime_ids = self.rating_df["anime_id"].unique().tolist()
            self.anime2anime_encoded = {x:i for i,x in enumerate(anime_ids)}
            self.anime2anime_decoded = {i:x for i,x in enumerate(anime_ids)}
            ## Error
            #self.rating_df["anime"] = self.rating_df.map(self.anime2anime_encoded)
            self.rating_df["anime"] = self.rating_df["anime_id"].map(self.anime2anime_encoded)

            logger.info("Successful Encoding for Users and Films")
        except Exception as e:
            raise CustomException("Failed to Encode data", sys)
    

    def split_data(self, test_size=1000, random_state=43):
        try:
            self.rating_df = self.rating_df.sample(frac=1, random_state=random_state)
            X = self.rating_df[["user", "anime"]].values
            y = self.rating_df["rating"]
            
            ## Train indices
            train_indices = self.rating_df.shape[0] - test_size

            X_train, X_test, y_train, y_test = (
                X[:train_indices],
                X[train_indices:],
                y[:train_indices],
                y[train_indices:],
            )

            ## Array for each column ["user", "anime"] on Train Set
            ## Store on already specified instances
            self.X_train_array = [X_train[:, 0], X_train[:, 1]]
            self.X_test_array = [X_test[:, 0], X_test[:, 1]]
            self.y_train_array = y_train
            self.y_test_array = y_test
            logger.info("Data Split Successful")
        except Exception as e:
            raise CustomException("Failed to Split data", sys)
    
    
    def save_artifacts(self):
        try:
            #pass
            ## artifacts name:array
            ## all of those are arrays
            artifacts = {
                "user2user_encoded": self.user2user_encoded,
                "user2user_decoded": self.user2user_decoded,
                "anime2anime_encoded": self.anime2anime_encoded,
                "anime2anime_decoded": self.anime2anime_decoded,
            }

            for name, data in artifacts.items():
                joblib.dump(data, os.path.join(self.output_dir, f"{name}.pkl"))
                logger.info(f"{name} saved successfully")
            
            logger.info(f"Encoder-Decoders saved successfully in following filepath: {self.output_dir}")
            
            ## Save the arrays
            joblib.dump(self.X_train_array, X_TRAIN_ARRAY)
            joblib.dump(self.X_test_array, X_TEST_ARRAY)
            joblib.dump(self.y_train, Y_TRAIN)
            joblib.dump(self.y_test, Y_TEST)
            
            logger.info(f"X Arrays and Train Sets Saved successfully in following filepath: {self.output_dir}")

            self.rating_df.to_csv(path_or_buf=RATING_DF, index=False)

            logger.info(f"Dataset rating_df Saved successfully in following filepath: {self.output_dir}")
        except Exception as e:
            raise CustomException("Failed To Save Artifacts Data", sys)
    

    def process_anime_data(self):
        try:
            #pass
            ## Open datasets:
            ## ANIME_CSV = "artifacts/raw/anime.csv"
            ## ANIME_SYNOPSIS_CSV = "artifacts/raw/anime_with_synopsis.csv"

            df = pd.read_csv(ANIME_CSV)
            cols = ["MAL_ID","Name","Genres","sypnopsis"]
            synopsis_df = pd.read_csv(ANIME_SYNOPSIS_CSV, usecols=cols)
            
            ## Replacing 'unknown' with Null values
            df = df.replace("unknown", np.nan)
            
            ## Function: getAnimeName 
            def getAnimeName(anime_id):
                try:
                    #pass
                    name = df[df["anime_id"] == anime_id]["eng_version"].values[0]
                    if name is np.nan:
                        name = df[df["anime_id"] == anime_id]["Name"].values[0]
                except:
                    print("Error in getAnimeName function")
                return name
            df["anime_id"] = df["MAL_ID"]
            df["eng_version"] = df["English name"]
            df["eng_version"] = df["anime_id"].apply(lambda x: getAnimeName(x))

            ## Sort values
            df.sort_values(by=["Score"],
                           inplace=True,
                           ascending=False,
                           kind="quicksort",
                           na_position="last"
            )
            
            df = df[["anime_id","eng_version","Score","Genres","Episodes","Type","Premiered","Members"]]
            
            ## Saving DataFrames into required/desired directory
            df.to_csv(DF, index=False, header=True)
            synopsis_df.to_csv(SYNOPSIS_DF, index=False, header=True)
            
            logger.info(f"Datasets DF and SYNOPSIS_DF saved successfully in following directories")
            logger.info(f"{DF}")
            logger.info(f"{SYNOPSIS_DF}")
        except Exception as e:
            raise CustomException("Failed to load data", sys)
    
    
    def run(self):
        try:
            #pass
            logger.info("Data Processing Pipeline - Started")
            cols_to_load=["user_id","anime_id","rating"]
            self.load_data(usecols=cols_to_load)
            self.filter_users(min_rating=400)
            self.remove_duplicates()
            #self.null_values()
            self.encode_data()
            self.split_data(test_size=1000, random_state=43)
            self.save_artifacts()
            self.process_anime_data()
            logger.info("Data Processing Pipeline - Run successfully")
        except Exception as e:
            raise CustomException("Failed to load data", sys)
    

    # def function_00(self):
    #     try:
    #         pass
    #     except Exception as e:
    #         raise CustomException("Failed to load data", sys)
if __name__ == "__main__":
    data_processor = DataProcessor(ANIMELIST_CSV, PROCESSED_DIR)
    data_processor.run()


## Note: When running this standalone script
### Use this:
##from .logger import get_logger
##from .custom_exception import CustomException
##% python -m src.data_ingestion
## This worked
