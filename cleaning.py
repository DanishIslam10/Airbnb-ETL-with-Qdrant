import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

RAW_PATH = Path("raw_data/Listings.csv")

USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DBNAME = os.getenv("DBNAME")

def extract(raw_path: Path) -> pd.DataFrame:
    # extracting raw data
    df = pd.read_csv(raw_path,encoding='latin1')
    # printing basic info about the raw data
    print(df.columns)
    print(df.shape)
    print(df.dtypes)
    print(df.isnull().sum())
    print(df.info())
    print(df.head(10))
    
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    
    # filling missing values for these columns with 0
    host_columns = ['host_response_time','host_response_rate','host_acceptance_rate']
    
    for col in host_columns:
       df[col] = df[col].fillna(0)
    
    # making a seperate column to show all ratings together in the form of text to increase the semantic meaning and search efficiency
    review_cols = ['review_scores_rating','review_scores_accuracy','review_scores_cleanliness',
               'review_scores_checkin','review_scores_communication','review_scores_location',
               'review_scores_value']
    
    df["text_reviews"] = (
        "Overall Ratings: " + df["review_scores_rating"].fillna("not rated yet").astype(str) + ", "
        + "Accuracy: " + df["review_scores_accuracy"].fillna("not rated yet").astype(str) + ", "
        + "Cleanliness: " + df["review_scores_cleanliness"].fillna("not rated yet").astype(str) + ", "
        + "Checkin: " + df["review_scores_checkin"].fillna("not rated yet").astype(str) + ", "
        + "Communication: " + df["review_scores_communication"].fillna("not rated yet").astype(str) + ", "
        + "Location: " + df["review_scores_location"].fillna("not rated yet").astype(str) + ", "
        + "Value: " + df["review_scores_value"].fillna("not rated yet").astype(str)
    )
    
    # handling other missing columns
    
    df['name'] = df['name'].fillna("no name provided")
    
    df['host_since'] = df['host_since'].fillna("unknown")
    
    df['host_location'] = df['host_location'].fillna("unknown")
    
    df['host_is_superhost'] = df['host_is_superhost'].fillna(False)
    df['host_is_superhost'] = df['host_is_superhost'].replace({"f":False,"t":True})
    
    df['host_total_listings_count'] = df['host_total_listings_count'].fillna(0)
    
    df['host_has_profile_pic'] = df['host_has_profile_pic'].fillna(False)
    df['host_has_profile_pic'] = df['host_has_profile_pic'].replace({'t':True,'f':False})
    
    df['host_identity_verified'] = df['host_identity_verified'].fillna(False)
    df['host_identity_verified'] = df['host_identity_verified'].replace({'t':True,'f':False})
    
    df['district'] = df['district'].fillna("unknown")
    
    df['bedrooms'] = df['bedrooms'].fillna(0)
    
    return df
    

def load_to_postgres(df: pd.DataFrame) -> None:
    
    DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
    engine = create_engine(DATABASE_URL)
    
    TABLE_NAME = "airbnb_listings_clean"
    
    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="replace", 
        index=False      
    )
    
    print(f"Saved {len(df)} rows to table '{TABLE_NAME}' in database '{DBNAME}'.")


if __name__ == "__main__":
    df_raw = extract(RAW_PATH)
    df_clean = transform(df_raw)
    load_to_postgres(df_clean)
   
    