from fastapi import FastAPI,HTTPException
from azure.storage.blob import BlobServiceClient
import re
from fastapi.middleware.cors import CORSMiddleware
import traceback
from contextlib import asynccontextmanager
import pandas as pd
import asyncio
from sqlalchemy import create_engine, text
import numpy as np
from urllib import parse
import os
from dotenv import load_dotenv
import json
from ast import literal_eval
from pydantic import BaseModel
import random
import time 
from typing import List
from bs4 import BeautifulSoup

# Define a Pydantic model for patient information
class PatientInfo(BaseModel):
    patient_name: str
    patient_age: int
    patient_sex: str
    patient_history: str
    mod_study:str
    filename: str
    
class UpdatePayload(BaseModel):
    study_id: str
    action: dict
    comment: str


# Load environment variables
load_dotenv('.env')



    
# Initialize FastAPI app
app = FastAPI()


# Configure CORS middleware for the app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

azure_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client("public")

# Database connection setup
host = os.getenv("POSTGRES_HOST")
username = os.getenv("POSTGRES_USER")
password = parse.quote(os.getenv("POSTGRES_PASSWORD"))    
port = os.getenv("POSTGRES_PORT")
db = f'postgresql://{username}:{password}@{host}:5432/ai'
engine = create_engine(db)

# Load data from CSV
data_core = pd.read_csv('./data/data.csv')
table_name = ""

# Function to execute database queries with optional data retrieval
def get_collection_data_with_placeholder(query, get_data=False):
    with engine.connect() as connection:
        if get_data:
            return connection.execute(text(query))
        return pd.DataFrame(connection.execute(text(query)))
    



def get_matching_files(filename, mod_study):
    filename = filename.replace(".png", "")

    folder_path = f"demo/{mod_study}"
    
    blobs = container_client.list_blobs(name_starts_with=folder_path)

    pattern = re.compile(rf"^{filename}_keyimage_\d+\.png$")
    matching_files = []
   
    for blob in blobs:
        blob_name = blob.name.split('/')[-1]

        if pattern.match(blob_name):
            blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/public/{blob.name}"
            matching_files.append(blob_url)
            
    return matching_files
    
    

        
        
                
@app.get("/data")
async def get_data():
    data = get_collection_data_with_placeholder(
    "SELECT * FROM bionictree.tb_screening ORDER BY created_at DESC")
    data = data.apply(lambda x: x.map(lambda y: y.item() if isinstance(y, np.int64) else y), axis=1)
    return {"data": data.to_dict(orient="records")}

@app.get("/get-info/{order_id}")
async def get_info(order_id: str):
    query = f"SELECT *, created_at as date FROM bionictree.tb_screening WHERE order_id = '{order_id}'"
    data = get_collection_data_with_placeholder(query)
    if data.empty:
        return {"message": "No data found for the provided order_id"}
    else:
        data = data.apply(lambda x: x.map(lambda y: y.item() if isinstance(y, np.int64) else y), axis=1)
        return {"data": data.to_dict(orient="records")}

@app.post("/insert-chestr")
async def insert_getinfo(patient_info: PatientInfo):
    print("patient_info", patient_info)
    record = data_core[data_core['image_name'] == int(patient_info.filename.replace(".png", ""))]
    print("record", record)
    
    if not record.empty: 
        print("record value", record)
        impression = literal_eval(record['impression'].values[0])
        tabledata = record['tabledata'].values[0]
        
        try:
            if pd.isna(tabledata) or tabledata == '':
                tabledata = [] 
            else:
                tabledata = json.loads(tabledata)  # Use json.loads() instead of literal_eval()
        except json.JSONDecodeError as e:
            print(f"Error parsing tabledata: {e}")
            tabledata = []  # Set to empty list if parsing fails
        
        findings_dict = json.loads(record['dictionary'].values[0])
        matching_files = get_matching_files(patient_info.filename, patient_info.mod_study)
        study_link_json = json.dumps(matching_files)

        json_response = {
            "url": study_link_json,
            "impression": impression,
            "pathologies": [findings_dict],
            "tabledata": tabledata
        }

        default_status = "AI Autonomous"
        time.sleep(10)
        insert_query = """
        INSERT INTO bionictree.tb_screening (order_id, patient_name, referring_doctor, mod_study, primary_doctor, patient_age, patient_sex, patient_mobile, patient_history, study_link, "json", created_at, status)
        VALUES (:order_id, :patient_name, :referring_doctor, :mod_study, :primary_doctor, :patient_age, :patient_sex, :patient_mobile, :patient_history, :study_link, :json, NOW(), :status)
        """
        
        params = {
            "order_id": str(random.randint(100000, 999999)),
            "patient_name": patient_info.patient_name,
            "referring_doctor": "demo",
            "mod_study": patient_info.mod_study,
            "primary_doctor": "demo",
            "patient_age": str(patient_info.patient_age),
            "patient_sex": patient_info.patient_sex,
            "patient_mobile": "demo",
            "patient_history": patient_info.patient_history,
            "study_link": study_link_json,
            "json": json.dumps(json_response),
            "status": default_status,
        }

        try:
            with engine.begin() as connection:
                connection.execute(text(insert_query), params)
        except Exception as e:
            print(f"Error executing SQL: {e}")
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

        return {"json": json_response}
    else:
        print("else case")
        raise HTTPException(status_code=404, detail="No matching record found")
    

@app.delete("/delete-all-data")
async def delete_all_data():
    delete_query = """
    DELETE FROM bionictree.tb_screening
    """
    
    try:
        with engine.begin() as connection:
            result = connection.execute(text(delete_query))
        return {"message": f"All data deleted successfully. Rows affected: {result.rowcount}"}
    except Exception as e:
        print(f"Error executing SQL: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")