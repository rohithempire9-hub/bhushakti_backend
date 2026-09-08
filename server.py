import os
import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.errors import PyMongoError

app = FastAPI(title="BHUSAKTHI Telemetry Engine")

# Enable CORS for frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URI = os.getenv("MONGO_URI", "")
client = None
logs_collection = None

# Attempt connection only if a valid URI is configured
if MONGO_URI and "yourcluster" not in MONGO_URI:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["bhusakthi_db"]
        logs_collection = db["telemetry_logs"]
        print("Connected successfully to MongoDB Atlas.")
    except Exception as e:
        print(f"MongoDB connection warning: {e}")
        logs_collection = None
else:
    print("No valid MONGO_URI provided. Operating in local memory mode.")

class IncidentLog(BaseModel):
    sector_name: str
    lat: float
    lng: float
    risk_score: int
    action_triggered: str

@app.get("/")
def health_check():
    return {
        "status": "online",
        "service": "BHUSAKTHI Backend",
        "database": "connected" if logs_collection is not None else "local_fallback"
    }

@app.post("/api/log-action")
async def log_user_action(payload: IncidentLog):
    doc = payload.dict()
    doc["timestamp"] = datetime.datetime.utcnow().isoformat()
    
    if logs_collection is not None:
        try:
            result = logs_collection.insert_one(doc)
            return {"status": "success", "source": "mongodb_atlas", "id": str(result.inserted_id)}
        except PyMongoError as err:
            return {"status": "fallback", "source": "local_cache", "error": str(err)}
    
    # Fallback response when MongoDB is not connected
    return {"status": "success", "source": "local_mock_store", "data": doc}