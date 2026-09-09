import os
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.errors import PyMongoError

app = FastAPI(title="BHUSAKTHI Early Warning & Response API", version="2.0.0")
origin = os.getenv("FRONTEND_ORIGIN", "*")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin] if origin != "*" else ["*"],
    allow_credentials=origin != "*",
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URI = os.getenv("MONGO_URI", "")
client = None
logs = None
incidents_db = None
memory_incidents: list[dict[str, Any]] = [
    {"id":"INC-DEMO-001","sector_name":"Bhalukpong - Bomdila Corridor","district":"West Kameng","state":"Arunachal Pradesh","lat":27.2645,"lng":92.4159,"severity":"CRITICAL","status":"ACTIVE","type":"Road blockage / slope failure","notes":"Demo incident: debris affecting highway connectivity.","media":[],"timestamp":"2026-09-09T12:00:00+00:00"},
    {"id":"INC-DEMO-002","sector_name":"Sairang - Aizawl Syncline","district":"Aizawl","state":"Mizoram","lat":23.7271,"lng":92.7176,"severity":"HIGH","status":"MONITORING","type":"Slope movement","notes":"Demo incident: field observation requires verification.","media":[],"timestamp":"2026-09-09T11:30:00+00:00"},
]

REGIONS = [
 {"id":"GSI-AR-01","name":"Bhalukpong - Bomdila Corridor","district":"West Kameng","state":"Arunachal Pradesh","lat":27.2645,"lng":92.4159,"susceptibility":89,"slope":46,"elevation":2150,"lithology":"Precambrian Metamorphic Gneiss","road":"NH-13","road_status":"CLOSED"},
 {"id":"GSI-ML-01","name":"Cherrapunji - Mawsynram Escarpment","district":"East Khasi Hills","state":"Meghalaya","lat":25.2986,"lng":91.5822,"susceptibility":81,"slope":38,"elevation":1430,"lithology":"Tertiary Sandstone & Limestone","road":"SH-5","road_status":"RESTRICTED"},
 {"id":"GSI-NL-01","name":"Pfutsero - Kohima Ridge","district":"Kohima","state":"Nagaland","lat":25.6751,"lng":94.1086,"susceptibility":76,"slope":32,"elevation":1444,"lithology":"Disang Shale & Turbidites","road":"NH-29","road_status":"RESTRICTED"},
 {"id":"GSI-MZ-01","name":"Sairang - Aizawl Syncline","district":"Aizawl","state":"Mizoram","lat":23.7271,"lng":92.7176,"susceptibility":84,"slope":35,"elevation":1132,"lithology":"Bhuban Formation Siltstone","road":"NH-54","road_status":"CLOSED"},
]

class IncidentCreate(BaseModel):
    sector_name: str = Field(min_length=2, max_length=120)
    district: str = Field(default="Field Report", max_length=80)
    state: str = Field(default="North Eastern Region", max_length=80)
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    severity: str = Field(default="HIGH", max_length=20)
    type: str = Field(default="Field observation", max_length=120)
    notes: str = Field(default="", max_length=2000)
    media: list[str] = Field(default_factory=list, max_length=10)

class ActionLog(BaseModel):
    sector_name: str
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    risk_score: int = Field(ge=0, le=100)
    action_triggered: str = Field(min_length=1, max_length=120)
    extra_data: dict[str, Any] = Field(default_factory=dict)


def init_db():
    global client, logs, incidents_db
    if not MONGO_URI or "yourcluster" in MONGO_URI:
        print("MONGO_URI not configured; using demo memory mode.")
        return
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["bhusakthi_db"]
        logs, incidents_db = db["telemetry_logs"], db["incidents"]
        logs.create_index("timestamp")
        incidents_db.create_index([("timestamp", -1)])
        print("MongoDB Atlas connected.")
    except Exception as exc:
        client = None
        logs = incidents_db = None
        print(f"MongoDB warning: {exc}; demo memory mode enabled.")

init_db()


def score(region, rain=135, moisture=84):
    value = (region["susceptibility"] * .45 + min(rain / 150 * 100, 100) * .25 + moisture * .15 + min(region["slope"] / 50 * 100, 100) * .15)
    return max(0, min(100, round(value)))


def level(value):
    return "CRITICAL" if value >= 75 else "HIGH" if value >= 55 else "MODERATE" if value >= 35 else "LOW"


@app.get("/")
def health():
    return {"status":"online","service":"BHUSAKTHI Early Warning & Response API","version":"2.0.0","database":"mongodb_atlas" if logs is not None else "memory_demo","demo_mode":logs is None}

@app.get("/api/regions")
def get_regions():
    return [{**r, "risk_score": score(r), "risk_level": level(score(r))} for r in REGIONS]

@app.get("/api/dashboard/summary")
def summary():
    scored = get_regions()
    active = [i for i in memory_incidents if i["status"] in {"ACTIVE", "MONITORING"}]
    return {
        "risk_zones":len(scored), "critical_zones":sum(r["risk_score"] >= 75 for r in scored), "high_zones":sum(55 <= r["risk_score"] < 75 for r in scored),
        "active_incidents":len(active), "sensors_online":12, "roads_impacted":sum(r["road_status"] != "OPEN" for r in scored), "rainfall_mm":135, "soil_moisture_pct":84,
        "forecast":[{"time":"00:00","rain":24,"risk":32},{"time":"04:00","rain":52,"risk":48},{"time":"08:00","rain":94,"risk":69},{"time":"12:00","rain":142,"risk":86},{"time":"16:00","rain":105,"risk":78},{"time":"20:00","rain":68,"risk":59}],
        "priority":[{"label":"Bhalukpong - Bomdila","score":92,"action":"Evacuate / close corridor"},{"label":"Sairang - Aizawl","score":83,"action":"Deploy field team"},{"label":"Cherrapunji - Mawsynram","score":76,"action":"Traffic restriction"}],
        "generated_at":datetime.now(timezone.utc).isoformat()
    }

@app.get("/api/incidents")
def get_incidents():
    if incidents_db is not None:
        try: return list(incidents_db.find({}, {"_id":0}).sort("timestamp", -1).limit(50))
        except PyMongoError: pass
    return sorted(memory_incidents, key=lambda x:x["timestamp"], reverse=True)

@app.post("/api/incidents")
def create_incident(payload: IncidentCreate):
    incident = payload.model_dump()
    incident.update({"id":f"INC-{int(datetime.now(timezone.utc).timestamp()*1000)}","status":"ACTIVE","timestamp":datetime.now(timezone.utc).isoformat()})
    if incidents_db is not None:
        try: incidents_db.insert_one(incident.copy())
        except PyMongoError: memory_incidents.append(incident)
    else: memory_incidents.append(incident)
    return {"status":"success","source":"mongodb_atlas" if incidents_db is not None else "memory_demo","incident":incident}

@app.patch("/api/incidents/{incident_id}")
def update_incident(incident_id: str, status: str):
    if status not in {"ACTIVE","MONITORING","RESOLVED"}: raise HTTPException(400, "Invalid status")
    if incidents_db is not None:
        try:
            result = incidents_db.update_one({"id":incident_id},{"$set":{"status":status}})
            if result.matched_count: return {"status":"success","id":incident_id,"new_status":status}
        except PyMongoError: pass
    for item in memory_incidents:
        if item["id"] == incident_id:
            item["status"] = status
            return {"status":"success","id":incident_id,"new_status":status}
    raise HTTPException(404, "Incident not found")

@app.post("/api/log-action")
def log_action(payload: ActionLog):
    doc = payload.model_dump(); doc["timestamp"] = datetime.now(timezone.utc).isoformat()
    if logs is not None:
        try:
            result = logs.insert_one(doc); return {"status":"success","source":"mongodb_atlas","id":str(result.inserted_id)}
        except PyMongoError as exc: print(f"Atlas write failed: {exc}")
    return {"status":"success","source":"memory_demo","data":doc}

@app.get("/api/analytics")
def analytics():
    return {"risk_model":"Weighted risk fusion baseline","features":["rainfall","soil moisture","terrain slope","historical susceptibility"],"seven_day":[68,72,74,81,86,78,71],"note":"Replace the transparent baseline with validated ML inference when trained model weights are available."}
