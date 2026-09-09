import os
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pymongo import MongoClient
from pymongo.errors import PyMongoError

app = FastAPI(title="BHUSAKTHI Early Warning & Response API", version="3.0.0")
origin = os.getenv("FRONTEND_ORIGIN", "*")
app.add_middleware(CORSMiddleware, allow_origins=[origin] if origin != "*" else ["*"], allow_credentials=origin != "*", allow_methods=["*"], allow_headers=["*"])

MONGO_URI = os.getenv("MONGO_URI", "")
client = None
logs = None
incidents_db = None
recipients_db = None
alerts_db = None

memory_incidents: list[dict[str, Any]] = [
 {"id":"INC-DEMO-001","sector_name":"Bhalukpong - Bomdila Corridor","district":"West Kameng","state":"Arunachal Pradesh","lat":27.2645,"lng":92.4159,"severity":"CRITICAL","status":"ACTIVE","type":"Road blockage / slope failure","notes":"DEMO incident: debris affecting highway connectivity. Verify with field team.","media":[],"timestamp":"2026-09-09T12:00:00+00:00"},
 {"id":"INC-DEMO-002","sector_name":"Sairang - Aizawl Syncline","district":"Aizawl","state":"Mizoram","lat":23.7271,"lng":92.7176,"severity":"HIGH","status":"MONITORING","type":"Slope movement","notes":"DEMO incident: field observation requires verification.","media":[],"timestamp":"2026-09-09T11:30:00+00:00"},
]
memory_recipients: list[dict[str, Any]] = []
memory_alerts: list[dict[str, Any]] = []

# Compact 11-zone pilot feature dataset. Weather/soil values are illustrative demo inputs,
# not claimed live observations. Replace with IMD/sensor/Sentinel-derived features in production.
REGIONS = [
 {"id":"GSI-AR-01","name":"Bhalukpong - Bomdila Corridor","district":"West Kameng","state":"Arunachal Pradesh","lat":27.2645,"lng":92.4159,"susceptibility":89,"slope":46,"elevation":2150,"lithology":"Precambrian Metamorphic Gneiss","road":"NH-13","road_status":"CLOSED","rainfall_mm":168,"soil_moisture_pct":88,"temperature_c":22,"humidity_pct":91,"wind_kmh":14,"population_at_risk":4200,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":38,"risk":58},{"time":"04:00","rain":72,"risk":68},{"time":"08:00","rain":128,"risk":81},{"time":"12:00","rain":168,"risk":92},{"time":"16:00","rain":140,"risk":88},{"time":"20:00","rain":96,"risk":76}],"villages":[{"name":"Tenzingaon","distance_km":2.1,"population":640},{"name":"Nyukmadung","distance_km":3.4,"population":520},{"name":"Rama Camp Colony","distance_km":5.8,"population":310}]},
 {"id":"GSI-AR-02","name":"Tawang - Bomdila Corridor","district":"Tawang","state":"Arunachal Pradesh","lat":27.5867,"lng":91.8590,"susceptibility":86,"slope":43,"elevation":2650,"lithology":"Metamorphic schist & gneiss","road":"NH-13","road_status":"RESTRICTED","rainfall_mm":142,"soil_moisture_pct":82,"temperature_c":14,"humidity_pct":86,"wind_kmh":18,"population_at_risk":2900,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":30,"risk":55},{"time":"04:00","rain":61,"risk":65},{"time":"08:00","rain":105,"risk":75},{"time":"12:00","rain":142,"risk":82},{"time":"16:00","rain":126,"risk":78},{"time":"20:00","rain":80,"risk":69}],"villages":[{"name":"Dirang","distance_km":3.0,"population":780},{"name":"Jang","distance_km":5.1,"population":460}]},
 {"id":"GSI-AR-03","name":"Itanagar - Naharlagun Corridor","district":"Papum Pare","state":"Arunachal Pradesh","lat":27.1020,"lng":93.6950,"susceptibility":72,"slope":29,"elevation":440,"lithology":"Siwalik sediments","road":"NH-15","road_status":"OPEN","rainfall_mm":116,"soil_moisture_pct":76,"temperature_c":27,"humidity_pct":84,"wind_kmh":8,"population_at_risk":6100,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":22,"risk":42},{"time":"04:00","rain":48,"risk":49},{"time":"08:00","rain":81,"risk":58},{"time":"12:00","rain":116,"risk":66},{"time":"16:00","rain":94,"risk":61},{"time":"20:00","rain":60,"risk":53}],"villages":[{"name":"Naharlagun","distance_km":2.0,"population":1400},{"name":"Doimukh","distance_km":5.4,"population":820}]},
 {"id":"GSI-AS-01","name":"Haflong - Silchar Corridor","district":"Dima Hasao","state":"Assam","lat":25.1640,"lng":93.0176,"susceptibility":79,"slope":34,"elevation":680,"lithology":"Sandstone & shale","road":"NH-27","road_status":"RESTRICTED","rainfall_mm":154,"soil_moisture_pct":87,"temperature_c":24,"humidity_pct":90,"wind_kmh":10,"population_at_risk":3500,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":36,"risk":54},{"time":"04:00","rain":67,"risk":62},{"time":"08:00","rain":112,"risk":73},{"time":"12:00","rain":154,"risk":80},{"time":"16:00","rain":130,"risk":76},{"time":"20:00","rain":88,"risk":67}],"villages":[{"name":"Mahur","distance_km":2.5,"population":510},{"name":"Jatinga","distance_km":4.7,"population":380}]},
 {"id":"GSI-ML-01","name":"Cherrapunji - Mawsynram Escarpment","district":"East Khasi Hills","state":"Meghalaya","lat":25.2986,"lng":91.5822,"susceptibility":81,"slope":38,"elevation":1430,"lithology":"Tertiary Sandstone & Limestone","road":"SH-5","road_status":"RESTRICTED","rainfall_mm":205,"soil_moisture_pct":92,"temperature_c":20,"humidity_pct":95,"wind_kmh":11,"population_at_risk":3100,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":64,"risk":62},{"time":"04:00","rain":110,"risk":74},{"time":"08:00","rain":168,"risk":83},{"time":"12:00","rain":205,"risk":87},{"time":"16:00","rain":182,"risk":85},{"time":"20:00","rain":121,"risk":77}],"villages":[{"name":"Mawkdok","distance_km":1.8,"population":410},{"name":"Nongriat approach hamlet","distance_km":4.2,"population":180},{"name":"Laitkynsew","distance_km":6.5,"population":390}]},
 {"id":"GSI-ML-02","name":"Shillong - Jowai Corridor","district":"East Khasi Hills / West Jaintia Hills","state":"Meghalaya","lat":25.4670,"lng":92.2030,"susceptibility":74,"slope":33,"elevation":1250,"lithology":"Quartzite & sandstone","road":"NH-6","road_status":"OPEN","rainfall_mm":132,"soil_moisture_pct":80,"temperature_c":19,"humidity_pct":88,"wind_kmh":9,"population_at_risk":4700,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":28,"risk":45},{"time":"04:00","rain":51,"risk":52},{"time":"08:00","rain":88,"risk":62},{"time":"12:00","rain":132,"risk":70},{"time":"16:00","rain":109,"risk":66},{"time":"20:00","rain":72,"risk":57}],"villages":[{"name":"Mawphlang","distance_km":3.1,"population":620},{"name":"Nartiang","distance_km":6.0,"population":560}]},
 {"id":"GSI-MZ-01","name":"Sairang - Aizawl Syncline","district":"Aizawl","state":"Mizoram","lat":23.7271,"lng":92.7176,"susceptibility":84,"slope":35,"elevation":1132,"lithology":"Bhuban Formation Siltstone","road":"NH-54","road_status":"CLOSED","rainfall_mm":149,"soil_moisture_pct":86,"temperature_c":23,"humidity_pct":89,"wind_kmh":13,"population_at_risk":3800,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":30,"risk":54},{"time":"04:00","rain":58,"risk":63},{"time":"08:00","rain":102,"risk":76},{"time":"12:00","rain":149,"risk":86},{"time":"16:00","rain":118,"risk":80},{"time":"20:00","rain":74,"risk":67}],"villages":[{"name":"Sairang Venglai","distance_km":1.2,"population":710},{"name":"Sialsuk","distance_km":3.9,"population":340},{"name":"Tuirial","distance_km":6.0,"population":410}]},
 {"id":"GSI-MZ-02","name":"Lunglei Corridor","district":"Lunglei","state":"Mizoram","lat":22.8877,"lng":92.7380,"susceptibility":78,"slope":37,"elevation":1220,"lithology":"Bhuban Formation shale & siltstone","road":"NH-302","road_status":"RESTRICTED","rainfall_mm":137,"soil_moisture_pct":83,"temperature_c":22,"humidity_pct":90,"wind_kmh":12,"population_at_risk":2500,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":27,"risk":49},{"time":"04:00","rain":55,"risk":58},{"time":"08:00","rain":91,"risk":68},{"time":"12:00","rain":137,"risk":77},{"time":"16:00","rain":110,"risk":72},{"time":"20:00","rain":70,"risk":61}],"villages":[{"name":"Hnahthial","distance_km":3.4,"population":480},{"name":"Lungsen","distance_km":5.2,"population":360}]},
 {"id":"GSI-NL-01","name":"Pfutsero - Kohima Ridge","district":"Kohima","state":"Nagaland","lat":25.6751,"lng":94.1086,"susceptibility":76,"slope":32,"elevation":1444,"lithology":"Disang Shale & Turbidites","road":"NH-29","road_status":"RESTRICTED","rainfall_mm":98,"soil_moisture_pct":70,"temperature_c":19,"humidity_pct":82,"wind_kmh":9,"population_at_risk":2600,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":18,"risk":41},{"time":"04:00","rain":34,"risk":48},{"time":"08:00","rain":62,"risk":58},{"time":"12:00","rain":98,"risk":71},{"time":"16:00","rain":80,"risk":66},{"time":"20:00","rain":46,"risk":52}],"villages":[{"name":"Chizami","distance_km":3.0,"population":450},{"name":"Losami","distance_km":4.6,"population":300},{"name":"Kikruma","distance_km":7.1,"population":520}]},
 {"id":"GSI-MN-01","name":"Ukhrul - Imphal Corridor","district":"Ukhrul / Imphal East","state":"Manipur","lat":24.7500,"lng":94.0100,"susceptibility":77,"slope":36,"elevation":1120,"lithology":"Disang Formation shale","road":"NH-202","road_status":"RESTRICTED","rainfall_mm":128,"soil_moisture_pct":81,"temperature_c":21,"humidity_pct":86,"wind_kmh":10,"population_at_risk":3300,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":24,"risk":48},{"time":"04:00","rain":52,"risk":57},{"time":"08:00","rain":86,"risk":67},{"time":"12:00","rain":128,"risk":76},{"time":"16:00","rain":105,"risk":72},{"time":"20:00","rain":65,"risk":61}],"villages":[{"name":"Hundung","distance_km":2.7,"population":430},{"name":"Sikhong","distance_km":5.5,"population":350}]},
 {"id":"GSI-SK-01","name":"Gangtok - Rangpo Corridor","district":"Gangtok / Pakyong","state":"Sikkim","lat":27.3389,"lng":88.6065,"susceptibility":88,"slope":44,"elevation":1650,"lithology":"Gneiss, schist & quartzite","road":"NH-10","road_status":"CLOSED","rainfall_mm":176,"soil_moisture_pct":89,"temperature_c":18,"humidity_pct":92,"wind_kmh":15,"population_at_risk":5200,"data_status":"DEMO_INPUTS","forecast":[{"time":"00:00","rain":41,"risk":61},{"time":"04:00","rain":78,"risk":71},{"time":"08:00","rain":132,"risk":83},{"time":"12:00","rain":176,"risk":91},{"time":"16:00","rain":150,"risk":87},{"time":"20:00","rain":101,"risk":78}],"villages":[{"name":"Rangpo","distance_km":1.9,"population":950},{"name":"Singtam","distance_km":4.0,"population":720}]},
]

class IncidentCreate(BaseModel):
    sector_name: str = Field(min_length=2, max_length=120)
    district: str = Field(default="Field Report", max_length=80)
    state: str = Field(default="North Eastern Region", max_length=80)
    lat: float = Field(ge=-90, le=90)
    lng: float = Field(ge=-180, le=180)
    severity: str = Field(default="HIGH", max_length=20)
    status: str = Field(default="ACTIVE", max_length=20)
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

class RecipientCreate(BaseModel):
    name: str = Field(min_length=2, max_length=100)
    mobile: str = Field(min_length=10, max_length=20)
    region_id: str = Field(min_length=3, max_length=30)
    village: str = Field(default="", max_length=120)
    language: str = Field(default="EN", max_length=10)
    min_alert_level: str = Field(default="HIGH", max_length=20)
    active: bool = True

class AlertCreate(BaseModel):
    region_id: str
    severity: str = Field(default="HIGH", max_length=20)
    message: str = Field(min_length=10, max_length=500)
    channels: list[str] = Field(default_factory=lambda: ["DASHBOARD", "SMS"])


def init_db():
    global client, logs, incidents_db, recipients_db, alerts_db
    if not MONGO_URI or "yourcluster" in MONGO_URI:
        print("MONGO_URI not configured; using demo memory mode.")
        return
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["bhusakthi_db"]
        logs, incidents_db = db["telemetry_logs"], db["incidents"]
        recipients_db, alerts_db = db["recipients"], db["alerts"]
        logs.create_index("timestamp")
        incidents_db.create_index([("timestamp", -1)])
        recipients_db.create_index("mobile")
        alerts_db.create_index([("timestamp", -1)])
        print("MongoDB Atlas connected.")
    except Exception as exc:
        client = None
        logs = incidents_db = recipients_db = alerts_db = None
        print(f"MongoDB warning: {exc}; demo memory mode enabled.")

init_db()


def score(region, rain=None, moisture=None):
    rain = region.get("rainfall_mm", 135) if rain is None else rain
    moisture = region.get("soil_moisture_pct", 84) if moisture is None else moisture
    value = region["susceptibility"] * .45 + min(rain / 150 * 100, 100) * .25 + moisture * .15 + min(region["slope"] / 50 * 100, 100) * .15
    return max(0, min(100, round(value)))


def level(value):
    return "CRITICAL" if value >= 75 else "HIGH" if value >= 55 else "MODERATE" if value >= 35 else "LOW"


def village_risk(parent_score, distance_km):
    return max(20, min(100, round(parent_score - distance_km * 4)))


def scored_region(region):
    s = score(region)
    villages = [{**v, "risk_score": village_risk(s, v["distance_km"]), "risk_level": level(village_risk(s, v["distance_km"]))} for v in region["villages"]]
    return {**region, "risk_score": s, "risk_level": level(s), "risk_source":"DEMO_WEIGHTED_BASELINE" if region.get("data_status") == "DEMO_INPUTS" else "LIVE_FUSED_DATA", "villages": villages}


def write_collection(collection, memory, doc):
    if collection is not None:
        try:
            collection.insert_one(doc.copy())
            return "mongodb_atlas"
        except PyMongoError:
            pass
    memory.append(doc)
    return "memory_demo"

@app.get("/")
def health():
    return {"status":"online","service":"BHUSAKTHI Early Warning & Response API","version":"3.0.0","database":"mongodb_atlas" if logs is not None else "memory_demo","demo_mode":logs is None,"risk_data_mode":"DEMO_INPUTS"}

@app.get("/api/regions")
def get_regions():
    return [scored_region(r) for r in REGIONS]

@app.get("/api/regions/{region_id}")
def get_region(region_id: str):
    for region in REGIONS:
        if region["id"] == region_id:
            return scored_region(region)
    raise HTTPException(404, "Region not found")

@app.get("/api/villages/at-risk")
def villages_at_risk(min_risk: int = 50):
    rows = []
    for region in REGIONS:
        r = scored_region(region)
        for village in r["villages"]:
            if village["risk_score"] >= min_risk:
                rows.append({"region_id":r["id"],"region":r["name"],**village})
    return sorted(rows, key=lambda x: x["risk_score"], reverse=True)

@app.get("/api/dashboard/summary")
def summary():
    scored = get_regions()
    active = get_incidents()
    active = [i for i in active if i.get("status") in {"ACTIVE", "MONITORING"}]
    priority = sorted(scored, key=lambda r:r["risk_score"], reverse=True)
    return {"risk_zones":len(scored),"critical_zones":sum(r["risk_score"] >= 75 for r in scored),"high_zones":sum(55 <= r["risk_score"] < 75 for r in scored),"active_incidents":len(active),"sensors_online":12,"roads_impacted":sum(r["road_status"] != "OPEN" for r in scored),"population_at_risk":sum(r["population_at_risk"] for r in scored),"data_mode":"DEMO INPUTS · API READY","priority":[{"label":r["name"],"score":r["risk_score"],"action":"Immediate response" if r["risk_score"]>=85 else "Enhanced monitoring"} for r in priority],"generated_at":datetime.now(timezone.utc).isoformat()}

@app.get("/api/incidents")
def get_incidents():
    if incidents_db is not None:
        try: return list(incidents_db.find({}, {"_id":0}).sort("timestamp", -1).limit(50))
        except PyMongoError: pass
    return sorted(memory_incidents, key=lambda x:x["timestamp"], reverse=True)

@app.post("/api/incidents")
def create_incident(payload: IncidentCreate):
    incident = payload.model_dump()
    incident.update({"id":f"INC-{int(datetime.now(timezone.utc).timestamp()*1000)}","timestamp":datetime.now(timezone.utc).isoformat()})
    source = write_collection(incidents_db, memory_incidents, incident)
    return {"status":"success","source":source,"incident":incident}

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
    source = write_collection(logs, [], doc) if logs is not None else "memory_demo"
    return {"status":"success","source":source,"data":doc}

@app.get("/api/analytics")
def analytics():
    return {"risk_model":"Weighted risk fusion baseline","features":["rainfall","soil moisture","terrain slope","historical susceptibility"],"seven_day":[68,72,74,81,86,78,71],"data_mode":"DEMO_INPUTS","note":"Replace the transparent baseline with validated ML inference after training on historical landslide inventory + rainfall/terrain/soil features."}

@app.get("/api/recipients")
def get_recipients():
    if recipients_db is not None:
        try: return list(recipients_db.find({}, {"_id":0}).sort("name", 1))
        except PyMongoError: pass
    return memory_recipients

@app.post("/api/recipients")
def add_recipient(payload: RecipientCreate):
    recipient = payload.model_dump(); recipient.update({"id":f"USR-{int(datetime.now(timezone.utc).timestamp()*1000)}","created_at":datetime.now(timezone.utc).isoformat()})
    source = write_collection(recipients_db, memory_recipients, recipient)
    return {"status":"registered","source":source,"recipient":recipient}

@app.get("/api/alerts")
def get_alerts():
    if alerts_db is not None:
        try: return list(alerts_db.find({}, {"_id":0}).sort("timestamp", -1).limit(50))
        except PyMongoError: pass
    return sorted(memory_alerts, key=lambda x:x["timestamp"], reverse=True)

@app.post("/api/alerts")
def create_alert(payload: AlertCreate):
    region = next((r for r in REGIONS if r["id"] == payload.region_id), None)
    if not region: raise HTTPException(404, "Region not found")
    alert = payload.model_dump()
    alert.update({"id":f"ALT-{int(datetime.now(timezone.utc).timestamp()*1000)}","timestamp":datetime.now(timezone.utc).isoformat(),"status":"QUEUED","delivery_mode":"SIMULATED_SMS","recipient_count":0,"delivered_count":0})
    recipients = get_recipients()
    alert["recipient_count"] = sum(1 for r in recipients if r.get("active") and r.get("region_id") == payload.region_id)
    alert["delivered_count"] = 0
    source = write_collection(alerts_db, memory_alerts, alert)
    return {"status":"queued","source":source,"alert":alert,"provider":"SIMULATED_SMS"}

@app.post("/api/alerts/{alert_id}/dispatch")
def dispatch_alert(alert_id: str):
    alerts = get_alerts()
    alert = next((a for a in alerts if a.get("id") == alert_id), None)
    if not alert: raise HTTPException(404, "Alert not found")
    count = alert.get("recipient_count", 0)
    delivered = count
    update = {"status":"SIMULATED_DELIVERED","delivered_count":delivered,"dispatched_at":datetime.now(timezone.utc).isoformat()}
    if alerts_db is not None:
        try: alerts_db.update_one({"id":alert_id},{"$set":update})
        except PyMongoError: pass
    for item in memory_alerts:
        if item.get("id") == alert_id: item.update(update)
    return {"status":"success","provider":"SIMULATED_SMS","alert_id":alert_id,"recipient_count":count,"delivered_count":delivered,"note":"Connect an approved SMS provider for real telecom delivery."}
