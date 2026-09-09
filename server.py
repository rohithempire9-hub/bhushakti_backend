import os, json, time, hashlib, hmac, base64, secrets, urllib.request, urllib.parse
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pymongo import MongoClient

app=FastAPI(title='BHUSAKTHI Early Warning & Response API',version='4.3.0')
origin=os.getenv('FRONTEND_ORIGIN','*')
app.add_middleware(CORSMiddleware,allow_origins=[origin] if origin!='*' else ['*'],allow_credentials=origin!='*',allow_methods=['*'],allow_headers=['*'])
MONGO_URI=os.getenv('MONGO_URI',''); client=None; db=None
if MONGO_URI:
    try:
        client=MongoClient(MONGO_URI,serverSelectionTimeoutMS=3000); client.admin.command('ping'); db=client[os.getenv('MONGO_DB','bhusakthi')]
    except Exception: client=None; db=None
RAW=[('GSI-AR-01','Bhalukpong - Bomdila Corridor','West Kameng','Arunachal Pradesh',27.2645,92.4159,89,46,2150,'NH-13',4200),('GSI-AR-02','Tawang - Bomdila Corridor','Tawang','Arunachal Pradesh',27.5867,91.859,86,43,2650,'NH-13',2900),('GSI-AR-03','Itanagar - Naharlagun Corridor','Papum Pare','Arunachal Pradesh',27.102,93.695,72,29,440,'NH-15',6100),('GSI-AS-01','Haflong - Silchar Corridor','Dima Hasao','Assam',25.164,93.0176,79,34,680,'NH-27',3500),('GSI-ML-01','Cherrapunji - Mawsynram Escarpment','East Khasi Hills','Meghalaya',25.2986,91.5822,81,38,1430,'SH-5',3100),('GSI-ML-02','Shillong - Jowai Corridor','East Khasi Hills / West Jaintia Hills','Meghalaya',25.467,92.203,74,33,1250,'NH-6',4700),('GSI-MZ-01','Sairang - Aizawl Syncline','Aizawl', 'Mizoram',23.7271,92.7176,84,35,1132,'NH-54',3800),('GSI-MZ-02','Lunglei Corridor','Lunglei','Mizoram',22.8877,92.738,78,37,1220,'NH-302',2500),('GSI-NL-01','Pfutsero - Kohima Ridge','Kohima','Nagaland',25.6751,94.1086,76,32,1444,'NH-29',2600),('GSI-MN-01','Ukhrul - Imphal Corridor','Ukhrul / Imphal East','Manipur',24.75,94.01,77,36,1120,'NH-202',3300),('GSI-SK-01','Gangtok - Rangpo Corridor','Gangtok / Pakyong','Sikkim',27.3389,88.6065,88,44,1650,'NH-10',5200)]
REGIONS=[{'id':a,'name':b,'district':c,'state':d,'lat':e,'lng':f,'susceptibility':g,'slope':h,'elevation':i,'road':j,'road_status':'OPEN','population_at_risk':k,'rainfall_mm':None,'soil_moisture_pct':None,'temperature_c':None,'humidity_pct':None,'wind_kmh':None,'data_status':'WAITING_FOR_LIVE_WEATHER','weather_source':'Open-Meteo','risk_source':'WEATHER_ADJUSTED_BASELINE'} for a,b,c,d,e,f,g,h,i,j,k in RAW]
memory_users=[]; memory_incidents=[]; memory_alerts=[]; memory_recipients=[]
def collection(name): return db[name] if db is not None else None
def pwd_hash(password,salt=None):
    salt=salt or secrets.token_hex(16); return f'{salt}${hashlib.pbkdf2_hmac("sha256",password.encode(),salt.encode(),120000).hex()}'
def pwd_ok(password,stored):
    try:
        salt,digest=stored.split('$',1); return hmac.compare_digest(pwd_hash(password,salt).split('$',1)[1],digest)
    except Exception:return False
def token(user):
    secret=os.getenv('AUTH_SECRET','bhusakthi-demo-change-this-secret'); raw=json.dumps({'id':str(user['_id']),'exp':int(time.time())+86400},separators=(',',':')).encode(); sig=hmac.new(secret.encode(),raw,'sha256').hexdigest(); return base64.urlsafe_b64encode(raw).decode()+'.'+sig
def auth_user(authorization):
    if not authorization or not authorization.startswith('Bearer '): raise HTTPException(401,'Login required')
    try:
        raw,sig=authorization[7:].split('.',1); decoded=base64.urlsafe_b64decode(raw+'=='); data=json.loads(decoded); secret=os.getenv('AUTH_SECRET','bhusakthi-demo-change-this-secret'); expected=hmac.new(secret.encode(),decoded,'sha256').hexdigest()
        if not hmac.compare_digest(sig,expected) or data['exp']<time.time(): raise ValueError()
        users=collection('users'); user=users.find_one({'_id':data['id']}) if users is not None else next((u for u in memory_users if str(u['_id'])==data['id']),None)
        if user is None and users is not None:
            try:user=users.find_one({'_id':__import__('bson').ObjectId(data['id'])})
            except Exception:user=None
        if not user: raise ValueError()
        return user
    except Exception: raise HTTPException(401,'Invalid or expired session')
def weather(lat,lng):
    q=urllib.parse.urlencode({'latitude':lat,'longitude':lng,'current':'temperature_2m,relative_humidity_2m,rain,precipitation,wind_speed_10m','hourly':'rain,precipitation,soil_moisture_0_to_1cm','past_hours':24,'forecast_hours':1,'timezone':'auto'})
    with urllib.request.urlopen('https://api.open-meteo.com/v1/forecast?'+q,timeout=8) as r:return json.loads(r.read().decode())
def apply_weather(r):
    try:
        w=weather(r['lat'],r['lng']); c=w.get('current',{}); h=w.get('hourly',{}); rains=h.get('rain',[]); soils=h.get('soil_moisture_0_to_1cm',[])
        rain24=round(sum(float(v or 0) for v in rains[:24]),1)
        soil_value=soils[23] if len(soils)>23 else (soils[-1] if soils else None)
        if soil_value is None: raise RuntimeError('Open-Meteo soil moisture unavailable')
        r.update({'rainfall_mm':rain24,'rain_1h_mm':round(float(c.get('rain') or 0),1),'soil_moisture_pct':round(float(soil_value)*100,1),'temperature_c':c.get('temperature_2m'),'humidity_pct':c.get('relative_humidity_2m'),'wind_kmh':c.get('wind_speed_10m'),'weather_source':'Open-Meteo LIVE','data_status':'LIVE_WEATHER','weather_updated_at':datetime.now(timezone.utc).isoformat()}); return r
    except Exception as e:
        r.update({'data_status':'WEATHER_ERROR','weather_source':'Open-Meteo ERROR','weather_error':str(e),'temperature_c':None,'rainfall_mm':None,'rain_1h_mm':None,'soil_moisture_pct':None}); return r
def risk(r,rain_override=None,soil_override=None):
    rain=float(rain_override if rain_override is not None else r.get('rainfall_mm') or 0); soil=float(soil_override if soil_override is not None else r.get('soil_moisture_pct') or 0); structural=min(28,r['susceptibility']*.18+(r['slope']/50*100)*.10); s=round(min(100,structural+min(45,rain/180*45)+min(27,soil/100*27))); return s,('CRITICAL' if s>=75 else 'HIGH' if s>=55 else 'WATCH' if s>=35 else 'LOW')
def flood(r):
    rain=float(r.get('rainfall_mm') or 0); soil=float(r.get('soil_moisture_pct') or 0); s=round(min(100,rain/220*65+soil/100*35)); return s,'HIGH' if s>=75 else 'MODERATE' if s>=45 else 'LOW'
def region_view(r,live=True):
    r=dict(r)
    if live:r=apply_weather(r)
    s,l=risk(r); fs,fl=flood(r); r.update({'risk_score':s,'risk_level':l,'flood_score':fs,'flood_level':fl}); return r
class Register(BaseModel):
    name:str=Field(min_length=2,max_length=100); mobile:str=Field(min_length=10,max_length=20); password:str=Field(min_length=6,max_length=100); region_id:str; village:str=''; role:str='citizen'
class Login(BaseModel): mobile:str; password:str
class Recipient(BaseModel): name:str; mobile:str; region_id:str; village:str=''; language:str='English'; alert_level:str='HIGH'
class RiskUpdate(BaseModel): region_id:str; rainfall_mm:float=Field(ge=0); soil_moisture_pct:float=Field(ge=0,le=100)
class Incident(BaseModel):
    sector_name:str=Field(min_length=2,max_length=160); district:str='Field Observation'; state:str='North Eastern Region'; lat:float; lng:float; severity:str='HIGH'; type:str='Field slope observation'; notes:str=''; media:list[str]=[]
@app.get('/health')
def health():return {'status':'ok','version':'4.3.0','weather':'Open-Meteo LIVE','database':'mongodb' if db is not None else 'memory','demo_mode':db is None}
@app.post('/api/auth/register')
def register(x:Register):
    if not any(r['id']==x.region_id for r in REGIONS):raise HTTPException(400,'Unknown region')
    users=collection('users'); existing=users.find_one({'mobile':x.mobile}) if users is not None else next((u for u in memory_users if u['mobile']==x.mobile),None)
    if existing:raise HTTPException(409,'Mobile already registered')
    u={'_id':secrets.token_hex(12),'name':x.name,'mobile':x.mobile,'password_hash':pwd_hash(x.password),'region_id':x.region_id,'village':x.village,'role':x.role,'created_at':datetime.now(timezone.utc).isoformat()}
    if users is not None:users.insert_one(u)
    else:memory_users.append(u)
    return {'token':token(u),'user':{k:u[k] for k in ['name','mobile','region_id','village','role']}}
@app.post('/api/auth/login')
def login(x:Login):
    users=collection('users'); u=users.find_one({'mobile':x.mobile}) if users is not None else next((z for z in memory_users if z['mobile']==x.mobile),None)
    if not u or not pwd_ok(x.password,u['password_hash']):raise HTTPException(401,'Invalid mobile or password')
    return {'token':token(u),'user':{k:u[k] for k in ['name','mobile','region_id','village','role']}}
@app.get('/api/auth/me')
def me(authorization:str=Header(None)):
    u=auth_user(authorization); return {k:u[k] for k in ['name','mobile','region_id','village','role']}
@app.get('/api/regions')
def regions():return [region_view(r) for r in REGIONS]
@app.get('/api/regions/{region_id}')
def one_region(region_id:str):
    r=next((z for z in REGIONS if z['id']==region_id),None)
    if not r:raise HTTPException(404,'Region not found')
    return region_view(r)
@app.post('/api/risk/update')
def manual_risk(x:RiskUpdate):
    r=next((z for z in REGIONS if z['id']==x.region_id),None)
    if not r:raise HTTPException(404,'Region not found')
    rr=dict(r); rr.update({'rainfall_mm':x.rainfall_mm,'soil_moisture_pct':x.soil_moisture_pct,'data_status':'MANUAL_SIMULATION'}); s,l=risk(rr); fs,fl=flood(rr); return {'region_id':x.region_id,'rainfall_mm':x.rainfall_mm,'soil_moisture_pct':x.soil_moisture_pct,'risk_score':s,'risk_level':l,'flood_score':fs,'flood_level':fl}
@app.get('/api/user/location-risk')
def location_risk(lat:float,lng:float,authorization:str=Header(None)):
    u=auth_user(authorization); best=min(REGIONS,key=lambda r:(r['lat']-lat)**2+(r['lng']-lng)**2); return {'user':{k:u[k] for k in ['name','region_id','village']},'region':region_view(best),'location':{'lat':lat,'lng':lng}}
@app.get('/api/dashboard/summary')
def summary():
    rs=[region_view(r) for r in REGIONS]; return {'critical_zones':sum(x['risk_level']=='CRITICAL' for x in rs),'high_risk_zones':sum(x['risk_level']=='HIGH' for x in rs),'active_incidents':len(memory_incidents),'sensors_online':11,'roads_impacted':sum(x['road_status']!='OPEN' for x in rs),'population_at_risk':sum(x['population_at_risk'] for x in rs),'weather_source':'Open-Meteo LIVE','risk_data_mode':'LIVE_WEATHER_ADJUSTED'}
@app.get('/api/recipients')
def recipients():
    c=collection('recipients'); return list(c.find({}, {'_id':0}).limit(500)) if c is not None else memory_recipients
@app.post('/api/recipients')
def add_recipient(x:Recipient,authorization:str=Header(None)):
    auth_user(authorization); d=x.model_dump(); d['created_at']=datetime.now(timezone.utc).isoformat(); c=collection('recipients'); c.insert_one(d) if c is not None else memory_recipients.append(d); return d
@app.get('/api/alerts')
def alerts():
    c=collection('alerts'); return list(c.find({}, {'_id':0}).sort('created_at',-1).limit(100)) if c is not None else memory_alerts
@app.post('/api/alerts')
def create_alert(x:dict):
    d=dict(x); d['id']='ALT-'+secrets.token_hex(5).upper(); d['created_at']=datetime.now(timezone.utc).isoformat(); d.setdefault('status','QUEUED'); c=collection('alerts'); c.insert_one(d) if c is not None else memory_alerts.insert(0,d); return {'alert':d}
@app.post('/api/alerts/{alert_id}/dispatch')
def dispatch(alert_id:str):
    c=collection('alerts'); a=c.find_one({'id':alert_id}) if c is not None else next((z for z in memory_alerts if z['id']==alert_id),None)
    if not a:raise HTTPException(404,'Alert not found')
    rc=collection('recipients'); rec=list(rc.find({}, {'_id':0})) if rc is not None else memory_recipients; a.update({'status':'SIMULATED_SMS','recipient_count':len(rec),'delivered_count':len(rec),'provider':'SIMULATED_SMS','dispatched_at':datetime.now(timezone.utc).isoformat()});
    if c is not None:c.update_one({'id':alert_id},{'$set':a})
    return a
@app.get('/api/incidents')
def incidents():
    c=collection('incidents'); return list(c.find({}, {'_id':0}).sort('timestamp',-1).limit(100)) if c is not None else memory_incidents
@app.post('/api/incidents')
def create_incident(x:Incident):
    d=x.model_dump(); d.update({'id':'INC-'+secrets.token_hex(5).upper(),'status':'RECEIVED','timestamp':datetime.now(timezone.utc).isoformat()}); c=collection('incidents');
    if c is not None:c.insert_one(d)
    else:memory_incidents.insert(0,d)
    return {'incident':d}
@app.get('/api/flood')
def flood_api(region_id:str):
    r=next((z for z in REGIONS if z['id']==region_id),None)
    if not r:raise HTTPException(404,'Region not found')
    rr=region_view(r); return {'region_id':region_id,'flood_score':rr['flood_score'],'flood_level':rr['flood_level'],'rainfall_mm':rr['rainfall_mm'],'soil_moisture_pct':rr['soil_moisture_pct'],'source':rr['weather_source']}
