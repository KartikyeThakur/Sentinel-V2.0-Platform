from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import hashlib, sqlite3, os, uvicorn
from datetime import datetime
import requests
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Backend is working "}

UPLOAD_DIR = "/tmp/datasets"
DB_PATH = "/tmp/sentinel.db"
os.makedirs(UPLOAD_DIR, exist_ok=True)

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_credentials=True, 
    allow_methods=["*"], 
    allow_headers=["*"]
)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT, timestamp TEXT)")
    cursor.execute("INSERT OR IGNORE INTO users VALUES (?, ?)", ("admin", hashlib.sha256("admin123".encode()).hexdigest()))
    conn.commit()
    conn.close()

def log_action(action):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO history (action, timestamp) VALUES (?, ?)", (action, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()
        conn.close()
    except: pass

init_db()

CURRENT_DF = None
ACTIVE_FILE = "None"

def encrypt(pw): return hashlib.sha256(pw.encode()).hexdigest()

@app.post("/api/register")
async def register(data: dict):
    u, p = data.get("username"), data.get("password")
    if not u or not p: raise HTTPException(status_code=400, detail="ID and Key required")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO users VALUES (?, ?)", (u, encrypt(p)))
        conn.commit()
        return {"status": "OK"}
    except sqlite3.IntegrityError: 
        raise HTTPException(status_code=400, detail="Username taken.")
    finally: conn.close()

@app.post("/api/login")
async def login(data: dict):
    u, p = data.get("username"), data.get("password")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT password FROM users WHERE username=?", (u,))
    res = cursor.fetchone()
    conn.close()
    if res and res[0] == encrypt(p): return {"status": "OK"}
    raise HTTPException(status_code=401, detail="Invalid Credentials")

@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    global CURRENT_DF, ACTIVE_FILE
    try:
        content = await file.read()
        path = os.path.join(UPLOAD_DIR, file.filename)
        with open(path, "wb") as f: f.write(content)
        
        if file.filename.lower().endswith(('.xlsx', '.xls')):
            CURRENT_DF = pd.read_excel(path)
        else:
            try: CURRENT_DF = pd.read_csv(path, low_memory=False)
            except UnicodeDecodeError: CURRENT_DF = pd.read_csv(path, encoding='latin1', low_memory=False)
            except Exception: CURRENT_DF = pd.read_csv(path, engine='python', on_bad_lines='skip')

        CURRENT_DF.columns = CURRENT_DF.columns.astype(str).str.strip().str.replace('\n', ' ').str.replace('\r', '')
        ACTIVE_FILE = file.filename
        log_action(f"Ingested Dataset: {file.filename}")
        return {"cols": list(CURRENT_DF.columns)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/clean")
async def clean_data():
    global CURRENT_DF
    if CURRENT_DF is None: raise HTTPException(status_code=400, detail="No dataset loaded.")
    before = len(CURRENT_DF)
    CURRENT_DF = CURRENT_DF.dropna()
    removed = before - len(CURRENT_DF)
    log_action(f"Cleansed: {removed} null rows purged.")
    return {"status": "OK", "removed": removed, "cols": list(CURRENT_DF.columns)}

@app.post("/api/reuse")
async def reuse(data: dict):
    global CURRENT_DF, ACTIVE_FILE
    name = data.get("filename")
    path = os.path.join(UPLOAD_DIR, name)
    if os.path.exists(path):
        try:
            if name.lower().endswith(('.xlsx', '.xls')): CURRENT_DF = pd.read_excel(path)
            else: 
                try: CURRENT_DF = pd.read_csv(path, low_memory=False)
                except: CURRENT_DF = pd.read_csv(path, encoding='latin1', low_memory=False)
            
            CURRENT_DF.columns = CURRENT_DF.columns.astype(str).str.strip().str.replace('\n', ' ').str.replace('\r', '')
            ACTIVE_FILE = name
            log_action(f"Restored Memory: {name}")
            return {"cols": list(CURRENT_DF.columns)}
        except Exception as e: 
            raise HTTPException(status_code=500, detail=f"Corrupted file: {e}")
    raise HTTPException(status_code=404)

@app.delete("/api/purge/{name}")
async def purge(name: str):
    global CURRENT_DF, ACTIVE_FILE
    path = os.path.join(UPLOAD_DIR, name)
    if os.path.exists(path):
        os.remove(path)
        if ACTIVE_FILE == name: ACTIVE_FILE, CURRENT_DF = "None", None
        log_action(f"Deleted Memory: {name}")
    return {"status": "OK"}

@app.get("/api/info")
async def get_info():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT action, timestamp FROM history ORDER BY id DESC LIMIT 20")
        h = cursor.fetchall()
        conn.close()
        cols = list(CURRENT_DF.columns) if CURRENT_DF is not None else []
        files = os.listdir(UPLOAD_DIR) if os.path.exists(UPLOAD_DIR) else []
        return {"files": files, "active": ACTIVE_FILE, "history": h, "cols": cols}
    except Exception: return {"files": [], "active": "None", "history": [], "cols": []}

@app.post("/api/viz")
async def get_viz(payload: dict):
    if CURRENT_DF is None or CURRENT_DF.empty: return []
    col = payload.get("col")
    if not col or col not in CURRENT_DF.columns: col = CURRENT_DF.columns[0]
    series = CURRENT_DF[col].dropna()
    if series.empty: return [] 
    res = series.value_counts().head(20).reset_index()
    res.columns = ['name', 'value']
    res['name'] = res['name'].astype(str)
    res['value'] = res['value'].astype(int)
    return res.to_dict(orient="records")

@app.get("/api/data")
async def get_data(page: int = 1, query: str = ""):
    if CURRENT_DF is None: return {"rows": [], "total": 0}
    df = CURRENT_DF
    if query: df = df[df.apply(lambda r: r.astype(str).str.contains(query, case=False).any(), axis=1)]
    limit = 4
    start = (page - 1) * limit
    return {"rows": df.iloc[start:start+limit].fillna("N/A").to_dict(orient="records"), "total": (len(df)//limit)+1}

@app.get("/api/map_data")
async def get_map():
    if CURRENT_DF is None: return []
    geo_cols = [c for c in CURRENT_DF.columns if any(x in c.lower() for x in ['venue', 'city', 'location'])]
    if not geo_cols: return []
    res = CURRENT_DF[geo_cols[0]].value_counts().head(50).reset_index()
    res.columns = ['name', 'count']
    
    # ... (map coordinates list stays the same) ...
    geo_dict = {"mumbai": [19.0760, 72.8777], "delhi": [28.7041, 77.1025], "bangalore": [12.9716, 77.5946]} 
    
    points = []
    for i, row in res.iterrows():
        name_str = str(row['name']).lower().strip()
        lat, lng = None, None
        for key, coords in geo_dict.items():
            if key in name_str:
                lat, lng = coords[0], coords[1]
                break
        if lat and lng:
            points.append({"name": str(row['name']), "lat": lat, "lng": lng, "count": int(row['count'])})
    return points

@app.delete("/api/clear_history")
async def clear():
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM history"); conn.commit(); conn.close()
        return {"status": "OK"}
    except: return {"status": "FAILED"}

@app.post("/api/ai")
def ask_ai(payload: dict):
    if CURRENT_DF is None: 
        return {"ans": "System offline. Please upload a dataset."}
    
    api_key = os.getenv("GOOGLE_API_KEY")
    
    user_q = payload.get('q', '')
    cols = list(CURRENT_DF.columns)[:15]
    
    prompt = f"You are Sentinel, a data AI. Dataset columns: {cols}. User asks: {user_q}. Keep your answer brief and helpful."
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
    headers = {'Content-Type': 'application/json'}
    data = {"contents": [{"parts": [{"text": prompt}]}]}
    
    try:
        resp = requests.post(url, headers=headers, json=data, timeout=15)
        if resp.status_code == 200:
            result = resp.json()
            answer = result['candidates'][0]['content']['parts'][0]['text']
            return {"ans": answer}
        else:
            return {"ans": f"Google Server Error Code {resp.status_code}."}
    except Exception as e:
        return {"ans": f"System Error: {str(e)}"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
