from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import hashlib
import sqlite3
import os
import json
from datetime import datetime
import requests
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()


@app.get("/")
def home():
    return {"message": "Backend is working"}


@app.get("/api/health")
def health():
    return {"status": "ok"}


DATA_ROOT = os.getenv("SENTINEL_DATA_DIR", "/tmp/sentinel")
UPLOAD_DIR = os.path.join(DATA_ROOT, "datasets")
DB_PATH = os.path.join(DATA_ROOT, "sentinel.db")
GEOCODE_CACHE_PATH = os.path.join(DATA_ROOT, "geo_cache.json")
os.makedirs(UPLOAD_DIR, exist_ok=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def safe_name(filename: str) -> str:
    cleaned = os.path.basename((filename or "").strip())
    if not cleaned:
        raise HTTPException(status_code=400, detail="filename is required")
    return cleaned


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT)")
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS history (id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT, timestamp TEXT)"
    )
    cursor.execute("INSERT OR IGNORE INTO users VALUES (?, ?)", ("admin", hashlib.sha256("admin123".encode()).hexdigest()))
    conn.commit()
    conn.close()


def log_action(action):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO history (action, timestamp) VALUES (?, ?)",
            (action, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


init_db()

CURRENT_DF = None
ACTIVE_FILE = "None"
GEOCODE_USER_AGENT = "SentinelMapGeocoder/2.0"
GEOCODE_TIMEOUT = 8
MAX_MAP_ITEMS = 500
MAX_GEOCODE_UNIQUE = 150
GEOCODE_COL_CANDIDATES = [
    "location",
    "city",
    "place",
    "venue",
    "stadium",
    "name",
    "district",
    "state",
    "country",
    "address",
]


def encrypt(pw):
    return hashlib.sha256(pw.encode()).hexdigest()


def load_geo_cache() -> dict:
    try:
        if os.path.exists(GEOCODE_CACHE_PATH):
            with open(GEOCODE_CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


def save_geo_cache(cache: dict) -> None:
    try:
        os.makedirs(os.path.dirname(GEOCODE_CACHE_PATH), exist_ok=True)
        with open(GEOCODE_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def geocode_place(place: str, cache: dict):
    key = (place or "").strip().lower()
    if not key:
        return None

    cached = cache.get(key)
    if isinstance(cached, dict) and "lat" in cached and "lng" in cached:
        return cached

    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"format": "json", "limit": 1, "q": place},
            headers={"User-Agent": GEOCODE_USER_AGENT},
            timeout=GEOCODE_TIMEOUT,
        )
        if resp.status_code == 200:
            payload = resp.json() or []
            if payload:
                loc = payload[0]
                point = {
                    "lat": float(loc["lat"]),
                    "lng": float(loc["lon"]),
                }
                cache[key] = point
                return point
    except Exception:
        pass

    cache[key] = None
    return None


@app.post("/api/register")
async def register(data: dict):
    u, p = data.get("username"), data.get("password")
    if not u or not p:
        raise HTTPException(status_code=400, detail="ID and Key required")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO users VALUES (?, ?)", (u, encrypt(p)))
        conn.commit()
        return {"status": "OK"}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="Username taken.")
    finally:
        conn.close()


@app.post("/api/login")
async def login(data: dict):
    u, p = data.get("username"), data.get("password")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT password FROM users WHERE username=?", (u,))
    res = cursor.fetchone()
    conn.close()
    if res and res[0] == encrypt(p):
        return {"status": "OK"}
    raise HTTPException(status_code=401, detail="Invalid Credentials")


@app.post("/api/upload")
async def upload(file: UploadFile = File(...)):
    global CURRENT_DF, ACTIVE_FILE
    try:
        filename = safe_name(file.filename)
        content = await file.read()
        path = os.path.join(UPLOAD_DIR, filename)
        with open(path, "wb") as f:
            f.write(content)

        if filename.lower().endswith((".xlsx", ".xls")):
            CURRENT_DF = pd.read_excel(path)
        else:
            try:
                CURRENT_DF = pd.read_csv(path, low_memory=False)
            except UnicodeDecodeError:
                CURRENT_DF = pd.read_csv(path, encoding="latin1", low_memory=False)
            except Exception:
                CURRENT_DF = pd.read_csv(path, engine="python", on_bad_lines="skip")

        CURRENT_DF.columns = CURRENT_DF.columns.astype(str).str.strip().str.replace("\n", " ").str.replace("\r", "")
        ACTIVE_FILE = filename
        log_action(f"Ingested Dataset: {filename}")
        return {"cols": list(CURRENT_DF.columns)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/clean")
async def clean_data():
    global CURRENT_DF
    if CURRENT_DF is None:
        raise HTTPException(status_code=400, detail="No dataset loaded.")
    before = len(CURRENT_DF)
    CURRENT_DF = CURRENT_DF.dropna()
    removed = before - len(CURRENT_DF)
    log_action(f"Cleansed: {removed} null rows purged.")
    return {"status": "OK", "removed": removed, "cols": list(CURRENT_DF.columns)}


@app.post("/api/reuse")
async def reuse(data: dict):
    global CURRENT_DF, ACTIVE_FILE
    name = safe_name(data.get("filename"))
    path = os.path.join(UPLOAD_DIR, name)
    if os.path.exists(path):
        try:
            if name.lower().endswith((".xlsx", ".xls")):
                CURRENT_DF = pd.read_excel(path)
            else:
                try:
                    CURRENT_DF = pd.read_csv(path, low_memory=False)
                except Exception:
                    CURRENT_DF = pd.read_csv(path, encoding="latin1", low_memory=False)

            CURRENT_DF.columns = CURRENT_DF.columns.astype(str).str.strip().str.replace("\n", " ").str.replace("\r", "")
            ACTIVE_FILE = name
            log_action(f"Restored Memory: {name}")
            return {"cols": list(CURRENT_DF.columns)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Corrupted file: {e}")
    raise HTTPException(status_code=404)


@app.delete("/api/purge/{name}")
async def purge(name: str):
    global CURRENT_DF, ACTIVE_FILE
    safe = safe_name(name)
    path = os.path.join(UPLOAD_DIR, safe)
    if os.path.exists(path):
        os.remove(path)
        if ACTIVE_FILE == safe:
            ACTIVE_FILE, CURRENT_DF = "None", None
        log_action(f"Deleted Memory: {safe}")
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
    except Exception:
        return {"files": [], "active": "None", "history": [], "cols": []}




@app.get("/api/data")
async def get_data(page: int = 1, query: str = ""):
    if CURRENT_DF is None:
        return {"rows": [], "total": 1}

    page_size = 20
    filtered_df = CURRENT_DF

    q = (query or "").strip().lower()
    if q:
        mask = filtered_df.astype(str).apply(lambda col: col.str.lower().str.contains(q, na=False))
        filtered_df = filtered_df[mask.any(axis=1)]

    total_pages = max(1, (len(filtered_df) + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size

    rows = filtered_df.iloc[start:end].fillna("").to_dict(orient="records")
    return {"rows": rows, "total": total_pages}


@app.get("/api/map_data")
async def map_data():
    if CURRENT_DF is None:
        return []

    cols = {c.lower(): c for c in CURRENT_DF.columns}

    lat_col = next((cols[k] for k in ["lat", "latitude", "y"] if k in cols), None)
    lng_col = next((cols[k] for k in ["lng", "lon", "long", "longitude", "x"] if k in cols), None)
    name_col = next((cols[k] for k in ["name", "city", "venue", "location", "stadium"] if k in cols), None)

    df = CURRENT_DF.copy()
    items = []

    if lat_col and lng_col:
        df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
        df[lng_col] = pd.to_numeric(df[lng_col], errors="coerce")
        df = df.dropna(subset=[lat_col, lng_col])

        for _, row in df.head(MAX_MAP_ITEMS).iterrows():
            items.append(
                {
                    "name": str(row.get(name_col, "Point")) if name_col else "Point",
                    "lat": float(row[lat_col]),
                    "lng": float(row[lng_col]),
                }
            )
        return items

    geocode_col = next((cols[k] for k in GEOCODE_COL_CANDIDATES if k in cols), None)
    if not geocode_col:
        return []

    cache = load_geo_cache()
    geocoded_count = 0
    subset = df.head(MAX_MAP_ITEMS)

    for _, row in subset.iterrows():
        place_value = row.get(geocode_col)
        if pd.isna(place_value):
            continue

        place = str(place_value).strip()
        if not place:
            continue

        point = geocode_place(place, cache)
        if point is None:
            continue

        geocoded_count += 1
        items.append(
            {
                "name": str(row.get(name_col, place)) if name_col else place,
                "lat": point["lat"],
                "lng": point["lng"],
            }
        )
        if geocoded_count >= MAX_GEOCODE_UNIQUE:
            break

    save_geo_cache(cache)
    return items


@app.post("/api/viz")
async def viz(data: dict):
    if CURRENT_DF is None:
        return []

    col = (data.get("col") or "").strip()
    if not col:
        return []
    if col not in CURRENT_DF.columns:
        raise HTTPException(status_code=400, detail=f"Column '{col}' not found")

    series = CURRENT_DF[col].dropna().astype(str).str.strip()
    series = series[series != ""]

    counts = series.value_counts().head(50)
    return [{"name": str(k), "value": int(v)} for k, v in counts.items()]

@app.post("/api/ai")
def ask_ai(payload: dict):
    if CURRENT_DF is None:
        return {"ans": "System offline. Please upload a dataset."}

    keys = [
        os.getenv("GEMINI_API_KEY_1"),
        os.getenv("GEMINI_API_KEY_2"),
        os.getenv("GEMINI_API_KEY_3"),
        os.getenv("GEMINI_API_KEY_4"),
        os.getenv("GEMINI_API_KEY_5"),
    ]

    api_key = next((k for k in keys if k), None)

    if not api_key:
        return {"ans": "No API keys configured."}

    user_q = payload.get("q", "")
    if not user_q:
        return {"ans": "No question provided."}

    cols = list(CURRENT_DF.columns)[:15]

    prompt = f"You are Sentinel, a data AI. Dataset columns: {cols}. User asks: {user_q}. Keep your answer brief and helpful."
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
    headers = {"Content-Type": "application/json"}
    data = {"contents": [{"parts": [{"text": prompt}]}]}

    try:
        resp = requests.post(url, headers=headers, json=data, timeout=15)
        if resp.status_code == 200:
            result = resp.json()
            answer = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "No response")
            return {"ans": answer}
        return {"ans": f"Google Server Error Code {resp.status_code}"}
    except Exception as e:
        return {"ans": f"System Error: {str(e)}"}
