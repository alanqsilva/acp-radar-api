import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import yfinance as yf
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

TTL = int(os.getenv("CACHE_TTL", "60"))
ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",")]
SP = ZoneInfo("America/Sao_Paulo")

app = FastAPI(title="ACP Radar API")
app.add_middleware(CORSMiddleware, allow_origins=ORIGINS, allow_methods=["GET"], allow_headers=["*"])

_cache = {"t": 0.0, "data": None}
_lock = threading.Lock()

def _hist(symbol: str):
    df = yf.Ticker(symbol).history(period="10d", interval="1d", auto_adjust=False)
    df = df.dropna(subset=["Close"])
    if len(df) < 2:
        raise ValueError(f"dados insuficientes para {symbol}")
    return df

def _last_and_pct(symbol: str):
    c = _hist(symbol)["Close"]
    last, prev = float(c.iloc[-1]), float(c.iloc[-2])
    return last, (last / prev - 1) * 100

def _build() -> dict:
    now = datetime.now(SP)
    out = {"ok": False, "errors": {}, "updated": now.isoformat(timespec="seconds")}

    try:
        last, pct = _last_and_pct("ES=F")
        out["sp_fut_price"], out["sp_fut_pct"] = round(last, 2), round(pct, 2)
    except Exception as e:
        out["errors"]["sp"] = str(e)

    try:
        last, pct = _last_and_pct("^VIX")
        out["vix"], out["vix_pct"] = round(last, 2), round(pct, 2)
    except Exception as e:
        out["errors"]["vix"] = str(e)

    try:
        c = _hist("^BVSP")["Close"]
        last_day = c.index[-1].date()
        in_session = last_day == now.date() and (now.hour * 60 + now.minute) < 18 * 60 + 30
        out["ibov_close_d1"] = round(float(c.iloc[-2] if in_session else c.iloc[-1]), 0)
        out["ibov_last"] = round(float(c.iloc[-1]), 0)
    except Exception as e:
        out["errors"]["ibov"] = str(e)

    out["ok"] = len(out["errors"]) < 3
    return out

@app.get("/api/shark")
def shark():
    with _lock:
        stale = _cache["data"] is None or time.time() - _cache["t"] > TTL
        if stale:
            data = _build()
            if data["ok"] or _cache["data"] is None:
                _cache["t"], _cache["data"] = time.time(), data
        return _cache["data"]

@app.get("/health")
def health():
    return {"status": "ok"}
