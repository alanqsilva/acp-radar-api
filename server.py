"""
ACPTelecom - Radar Financeiro: backend de dados (FastAPI + yfinance)

Endpoint:  GET /api/shark
Retorna S&P 500 futuro (%), VIX, dados D-1 (Close, High, Low, Amplitude)
e histórico recente do Ibovespa (^BVSP) para o motor Shark Flow WIN.
"""
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


def _hist(symbol: str, period: str = "20d"):
    """Candles diários recentes de um símbolo do Yahoo Finance."""
    df = yf.Ticker(symbol).history(period=period, interval="1d", auto_adjust=False)
    df = df.dropna(subset=["Close"])
    if len(df) < 2:
        raise ValueError(f"dados insuficientes para {symbol}")
    return df


def _last_and_pct(symbol: str):
    """Último preço e variação % contra o fechamento anterior."""
    c = _hist(symbol, period="10d")["Close"]
    last, prev = float(c.iloc[-1]), float(c.iloc[-2])
    return last, (last / prev - 1) * 100


def _build() -> dict:
    """Monta a resposta; cada ativo é isolado para que uma falha não derrube o resto."""
    now = datetime.now(SP)
    out = {"ok": False, "errors": {}, "updated": now.isoformat(timespec="seconds")}

    try:  # S&P 500 futuro (E-mini)
        last, pct = _last_and_pct("ES=F")
        out["sp_fut_price"], out["sp_fut_pct"] = round(last, 2), round(pct, 2)
    except Exception as e:
        out["errors"]["sp"] = str(e)

    try:  # VIX
        last, pct = _last_and_pct("^VIX")
        out["vix"], out["vix_pct"] = round(last, 2), round(pct, 2)
    except Exception as e:
        out["errors"]["vix"] = str(e)

    try:  # Ibovespa: D-1 + tabela histórica estilo Yahoo Finance
        df = _hist("^BVSP", period="20d")
        last_day = df.index[-1].date()
        # Se o pregão de hoje ainda está aberto (antes das 18h30 BRT), D-1 é o penúltimo candle.
        in_session = last_day == now.date() and (now.hour * 60 + now.minute) < 18 * 60 + 30
        d1_row = df.iloc[-2] if in_session else df.iloc[-1]

        close_d1 = round(float(d1_row["Close"]), 2)
        high_d1 = round(float(d1_row["High"]), 2)
        low_d1 = round(float(d1_row["Low"]), 2)

        out["ibov_close_d1"] = close_d1
        out["ibov_high_d1"] = high_d1
        out["ibov_low_d1"] = low_d1
        out["ibov_amp_d1"] = round(high_d1 - low_d1, 2)
        out["ibov_last"] = round(float(df["Close"].iloc[-1]), 2)
        out["ibov_prev"] = round(float(df["Close"].iloc[-2]), 2)

        # Últimos 10 pregões ordenados do mais recente para o mais antigo
        rows = []
        for idx, r in df.tail(10).iloc[::-1].iterrows():
            rows.append({
                "date": idx.strftime("%d/%m/%Y"),
                "open": round(float(r["Open"]), 2),
                "high": round(float(r["High"]), 2),
                "low": round(float(r["Low"]), 2),
                "close": round(float(r["Close"]), 2),
                "volume": int(r["Volume"]) if r["Volume"] > 0 else 0
            })
        out["history"] = rows
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