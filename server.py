"""
ACPTelecom - Radar Financeiro: backend FastAPI

Endpoints:
  GET /api/shark  -> S&P 500 futuro, VIX e histórico D-1 do Ibovespa
  GET /api/win-flow -> alias compatível com o dashboard atual (mesmo payload de /api/shark)
  GET /api/macro  -> Selic, IPCA, dólar PTAX e IBC-Br (Banco Central/SGS)
  GET /health     -> saúde básica da API

Variáveis de ambiente:
  CACHE_TTL=60                 Cache do endpoint /api/shark, em segundos
  MACRO_CACHE_TTL=900          Cache do endpoint /api/macro, em segundos
  ALLOWED_ORIGINS=*             Origens CORS separadas por vírgula
"""
from __future__ import annotations

import asyncio
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import yfinance as yf
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# -----------------------------------------------------------------------------
# Configuração
# -----------------------------------------------------------------------------
TTL = max(5, int(os.getenv("CACHE_TTL", "60")))
MACRO_TTL = max(60, int(os.getenv("MACRO_CACHE_TTL", "900")))
ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
SP = ZoneInfo("America/Sao_Paulo")
BCB_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"

app = FastAPI(title="ACP Radar API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS or ["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_shark_cache: dict[str, Any] = {"t": 0.0, "data": None}
_shark_lock = threading.Lock()

_macro_cache: dict[str, Any] = {"t": 0.0, "data": None}
_macro_lock = asyncio.Lock()

# Séries SGS utilizadas pelo painel macro.
# A data retornada ao navegador é normalizada para YYYY-MM-DD.
MACRO_SERIES = {
    "selic": {"code": "4189", "label": "Meta Selic", "unit": "% a.a."},
    "ipca": {"code": "433", "label": "IPCA mensal", "unit": "% a.m."},
    "usd_ptax": {"code": "1", "label": "Dólar PTAX venda", "unit": "R$/US$"},
    "ibc_br": {
        "code": "24364",
        "label": "IBC-Br dessazonalizado",
        "unit": "índice",
    },
}


# -----------------------------------------------------------------------------
# Shark Flow: dados de mercado via Yahoo Finance
# -----------------------------------------------------------------------------
def _hist(symbol: str, period: str = "20d"):
    """Candles diários recentes de um símbolo do Yahoo Finance."""
    df = yf.Ticker(symbol).history(
        period=period,
        interval="1d",
        auto_adjust=False,
    )
    df = df.dropna(subset=["Close"])
    if len(df) < 2:
        raise ValueError(f"dados insuficientes para {symbol}")
    return df


def _last_and_pct(symbol: str):
    """Último preço diário e variação contra o fechamento diário anterior."""
    closes = _hist(symbol, period="10d")["Close"]
    last, prev = float(closes.iloc[-1]), float(closes.iloc[-2])
    return last, (last / prev - 1) * 100


def _build_shark() -> dict[str, Any]:
    """Monta a resposta; a falha de um ativo não derruba os demais."""
    now = datetime.now(SP)
    out: dict[str, Any] = {
        "ok": False,
        "errors": {},
        "updated": now.isoformat(timespec="seconds"),
        "source": "Yahoo Finance",
    }

    try:  # S&P 500 futuro (E-mini)
        last, pct = _last_and_pct("ES=F")
        out["sp_fut_price"] = round(last, 2)
        out["sp_fut_pct"] = round(pct, 2)
    except Exception as exc:
        out["errors"]["sp"] = str(exc)

    try:  # VIX
        last, pct = _last_and_pct("^VIX")
        out["vix"] = round(last, 2)
        out["vix_pct"] = round(pct, 2)
    except Exception as exc:
        out["errors"]["vix"] = str(exc)

    try:  # Ibovespa: D-1 + histórico recente
        df = _hist("^BVSP", period="20d")
        last_day = df.index[-1].date()

        # Se o candle de hoje ainda está em formação, D-1 é o candle anterior.
        in_session = (
            last_day == now.date()
            and (now.hour * 60 + now.minute) < (18 * 60 + 30)
        )
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

        rows = []
        for idx, row in df.tail(10).iloc[::-1].iterrows():
            volume = row.get("Volume", 0)
            rows.append({
                "date": idx.strftime("%d/%m/%Y"),
                "open": round(float(row["Open"]), 2),
                "high": round(float(row["High"]), 2),
                "low": round(float(row["Low"]), 2),
                "close": round(float(row["Close"]), 2),
                "volume": int(volume) if volume and volume > 0 else 0,
            })
        out["history"] = rows
    except Exception as exc:
        out["errors"]["ibov"] = str(exc)

    out["ok"] = len(out["errors"]) < 3
    return out


@app.get("/api/shark")
@app.get("/api/win-flow")
def shark():
    """Mantém o contrato já utilizado pelo motor Shark Flow."""
    with _shark_lock:
        stale = (
            _shark_cache["data"] is None
            or time.time() - _shark_cache["t"] > TTL
        )
        if stale:
            fresh = _build_shark()
            # Se já existe cache, não substitui dados válidos por falha total.
            if fresh["ok"] or _shark_cache["data"] is None:
                _shark_cache["t"] = time.time()
                _shark_cache["data"] = fresh
        return _shark_cache["data"]


# -----------------------------------------------------------------------------
# Contexto Macro: Banco Central do Brasil / SGS
# -----------------------------------------------------------------------------
def _iso_bcb_date(value: str | None) -> str | None:
    """Converte a data SGS DD/MM/YYYY para YYYY-MM-DD, amigável ao JavaScript."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%d/%m/%Y").date().isoformat()
    except ValueError:
        return value


async def _bcb_last(client: httpx.AsyncClient, code: str) -> dict[str, Any]:
    url = f"{BCB_BASE}.{code}/dados/ultimos/1?formato=json"
    response = await client.get(url)
    response.raise_for_status()
    rows = response.json()

    if not rows:
        return {"value": None, "date": None}

    row = rows[-1]
    raw_value = row.get("valor")
    try:
        value = float(str(raw_value).replace(",", "."))
    except (TypeError, ValueError):
        value = None

    return {
        "value": value,
        "date": _iso_bcb_date(row.get("data")),
    }


async def _build_macro() -> dict[str, Any]:
    keys = list(MACRO_SERIES)
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(12.0, connect=5.0),
        headers={"Accept": "application/json", "User-Agent": "ACP-Radar/2.0"},
    ) as client:
        values = await asyncio.gather(
            *[_bcb_last(client, MACRO_SERIES[key]["code"]) for key in keys],
            return_exceptions=True,
        )

    result: dict[str, Any] = {}
    for key, item in zip(keys, values):
        meta = MACRO_SERIES[key]
        if isinstance(item, Exception):
            result[key] = {
                "value": None,
                "date": None,
                "label": meta["label"],
                "unit": meta["unit"],
                "error": str(item),
            }
        else:
            result[key] = {
                **item,
                "label": meta["label"],
                "unit": meta["unit"],
            }

    return {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "series": result,
        "meta": {
            "source": "Banco Central do Brasil — SGS",
            "cache_ttl_seconds": MACRO_TTL,
            "notes": [
                "A data é a observação publicada, não o horário da consulta.",
                "As séries possuem periodicidades e defasagens distintas.",
                "IBC-Br é indicador de atividade, não cotação intradiária.",
                "Confira unidade e descrição no catálogo SGS antes de modelar sinais.",
                "Informação contextual; não constitui recomendação de investimento.",
            ],
        },
    }


@app.get("/api/macro")
async def macro():
    """Entrega os últimos dados macro do SGS, com cache independente de /api/shark."""
    async with _macro_lock:
        stale = (
            _macro_cache["data"] is None
            or time.time() - _macro_cache["t"] > MACRO_TTL
        )
        if stale:
            try:
                fresh = await _build_macro()
                _macro_cache["t"] = time.time()
                _macro_cache["data"] = fresh
            except Exception as exc:
                # Se houver cache anterior, mantém a última resposta conhecida.
                if _macro_cache["data"] is not None:
                    cached = dict(_macro_cache["data"])
                    cached["cache_warning"] = str(exc)
                    return cached
                return {
                    "as_of": datetime.now(timezone.utc).isoformat(),
                    "series": {
                        key: {
                            "value": None,
                            "date": None,
                            "label": meta["label"],
                            "unit": meta["unit"],
                            "error": str(exc),
                        }
                        for key, meta in MACRO_SERIES.items()
                    },
                    "meta": {"source": "Banco Central do Brasil — SGS"},
                    "ok": False,
                }
        return _macro_cache["data"]


# -----------------------------------------------------------------------------
# Health check
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "ACP Radar API",
        "version": app.version,
        "endpoints": ["/api/shark", "/api/win-flow", "/api/macro", "/health"],
        "server_time": datetime.now(SP).isoformat(timespec="seconds"),
    }
