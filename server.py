"""
ACPTelecom - Radar Financeiro (v2.4.0)
Backend FastAPI para Render — Dados reais (sem valores simulados/chumbados).

Endpoints:
  GET /api/shark     -> Ativos globais, IBOV (histórico + movimento do dia),
                        Câmbio multi-moedas (AwesomeAPI) e Cotações B3 (brapi)
  GET /api/win-flow  -> alias de compatibilidade para /api/shark
  GET /api/macro     -> Selic, IPCA, PTAX venda, IBC-Br e setor externo (BCB/SGS)
  GET /health        -> saúde da aplicação
"""

import asyncio
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pandas as pd
import yfinance as yf
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------

TTL = int(os.getenv("CACHE_TTL", "45"))
MACRO_TTL = int(os.getenv("MACRO_CACHE_TTL", "900"))
BRAPI_TTL = int(os.getenv("BRAPI_CACHE_TTL", "90"))
BRAPI_TOKEN = os.getenv("BRAPI_TOKEN", "").strip()

ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
]
SP = ZoneInfo("America/Sao_Paulo")

app = FastAPI(title="ACP Radar API", version="2.4.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS or ["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_shark_cache = {"t": 0.0, "data": None}
_shark_lock = threading.Lock()

_brapi_cache = {"t": 0.0, "data": None}
_brapi_lock = threading.Lock()

_macro_cache = {"t": 0.0, "data": None}
_macro_lock = asyncio.Lock()

BCB_BASE = "https://api.bcb.gov.br/dados/serie/bcdata.sgs"
AWESOME_FX_URL = (
    "https://economia.awesomeapi.com.br/last/"
    "USD-BRL,EUR-BRL,GBP-BRL,JPY-BRL,CAD-BRL,MXN-BRL,CNY-BRL"
)

BCB_SERIES = {
    "selic": {"code": "4189", "label": "Selic anualizada", "unit": "% a.a."},
    "selic_meta": {"code": "432", "label": "Meta Selic (Copom)", "unit": "% a.a."},
    "ipca": {"code": "433", "label": "IPCA mensal", "unit": "% a.m."},
    "usd_ptax": {"code": "1", "label": "Dólar PTAX venda", "unit": "R$/US$"},
    "ibc_br": {"code": "24364", "label": "IBC-Br dessazonalizado", "unit": "índice"},
    "balanca_comercial": {
        "code": "22707",
        "label": "Balança comercial (saldo)",
        "unit": "US$ milhões",
    },
    "transacoes_correntes": {
        "code": "22701",
        "label": "Transações correntes (saldo)",
        "unit": "US$ milhões",
    },
}

GLOBAL_SYMBOLS = {
    "sp_fut": "ES=F",
    "nq_fut": "NQ=F",
    "vix": "^VIX",
    "dxy": "DX-Y.NYB",
    "ewz": "EWZ",
    "wti": "CL=F",
    "brent": "BZ=F",
    "us10y": "^TNX",
}

B3_PESOS_TICKERS = [
    "VALE3",
    "PETR4",
    "ITUB4",
    "PETR3",
    "BBDC4",
    "B3SA3",
    "ELET3",
    "BBAS3",
    "ABEV3",
    "RENT3",
    "WEGE3",
    "ITSA4",
    "PRIO3",
    "BPAC11",
    "EQTL3",
    "SUZB3",
    "RDOR3",
    "GGBR4",
    "MGLU3",
]
B3_FREE_FALLBACK_TICKERS = ["VALE3", "PETR4", "ITUB4", "MGLU3"]


# ---------------------------------------------------------------------
# Helpers de Coleta Real
# ---------------------------------------------------------------------

def _hist(symbol: str, period: str = "20d") -> pd.DataFrame:
    df = yf.Ticker(symbol).history(
        period=period,
        interval="1d",
        auto_adjust=False,
    )
    df = df.dropna(subset=["Close"])
    if len(df) < 2:
        raise ValueError(f"dados insuficientes para {symbol}")
    return df


def _last_and_pct_single(symbol: str, decimals: int = 2) -> tuple[float, float]:
    closes = _hist(symbol, period="10d")["Close"]
    last = float(closes.iloc[-1])
    previous = float(closes.iloc[-2])
    return round(last, decimals), round((last / previous - 1) * 100, 2)


def _fetch_global_batch() -> tuple[dict, dict]:
    parsed = {}
    errors = {}
    symbols = list(GLOBAL_SYMBOLS.values())

    try:
        raw = yf.download(
            tickers=symbols,
            period="10d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
    except Exception as exc:
        raw = None
        errors["batch"] = str(exc)

    for key, sym in GLOBAL_SYMBOLS.items():
        dec = 3 if key == "dxy" else 2
        extracted = False

        if raw is not None and not raw.empty:
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if sym in raw.columns.get_level_values(0):
                        closes = raw[sym]["Close"].dropna()
                    else:
                        closes = pd.Series(dtype=float)
                else:
                    closes = raw["Close"].dropna()

                if len(closes) >= 2:
                    last = float(closes.iloc[-1])
                    prev = float(closes.iloc[-2])
                    parsed[key] = (
                        round(last, dec),
                        round((last / prev - 1) * 100, 2),
                    )
                    extracted = True
            except Exception:
                extracted = False

        if not extracted:
            try:
                parsed[key] = _last_and_pct_single(sym, decimals=dec)
            except Exception as exc:
                errors[key] = str(exc)

    return parsed, errors


def _calc_fibonacci_levels(high: float, low: float) -> dict:
    amp = high - low
    return {
        "amp": round(amp, 2),
        "ext_minus_11_8": round(low - (amp * 0.118), 2),
        "ret_38_2": round(low + (amp * 0.382), 2),
        "ret_50_0": round(low + (amp * 0.500), 2),
        "ret_61_8": round(low + (amp * 0.618), 2),
        "ext_plus_111_8": round(low + (amp * 1.118), 2),
    }


def _fetch_realtime_fx(timeout_s: float = 8.0) -> dict:
    headers = {"Accept": "application/json", "User-Agent": "ACPTelecomRadar/2.4"}
    with httpx.Client(timeout=timeout_s, headers=headers) as client:
        resp = client.get(AWESOME_FX_URL)
        resp.raise_for_status()
        raw = resp.json()

    pairs = [
        ("USDBRL", "usd_brl", 4),
        ("EURBRL", "eur_brl", 4),
        ("GBPBRL", "gbp_brl", 4),
        ("JPYBRL", "jpy_brl", 4),
        ("CADBRL", "cad_brl", 4),
        ("MXNBRL", "mxn_brl", 4),
        ("CNYBRL", "cny_brl", 4),
    ]
    fx = {}
    for pair_key, out_key, dec in pairs:
        item = raw.get(pair_key)
        if item and item.get("bid") is not None:
            fx[out_key] = {
                "price": round(float(item["bid"]), dec),
                "change": round(float(item.get("pctChange") or 0.0), 2),
                "high": round(float(item["high"]), dec) if item.get("high") is not None else None,
                "low": round(float(item["low"]), dec) if item.get("low") is not None else None,
                "timestamp": item.get("create_date"),
            }
    return fx


def _fetch_brapi_quotes(tickers: list[str], timeout_s: float = 12.0) -> dict:
    symbols = ",".join(tickers)
    url = f"https://brapi.dev/api/quote/{symbols}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 ACPTelecomRadar/2.4",
    }
    if BRAPI_TOKEN:
        headers["Authorization"] = f"Bearer {BRAPI_TOKEN}"

    with httpx.Client(timeout=timeout_s, headers=headers) as client:
        resp = client.get(url)
        resp.raise_for_status()
        payload = resp.json()

    results = payload.get("results") or []
    quotes = {}
    for item in results:
        sym = item.get("symbol")
        if sym:
            price = item.get("regularMarketPrice")
            change = item.get("regularMarketChangePercent")
            quotes[sym] = {
                "price": round(float(price), 2) if price is not None else None,
                "change": round(float(change), 2) if change is not None else None,
                "name": item.get("shortName") or item.get("longName") or sym,
            }
    return quotes


def _get_b3_quotes() -> tuple[dict, str | None]:
    with _brapi_lock:
        stale = (
            _brapi_cache["data"] is None
            or (time.time() - _brapi_cache["t"]) > BRAPI_TTL
        )
        if stale:
            try:
                fresh = _fetch_brapi_quotes(B3_PESOS_TICKERS)
                if fresh:
                    _brapi_cache["data"] = fresh
                    _brapi_cache["t"] = time.time()
                    return _brapi_cache["data"], None
            except Exception as exc:
                if not BRAPI_TOKEN:
                    try:
                        free_quotes = _fetch_brapi_quotes(B3_FREE_FALLBACK_TICKERS)
                        if free_quotes:
                            _brapi_cache["data"] = free_quotes
                            _brapi_cache["t"] = time.time()
                            return _brapi_cache["data"], None
                    except Exception as free_exc:
                        if _brapi_cache["data"] is not None:
                            return _brapi_cache["data"], str(free_exc)
                        return {}, str(free_exc)
                if _brapi_cache["data"] is not None:
                    return _brapi_cache["data"], str(exc)
                return {}, str(exc)

        return _brapi_cache["data"] or {}, None


def _build_shark() -> dict:
    now = datetime.now(SP)
    out = {
        "ok": False,
        "errors": {},
        "updated": now.isoformat(timespec="seconds"),
        "source": "Yahoo Finance + AwesomeAPI FX + BCB SGS + brapi",
    }

    with ThreadPoolExecutor(max_workers=4) as pool:
        global_fut = pool.submit(_fetch_global_batch)
        fx_fut = pool.submit(_fetch_realtime_fx)
        b3_fut = pool.submit(_get_b3_quotes)
        ibov_fut = pool.submit(_hist, "^BVSP", "20d")

        global_data, global_errs = global_fut.result()
        out["errors"].update(global_errs)

        for key, (price, pct) in global_data.items():
            if key == "vix":
                out["vix"] = price
                out["vix_pct"] = pct
            else:
                out[f"{key}_price"] = price
                out[f"{key}_pct"] = pct
                obj_key = "nasdaq" if key == "nq_fut" else key
                out[obj_key] = {"price": price, "change": pct}

        try:
            out["fx"] = fx_fut.result()
        except Exception as exc:
            out["fx"] = {}
            out["errors"]["fx"] = str(exc)

        b3_quotes, brapi_err = b3_fut.result()
        out["b3_quotes"] = b3_quotes
        if brapi_err:
            out["errors"]["brapi"] = brapi_err

        try:
            df = ibov_fut.result()
            last_day = df.index[-1].date()

            in_session = (
                last_day == now.date()
                and (now.hour * 60 + now.minute) < 18 * 60 + 30
            )
            d1_row = df.iloc[-2] if in_session else df.iloc[-1]
            latest_row = df.iloc[-1]

            close_d1 = round(float(d1_row["Close"]), 2)
            high_d1 = round(float(d1_row["High"]), 2)
            low_d1 = round(float(d1_row["Low"]), 2)

            ibov_last = round(float(latest_row["Close"]), 2)
            ibov_prev = round(float(df["Close"].iloc[-2]), 2)
            ibov_open_today = round(float(latest_row["Open"]), 2)
            ibov_high_today = round(float(latest_row["High"]), 2)
            ibov_low_today = round(float(latest_row["Low"]), 2)
            ibov_day_pct = round(((ibov_last / ibov_open_today) - 1) * 100, 2) if ibov_open_today > 0 else 0.0

            out["ibov_close_d1"] = close_d1
            out["ibov_high_d1"] = high_d1
            out["ibov_low_d1"] = low_d1
            out["ibov_amp_d1"] = round(high_d1 - low_d1, 2)
            out["ibov_last"] = ibov_last
            out["ibov_prev"] = ibov_prev
            out["ibov_open_today"] = ibov_open_today
            out["ibov_high_today"] = ibov_high_today
            out["ibov_low_today"] = ibov_low_today
            out["ibov_day_pct"] = ibov_day_pct
            out["fibo_ibov"] = _calc_fibonacci_levels(high_d1, low_d1)

            rows = []
            for idx, row in df.tail(10).iloc[::-1].iterrows():
                rows.append({
                    "date": idx.strftime("%d/%m/%Y"),
                    "open": round(float(row["Open"]), 2),
                    "high": round(float(row["High"]), 2),
                    "low": round(float(row["Low"]), 2),
                    "close": round(float(row["Close"]), 2),
                    "volume": int(row["Volume"]) if row["Volume"] > 0 else 0,
                })
            out["history"] = rows
        except Exception as exc:
            out["errors"]["ibov"] = str(exc)

    out["ok"] = len(out["errors"]) < 4
    return out


def _get_shark_data() -> dict:
    with _shark_lock:
        now_ts = time.time()
        stale = (
            _shark_cache["data"] is None
            or (now_ts - _shark_cache["t"]) > TTL
        )
        if stale:
            fresh = _build_shark()
            if fresh["ok"] or _shark_cache["data"] is None:
                _shark_cache["t"] = now_ts
                _shark_cache["data"] = fresh
            else:
                cached = dict(_shark_cache["data"])
                cached["stale_fallback"] = True
                cached["last_refresh_errors"] = fresh["errors"]
                cached["cache_age_s"] = round(now_ts - _shark_cache["t"], 1)
                return cached

        payload = dict(_shark_cache["data"])
        payload["cache_age_s"] = round(time.time() - _shark_cache["t"], 1)
        return payload


@app.get("/api/shark")
def shark():
    return _get_shark_data()


@app.get("/api/win-flow")
def win_flow():
    return _get_shark_data()


# ---------------------------------------------------------------------
# Contexto macro - Banco Central / SGS
# ---------------------------------------------------------------------

async def _fetch_bcb_series(
    client: httpx.AsyncClient,
    code: str,
    retries: int = 2,
) -> dict:
    url = f"{BCB_BASE}.{code}/dados/ultimos/1?formato=json"
    last_exc = None

    for attempt in range(retries + 1):
        try:
            response = await client.get(url)
            response.raise_for_status()
            rows = response.json()
            break
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                await asyncio.sleep(0.8 * (attempt + 1))
            else:
                raise last_exc

    if not rows:
        return {"value": None, "date": None}

    row = rows[-1]
    raw_value = row.get("valor")

    try:
        value = float(str(raw_value).replace(",", "."))
    except (TypeError, ValueError):
        value = None

    raw_date = row.get("data")
    try:
        parsed_date = datetime.strptime(raw_date, "%d/%m/%Y").date().isoformat()
    except (TypeError, ValueError):
        parsed_date = raw_date

    return {
        "value": value,
        "date": parsed_date,
    }


async def _build_macro() -> dict:
    timeout = httpx.Timeout(15.0, connect=6.0)
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 ACPTelecomRadar/2.4",
    }

    async with httpx.AsyncClient(
        timeout=timeout,
        headers=headers,
    ) as client:
        keys = list(BCB_SERIES)
        results = await asyncio.gather(
            *[
                _fetch_bcb_series(client, BCB_SERIES[key]["code"])
                for key in keys
            ],
            return_exceptions=True,
        )

    series = {}
    errors = {}

    for key, result in zip(keys, results):
        meta = BCB_SERIES[key]

        if isinstance(result, Exception):
            errors[key] = str(result)
            series[key] = {
                "value": None,
                "date": None,
                "label": meta["label"],
                "unit": meta["unit"],
                "error": str(result),
            }
        else:
            series[key] = {
                **result,
                "label": meta["label"],
                "unit": meta["unit"],
            }

    return {
        "ok": len(errors) == 0,
        "as_of": datetime.now(SP).isoformat(timespec="seconds"),
        "series": series,
        "errors": errors,
        "meta": {
            "source": "Banco Central do Brasil — SGS",
            "cache_ttl_seconds": MACRO_TTL,
            "notes": [
                "A data exibida é a data da observação publicada pelo SGS.",
                "As séries possuem periodicidades e defasagens distintas.",
                "IBC-Br e séries de setor externo são estruturais/mensais, não fluxo intradiário.",
                "Dados macroeconômicos não representam agressão ou fluxo do WIN.",
            ],
        },
    }


async def _get_macro_data() -> dict:
    async with _macro_lock:
        stale = (
            _macro_cache["data"] is None
            or time.time() - _macro_cache["t"] > MACRO_TTL
        )

        if stale:
            try:
                data = await _build_macro()
                _macro_cache["t"] = time.time()
                _macro_cache["data"] = data
            except Exception as exc:
                if _macro_cache["data"] is not None:
                    return _macro_cache["data"]
                return {
                    "ok": False,
                    "as_of": datetime.now(SP).isoformat(timespec="seconds"),
                    "series": {},
                    "errors": {"macro": str(exc)},
                    "meta": {"source": "Banco Central do Brasil — SGS"},
                }

        return _macro_cache["data"]


@app.get("/api/macro")
async def macro():
    return await _get_macro_data()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "ACP Radar API",
        "version": "2.4.0",
        "time": datetime.now(SP).isoformat(timespec="seconds"),
    }
