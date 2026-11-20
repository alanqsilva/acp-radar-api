"""
ACPTelecom - Radar Financeiro
Backend FastAPI para Render.

Endpoints:
  GET /api/shark     -> S&P 500 futuro, VIX, histórico D-1 do Ibovespa e cotações B3 (brapi)
  GET /api/win-flow  -> alias de compatibilidade para /api/shark
  GET /api/macro     -> Selic, IPCA, PTAX venda, IBC-Br e setor externo (Banco Central/SGS)
  GET /health        -> saúde da aplicação

Observação:
  yfinance e brapi fornecem dados de mercado com possíveis atrasos e limitações.
  Séries mensais do BCB (setor externo/inflação/atividade) são contexto estrutural.
  Este servidor não fornece agressão real, book ou negócios do WIN/B3.
"""

import asyncio
import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import yfinance as yf
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------

TTL = int(os.getenv("CACHE_TTL", "60"))
MACRO_TTL = int(os.getenv("MACRO_CACHE_TTL", "900"))  # 15 minutos
BRAPI_TTL = int(os.getenv("BRAPI_CACHE_TTL", "90"))
BRAPI_TOKEN = os.getenv("BRAPI_TOKEN", "").strip()

ORIGINS = [
    origin.strip()
    for origin in os.getenv("ALLOWED_ORIGINS", "*").split(",")
    if origin.strip()
]
SP = ZoneInfo("America/Sao_Paulo")

app = FastAPI(title="ACP Radar API", version="2.1.0")
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

# Séries SGS:
# 4189  = Meta Selic (% a.a.)
# 433   = IPCA mensal (% a.m.)
# 1     = Taxa de câmbio - Dólar venda (PTAX)
# 24364 = IBC-Br dessazonalizado
# 22707 = Balança comercial - saldo mensal (US$ milhões)
# 22701 = Transações correntes - saldo mensal (US$ milhões)
BCB_SERIES = {
    "selic": {
        "code": "4189",
        "label": "Meta Selic",
        "unit": "% a.a.",
    },
    "ipca": {
        "code": "433",
        "label": "IPCA mensal",
        "unit": "% a.m.",
    },
    "usd_ptax": {
        "code": "1",
        "label": "Dólar PTAX venda",
        "unit": "R$/US$",
    },
    "ibc_br": {
        "code": "24364",
        "label": "IBC-Br dessazonalizado",
        "unit": "índice",
    },
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

B3_PESOS_TICKERS = [
    "VALE3",
    "PETR4",
    "ITUB4",
    "PETR3",
    "BBDC4",
    "B3SA3",
    "ELET3",
    "BBAS3",
    "MGLU3",
]
B3_FREE_FALLBACK_TICKERS = ["VALE3", "PETR4", "ITUB4", "MGLU3"]


# ---------------------------------------------------------------------
# Shark / mercado + brapi (pesos da B3)
# ---------------------------------------------------------------------

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
    """Último preço e variação % contra o fechamento anterior."""
    closes = _hist(symbol, period="10d")["Close"]
    last = float(closes.iloc[-1])
    previous = float(closes.iloc[-2])
    return last, (last / previous - 1) * 100


def _fetch_brapi_quotes(tickers: list[str], timeout_s: float = 10.0) -> dict:
    """Busca cotações na brapi.dev (usa Bearer token quando configurado)."""
    symbols = ",".join(tickers)
    url = f"https://brapi.dev/api/quote/{symbols}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 ACPTelecomRadar/2.1",
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
    """Retorna cotações B3 com cache e fallback seguro."""
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
    """Monta resposta; falha em um ativo não derruba os demais."""
    now = datetime.now(SP)
    out = {
        "ok": False,
        "errors": {},
        "updated": now.isoformat(timespec="seconds"),
        "source": "Yahoo Finance + BCB SGS + brapi",
    }

    try:
        last, pct = _last_and_pct("ES=F")
        out["sp_fut_price"] = round(last, 2)
        out["sp_fut_pct"] = round(pct, 2)
    except Exception as exc:
        out["errors"]["sp"] = str(exc)

    try:
        last, pct = _last_and_pct("^VIX")
        out["vix"] = round(last, 2)
        out["vix_pct"] = round(pct, 2)
    except Exception as exc:
        out["errors"]["vix"] = str(exc)

    try:
        df = _hist("^BVSP", period="20d")
        last_day = df.index[-1].date()

        in_session = (
            last_day == now.date()
            and (now.hour * 60 + now.minute) < 18 * 60 + 30
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

    b3_quotes, brapi_err = _get_b3_quotes()
    out["b3_quotes"] = b3_quotes
    if brapi_err:
        out["errors"]["brapi"] = brapi_err

    out["ok"] = len(out["errors"]) < 3
    return out


def _get_shark_data():
    with _shark_lock:
        stale = (
            _shark_cache["data"] is None
            or time.time() - _shark_cache["t"] > TTL
        )
        if stale:
            data = _build_shark()
            if data["ok"] or _shark_cache["data"] is None:
                _shark_cache["t"] = time.time()
                _shark_cache["data"] = data
        return _shark_cache["data"]


@app.get("/api/shark")
def shark():
    return _get_shark_data()


@app.get("/api/win-flow")
def win_flow():
    """Alias para dashboards que usam o caminho /api/win-flow."""
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
        "User-Agent": "Mozilla/5.0 ACPTelecomRadar/2.1",
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
                "IBC-Br e séries de setor externo (balança comercial / transações correntes) são estruturais/mensais, não fluxo intradiário.",
                "Dados macroeconômicos não representam agressão ou fluxo do WIN.",
            ],
        },
    }


async def _get_macro_data():
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


# ---------------------------------------------------------------------
# Saúde
# ---------------------------------------------------------------------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "ACP Radar API",
        "time": datetime.now(SP).isoformat(timespec="seconds"),
    }
