B3_YF_FALLBACK_MAP = {
    "VALE3": "VALE3.SA",
    "PETR4": "PETR4.SA",
    "ITUB4": "ITUB4.SA",
    "PETR3": "PETR3.SA",
    "BBDC4": "BBDC4.SA",
    "B3SA3": "B3SA3.SA",
    "ELET3": "ELET3.SA",
    "BBAS3": "BBAS3.SA",
    "WEGE3": "WEGE3.SA",
    "ABEV3": "ABEV3.SA",
}


def _fetch_b3_yfinance_fallback() -> dict:
    quotes = {}
    for sym, yf_sym in B3_YF_FALLBACK_MAP.items():
        try:
            price, pct = _last_and_pct_single(yf_sym, decimals=2)
            quotes[sym] = {
                "price": price,
                "change": pct,
                "name": sym,
                "source": "yfinance",
            }
        except Exception:
            continue
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
                    except Exception:
                        pass

                yf_quotes = _fetch_b3_yfinance_fallback()
                if yf_quotes:
                    _brapi_cache["data"] = yf_quotes
                    _brapi_cache["t"] = time.time()
                    return _brapi_cache["data"], f"brapi fallback ({exc})"

                if _brapi_cache["data"] is not None:
                    return _brapi_cache["data"], str(exc)
                return {}, str(exc)

        return _brapi_cache["data"] or {}, None
