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
) -> dict:
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

    # SGS costuma devolver a data como DD/MM/AAAA.
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
    timeout = httpx.Timeout(12.0, connect=5.0)

    async with httpx.AsyncClient(
        timeout=timeout,
        headers={"Accept": "application/json"},
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
                "IBC-Br é indicador de atividade econômica, não cotação intradiária.",
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
                # Atualiza o cache mesmo com falha parcial, para expor os erros.
                _macro_cache["t"] = time.time()
                _macro_cache["data"] = data
            except Exception as exc:
                if _macro_cache["data"] is None:
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
