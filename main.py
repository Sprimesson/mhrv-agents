import os
import re
import base64
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response, JSONResponse, HTMLResponse

app = FastAPI()
AUTH_KEY = os.environ["AUTH_KEY"]
UPSTREAM_AUTH_KEY = os.environ["UPSTREAM_AUTH_KEY"]
UPSTREAM_SERVER_URL = os.environ["UPSTREAM_SERVER_URL"]

SKIP_HEADERS = {"host", "connection", "content-length", "transfer-encoding", "priority", "proxy-connection", "proxy-authorization"}

DIAGNOSTIC_MODE = os.environ.get("DIAGNOSTIC_MODE", "false").lower() == "true"

NO_RESULT_HTML = (
    "<!DOCTYPE html><html><head><title>Web App</title></head>"
    "<body><p>The upstream completed but did not return anything.</p>"
    "</body></html>"
)

URL_RE = re.compile(r"^https?://", re.I)

app = FastAPI()


# ========================== Response helpers ==========================

def _json(obj: Dict[str, Any], status_code: int = 200) -> JSONResponse:
    return JSONResponse(content=obj, status_code=status_code)


def _html(html: str, status_code: int = 200) -> HTMLResponse:
    return HTMLResponse(content=html, status_code=status_code)


def _unauth_or_error(json_body: Dict[str, Any]) -> Response:
    if DIAGNOSTIC_MODE:
        return _json(json_body)
    return _html(NO_RESULT_HTML)


def _resp_headers(resp: httpx.Response) -> Dict[str, Any]:
    """
    Apps Script getAllHeaders() may preserve multi-value headers differently.
    httpx exposes repeated headers through resp.headers.multi_items().
    For compatibility, this returns:
      - string for normal single headers
      - list[str] for repeated headers
    """
    out: Dict[str, Any] = {}
    for key, value in resp.headers.multi_items():
        if key in out:
            if not isinstance(out[key], list):
                out[key] = [out[key]]
            out[key].append(value)
        else:
            out[key] = value
    return out


def _is_good_url(url: Any) -> bool:
    return isinstance(url, str) and bool(URL_RE.match(url))


# ========================== Request option builder ==========================

def _build_opts(req: Dict[str, Any]) -> Dict[str, Any]:
    method = str(req.get("m") or "GET").upper()

    headers: Dict[str, str] = {}
    raw_headers = req.get("h")

    if isinstance(raw_headers, dict):
        for k, v in raw_headers.items():
            if str(k).lower() not in SKIP_HEADERS:
                headers[str(k)] = str(v)

    content: Optional[bytes] = None

    if req.get("b"):
        content = base64.b64decode(req["b"])

    if req.get("ct"):
        headers["content-type"] = str(req["ct"])

    return {
        "method": method,
        "headers": headers,
        "content": content,
        "follow_redirects": req.get("r") is not False,
    }


# ========================== Core handlers ==========================

async def _do_single(req: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    if not _is_good_url(req.get("u")):
        return {"e": "bad url"}

    opts = _build_opts(req)

    resp = await client.request(
        opts["method"],
        req["u"],
        headers=opts["headers"],
        content=opts["content"],
        follow_redirects=opts["follow_redirects"],
    )

    return {
        "s": resp.status_code,
        "h": _resp_headers(resp),
        "b": base64.b64encode(resp.content).decode("ascii"),
    }


async def _do_batch(items: List[Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    Equivalent to Apps Script UrlFetchApp.fetchAll:
    - invalid URLs are returned in original position as {e: "bad url"}
    - valid requests are executed concurrently
    - output order matches input order
    """
    import asyncio

    results: List[Optional[Dict[str, Any]]] = [None] * len(items)
    tasks = []

    async def run_one(index: int, item: Dict[str, Any]) -> None:
        try:
            results[index] = await _do_single(item, client)
        except Exception as exc:
            results[index] = {"e": str(exc)}

    for i, item in enumerate(items):
        if not isinstance(item, dict) or not _is_good_url(item.get("u")):
            results[i] = {"e": "bad url"}
        else:
            tasks.append(run_one(i, item))

    if tasks:
        await asyncio.gather(*tasks)

    return {"q": [r if r is not None else {"e": "unknown error"} for r in results]}


async def _do_upstream(req: Dict[str, Any], client: httpx.AsyncClient) -> Response:
    if req.get("t") == "batch":
        return await _do_upstream_batch(req, client)

    payload: Dict[str, Any] = {"k": UPSTREAM_AUTH_KEY}

    t = req.get("t")

    if t == "connect":
        payload["op"] = "connect"
        payload["host"] = req.get("h")
        payload["port"] = req.get("p")

    elif t == "connect_data":
        payload["op"] = "connect_data"
        payload["host"] = req.get("h")
        payload["port"] = req.get("p")
        if req.get("d"):
            payload["data"] = req.get("d")

    elif t == "data":
        payload["op"] = "data"
        payload["sid"] = req.get("sid")
        if req.get("d"):
            payload["data"] = req.get("d")

    elif t == "close":
        payload["op"] = "close"
        payload["sid"] = req.get("sid")

    else:
        return _json({"e": f"unknown upstream op: {t}", "code": "UNSUPPORTED_OP"})

    resp = await client.post(
        f"{UPSTREAM_SERVER_URL}/tunnel",
        json=payload,
        follow_redirects=True,
    )

    if resp.status_code != 200:
        return _json({"e": f"upstream node HTTP {resp.status_code}"})

    return Response(
        content=resp.content,
        media_type="application/json",
        status_code=200,
    )


async def _do_upstream_batch(req: Dict[str, Any], client: httpx.AsyncClient) -> Response:
    payload = {
        "k": UPSTREAM_AUTH_KEY,
        "ops": req.get("ops") or [],
    }

    resp = await client.post(
        f"{UPSTREAM_SERVER_URL}/tunnel/batch",
        json=payload,
        follow_redirects=True,
    )

    if resp.status_code != 200:
        return _json({"e": f"upstream batch HTTP {resp.status_code}"})

    return Response(
        content=resp.content,
        media_type="application/json",
        status_code=200,
    )


# ========================== Cloud Run entry points ==========================

@app.post("/")
async def do_post(request: Request) -> Response:
    try:
        req = await request.json()

        if not isinstance(req, dict):
            return _unauth_or_error({"e": "request body must be JSON object"})

        if req.get("k") != AUTH_KEY:
            return _unauth_or_error({"e": "unauthorized"})

        timeout = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=60.0)

        async with httpx.AsyncClient(timeout=timeout, verify=True) as client:
            if req.get("t"):
                return await _do_upstream(req, client)

            if isinstance(req.get("q"), list):
                return _json(await _do_batch(req["q"], client))

            return _json(await _do_single(req, client))

    except Exception as exc:
        return _unauth_or_error({"e": str(exc)})


@app.get("/")
async def do_get() -> HTMLResponse:
    return _html(
        "<!DOCTYPE html><html><head><title>My App</title></head>"
        '<body style="font-family:sans-serif;max-width:600px;margin:40px auto">'
        "<h1>Welcome</h1><p>This application is running normally.</p>"
        "</body></html>"
    )