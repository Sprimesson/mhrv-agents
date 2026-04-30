
import os
import re
import base64
import logging
import asyncio
from typing import Any, Dict, List, Optional

if os.name == "nt":
    try:
        os.system("chcp 65001")
    except:
        pass

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response, JSONResponse, HTMLResponse, StreamingResponse

app = FastAPI()

# ========================== Environment ==========================
# Legacy / mhrv protocol.
# New names are preferred; old names are kept as fallbacks so existing Cloud Run env vars still work.
MHRV_DOWNSTREAM_AUTH_KEY = os.getenv("MHRV_AUTH_KEY", "")
MHRV_UPSTREAM_AUTH_KEY = os.getenv("MHRV_UPSTREAM_AUTH_KEY", "")
MHRV_UPSTREAM_SERVER_URL = (os.getenv("MHRV_UPSTREAM_SERVER_URL", "")).rstrip("/")

# New / xh protocol.
XH_TARGET_BASE = (os.getenv("XH_TARGET_BASE", "")).rstrip("/")

if not XH_TARGET_BASE or not MHRV_UPSTREAM_SERVER_URL:
    raise Exception(f"XH_TARGET_BASE? {XH_TARGET_BASE}. MHRV_UPSTREAM_SERVER_URL? {MHRV_UPSTREAM_SERVER_URL}")

DIAGNOSTIC_MODE = os.environ.get("DIAGNOSTIC_MODE", "false").lower() == "true"

URL_RE = re.compile(r"^https?://", re.I)

NO_RESULT_HTML = (
    "<!DOCTYPE html><html><head><title>Web App</title></head>"
    "<body><p>The upstream completed but did not return anything.</p>"
    "</body></html>"
)

WELCOME_HTML = (
    "<!DOCTYPE html><html><head><title>My App</title></head>"
    '<body style="font-family:sans-serif;max-width:600px;margin:40px auto">'
    "<h1>Welcome</h1><p>This application is running normally.</p>"
    "</body></html>"
)

COMMON_TIMEOUT = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=60.0)

# Legacy mhrv skips.
MHRV_SKIP_HEADERS = {
    "host",
    "connection",
    "content-length",
    "transfer-encoding",
    "priority",
    "proxy-connection",
    "proxy-authorization",
}

# New xh relay skips, matching the Netlify worker behavior.
XH_STRIP_REQUEST_HEADERS = {
    "host",
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "forwarded",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-port",
}

XH_STRIP_RESPONSE_HEADERS = {
    "transfer-encoding",
}


# ========================== Shared response helpers ==========================

def _json(obj: Dict[str, Any], status_code: int = 200) -> JSONResponse:
    return JSONResponse(content=obj, status_code=status_code)


def _html(html: str, status_code: int = 200) -> HTMLResponse:
    return HTMLResponse(content=html, status_code=status_code)


def _text(text: str, status_code: int = 200) -> Response:
    return Response(content=text, status_code=status_code, media_type="text/plain")


def _unauth_or_error(json_body: Dict[str, Any]) -> Response:
    if DIAGNOSTIC_MODE:
        return _json(json_body)
    return _html(NO_RESULT_HTML)


def _is_good_url(url: Any) -> bool:
    return isinstance(url, str) and bool(URL_RE.match(url))


def _new_http_client(*, follow_redirects: bool = False) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=COMMON_TIMEOUT,
        verify=True,
        follow_redirects=follow_redirects,
    )


# ========================== Legacy mhrv helpers ==========================

def _mhrv_resp_headers(resp: httpx.Response) -> Dict[str, Any]:
    """
    Preserve repeated response headers as lists, matching the previous mhrv behavior.
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


def _mhrv_build_opts(req: Dict[str, Any]) -> Dict[str, Any]:
    method = str(req.get("m") or "GET").upper()

    headers: Dict[str, str] = {}
    raw_headers = req.get("h")

    if isinstance(raw_headers, dict):
        for k, v in raw_headers.items():
            if str(k).lower() not in MHRV_SKIP_HEADERS:
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


async def _mhrv_do_single(req: Dict[str, Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    if not _is_good_url(req.get("u")):
        return {"e": "bad url"}

    opts = _mhrv_build_opts(req)

    resp = await client.request(
        opts["method"],
        req["u"],
        headers=opts["headers"],
        content=opts["content"],
        follow_redirects=opts["follow_redirects"],
    )

    return {
        "s": resp.status_code,
        "h": _mhrv_resp_headers(resp),
        "b": base64.b64encode(resp.content).decode("ascii"),
    }


async def _mhrv_do_batch(items: List[Any], client: httpx.AsyncClient) -> Dict[str, Any]:
    """
    Equivalent to Apps Script UrlFetchApp.fetchAll:
    - invalid URLs return {e: "bad url"} in their original position
    - valid requests run concurrently
    - output order matches input order
    """
    

    results: List[Optional[Dict[str, Any]]] = [None] * len(items)
    tasks = []

    async def run_one(index: int, item: Dict[str, Any]) -> None:
        try:
            results[index] = await _mhrv_do_single(item, client)
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


async def _mhrv_do_upstream(req: Dict[str, Any], client: httpx.AsyncClient) -> Response:
    if not MHRV_UPSTREAM_AUTH_KEY or not MHRV_UPSTREAM_SERVER_URL:
        return _json({"e": "mhrv upstream is not configured"})

    if req.get("t") == "batch":
        return await _mhrv_do_upstream_batch(req, client)

    payload: Dict[str, Any] = {"k": MHRV_UPSTREAM_AUTH_KEY}
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
        f"{MHRV_UPSTREAM_SERVER_URL}/tunnel",
        json=payload,
        follow_redirects=True,
    )

    if resp.status_code != 200:
        return _json({"e": f"upstream node HTTP {resp.status_code}"})

    return Response(content=resp.content, media_type="application/json", status_code=200)


async def _mhrv_do_upstream_batch(req: Dict[str, Any], client: httpx.AsyncClient) -> Response:
    if not MHRV_UPSTREAM_AUTH_KEY or not MHRV_UPSTREAM_SERVER_URL:
        return _json({"e": "mhrv upstream is not configured"})

    payload = {
        "k": MHRV_UPSTREAM_AUTH_KEY,
        "ops": req.get("ops") or [],
    }

    resp = await client.post(
        f"{MHRV_UPSTREAM_SERVER_URL}/tunnel/batch",
        json=payload,
        follow_redirects=True,
    )

    if resp.status_code != 200:
        return _json({"e": f"upstream batch HTTP {resp.status_code}"})

    return Response(content=resp.content, media_type="application/json", status_code=200)


# ========================== New xh relay helpers ==========================

def _xh_target_url(request: Request, path: str) -> str:
    clean_path = f"/{path}" if path else "/"
    query = request.url.query
    return f"{XH_TARGET_BASE}{clean_path}{f'?{query}' if query else ''}"


def _xh_forward_headers(request: Request) -> Dict[str, str]:
    """
    Mirrors the Netlify relay:
    - strips hop-by-hop and platform headers
    - removes Netlify headers
    - preserves one client IP into x-forwarded-for
    """
    headers: Dict[str, str] = {}
    client_ip: Optional[str] = None

    for key, value in request.headers.items():
        k = key.lower()

        if k in XH_STRIP_REQUEST_HEADERS:
            continue

        if k.startswith("x-nf-") or k.startswith("x-netlify-"):
            continue

        if k == "x-real-ip":
            client_ip = value
            continue

        if k == "x-forwarded-for":
            if not client_ip:
                client_ip = value
            continue

        headers[k] = value

    if client_ip:
        headers["x-forwarded-for"] = client_ip

    return headers


def _xh_response_headers(upstream_headers: httpx.Headers) -> Dict[str, str]:
    response_headers: Dict[str, str] = {}

    for key, value in upstream_headers.items():
        if key.lower() in XH_STRIP_RESPONSE_HEADERS:
            continue
        response_headers[key] = value

    return response_headers


from fastapi.responses import StreamingResponse

idx_act = 0

async def _xh_relay(path: str, request: Request) -> Response:
    global idx_act
    idx_act = idx_act + 1
    this_act = idx_act

    if not XH_TARGET_BASE:
        return _text("Misconfigured: XH_TARGET_BASE is not set", 500)

    method = request.method
    has_body = method not in {"GET", "HEAD"}
    body = await request.body() if has_body else None

    client = _new_http_client(follow_redirects=False)
    req = client.build_request(
        method=method,
        url=_xh_target_url(request, path),
        headers=_xh_forward_headers(request),
        content=body,
    )

    try:
        print(f"<{this_act}> up sz{len(body) if body else 0} {req.url}")
        upstream = await client.send(req, stream=True)

        async def stream_body():
            try:
                async for chunk in upstream.aiter_raw():
                    print(f"<{this_act}> chunk sz{len(chunk)}")
                    yield chunk
            finally:
                print(f"<{this_act}> final")
                await upstream.aclose()
                await client.aclose()

        print(f"<{this_act}> resp {upstream.status_code}")
        return StreamingResponse(
            stream_body(),
            status_code=upstream.status_code,
            headers=_xh_response_headers(upstream.headers),
            media_type=upstream.headers.get("content-type"),
        )

    except Exception:
        await client.aclose()
        logging.exception("xh relay failed")
        return _text("Bad Gateway: Relay Failed", 502)


# ========================== Cloud Run entry points ==========================
# Legacy mhrv remains served at "/".

@app.post("/")
async def mhrv_post(request: Request) -> Response:
    try:
        if not MHRV_DOWNSTREAM_AUTH_KEY:
            return _unauth_or_error({"e": "mhrv downstream auth key is not configured"})

        req = await request.json()

        if not isinstance(req, dict):
            return _unauth_or_error({"e": "request body must be JSON object"})

        if req.get("k") != MHRV_DOWNSTREAM_AUTH_KEY:
            return _unauth_or_error({"e": "unauthorized"})

        async with _new_http_client() as client:
            if req.get("t"):
                return await _mhrv_do_upstream(req, client)

            if isinstance(req.get("q"), list):
                return _json(await _mhrv_do_batch(req["q"], client))

            return _json(await _mhrv_do_single(req, client))

    except Exception as exc:
        logging.exception("mhrv request failed")
        return _unauth_or_error({"e": str(exc)})


@app.get("/")
async def mhrv_get() -> HTMLResponse:
    return _html(WELCOME_HTML)


# New xh relay is served everywhere else.

@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
)
async def xh_catch_all(path: str, request: Request) -> Response:
    return await _xh_relay(path, request)
