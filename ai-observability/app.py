import os
import re
import time
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

app = FastAPI(title="Payment AI Observability", version="1.0.0")

LOKI_URL = os.getenv("LOKI_URL", "http://localhost:3100")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2:1b")

RECONCILE_CACHE_TTL_SECONDS = 15
PAYMENT_TRACE_CACHE_TTL_SECONDS = 15
_reconcile_cache: dict[int, tuple[float, dict[str, Any]]] = {}
_payment_trace_cache: dict[tuple[str, int, bool], tuple[float, dict[str, Any]]] = {}
_reconcile_lock = asyncio.Lock()
_payment_trace_lock = asyncio.Lock()

SERVICES = [
    "payment-initiation-service",
    "payment-router",
    "instant-payment-handler",
]

PAYMENT_ID_RE = re.compile(r"paymentId=([^\s|]+)")
STATUS_RE = re.compile(r"status=([A-Z]{4}|FAILED|RECEIVED|RGTC|SCHD)")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
PAYMENT_TYPE_RE = re.compile(r"paymentType=([^|]+)")


def now_ns() -> int:
    return int(time.time() * 1_000_000_000)


def seconds_ago_ns(seconds: int) -> int:
    return now_ns() - (seconds * 1_000_000_000)


def hours_ago_ns(hours: int) -> int:
    t = datetime.now(timezone.utc) - timedelta(hours=hours)
    return int(t.timestamp() * 1_000_000_000)


async def query_loki(query: str, start_ns: int, end_ns: int, limit: int = 500) -> list[dict[str, Any]]:
    params = {
        "query": query,
        "start": str(start_ns),
        "end": str(end_ns),
        "limit": str(limit),
        "direction": "forward",
    }
    retryable_statuses = {429, 502, 503, 504}
    backoff_seconds = (0.2, 0.5, 1.0)
    resp = None
    for attempt, backoff in enumerate(backoff_seconds, start=1):
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(f"{LOKI_URL}/loki/api/v1/query_range", params=params)
        if resp.status_code == 200:
            break
        if resp.status_code not in retryable_statuses or attempt == len(backoff_seconds):
            raise HTTPException(status_code=502, detail=f"Loki query failed: {resp.text}")
        await asyncio.sleep(backoff)

    if resp is None or resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Loki query failed: no response")

    data = resp.json()
    results = data.get("data", {}).get("result", [])
    events: list[dict[str, Any]] = []
    for stream in results:
        labels = stream.get("stream", {})
        for ts, line in stream.get("values", []):
            events.append(
                {
                    "ts_ns": int(ts),
                    "ts": datetime.fromtimestamp(int(ts) / 1_000_000_000, tz=timezone.utc).isoformat(),
                    "service": labels.get("service", "unknown"),
                    "line": line,
                }
            )
    events.sort(key=lambda x: x["ts_ns"])
    return events


def extract_payment_id(line: str) -> str | None:
    cleaned = ANSI_RE.sub("", line)
    matches = PAYMENT_ID_RE.findall(cleaned)
    if not matches:
        return None
    # Router logs can contain two paymentId fields: MDC messageId first, business paymentId later.
    # The last match is the business paymentId used across services.
    return matches[-1]


def extract_status(line: str) -> str | None:
    cleaned = ANSI_RE.sub("", line)
    m = STATUS_RE.search(cleaned)
    return m.group(1) if m else None


def extract_payment_type(line: str) -> str | None:
    cleaned = ANSI_RE.sub("", line)
    m = PAYMENT_TYPE_RE.search(cleaned)
    if not m:
        return None
    return m.group(1).strip()


def classify_stage(line: str) -> str:
    lower = ANSI_RE.sub("", line).lower()
    if "validation failed" in lower or "rejection" in lower or "rejected" in lower:
        return "rejected"
    if "pain 002 status updated" in lower:
        return "status-updated"
    if "broker ack" in lower:
        return "kafka-ack"
    if "publishing" in lower and "pain 001" in lower:
        return "pain001-published"
    if "publishing" in lower and "pain 002" in lower:
        return "pain002-published"
    if "payment routed successfully" in lower:
        return "routed"
    if "processing instant payment" in lower:
        return "engine-processing"
    return "event"


def format_timeline(events: list[dict[str, Any]], max_events: int = 120) -> str:
    clipped = events[-max_events:]
    return "\n".join(
        [
            f"{e['ts']} | {e['service']} | {classify_stage(e['line'])} | {ANSI_RE.sub('', e['line'])[:400]}"
            for e in clipped
        ]
    )


def build_rule_based_summary(events: list[dict[str, Any]]) -> str:
    if not events:
        return "No matching log events found in the selected window."

    latest = events[-1]
    latest_status = None
    for event in reversed(events):
        s = extract_status(event["line"])
        if s:
            latest_status = s
            break

    stage_counts: dict[str, int] = defaultdict(int)
    for event in events:
        stage_counts[classify_stage(event["line"])] += 1

    top_stages = sorted(stage_counts.items(), key=lambda x: x[1], reverse=True)[:3]
    stage_summary = ", ".join([f"{name}={count}" for name, count in top_stages])

    last_events = events[-3:]
    evidence = " | ".join(
        [f"{e['service']}: {ANSI_RE.sub('', e['line'])[:140]}" for e in last_events]
    )

    return (
        f"Current Service: {latest['service']}. "
        f"Latest Status: {latest_status or 'unknown'}. "
        f"Event Count: {len(events)}. "
        f"Top Stages: {stage_summary or 'n/a'}. "
        f"Recent Evidence: {evidence}"
    )


def latest_payment_type(events: list[dict[str, Any]]) -> str | None:
    for event in reversed(events):
        payment_type = extract_payment_type(event["line"])
        if payment_type:
            return payment_type
    return None


async def build_payment_trace_result(payment_id: str, hours: int, use_model: bool = False) -> dict[str, Any]:
    start_ns = hours_ago_ns(hours)
    end_ns = now_ns()

    all_events: list[dict[str, Any]] = []
    for service in SERVICES:
        query = f'{{service="{service}"}} |= "paymentId={payment_id}"'
        events = await query_loki(query, start_ns, end_ns, limit=250)
        all_events.extend(events)

    all_events.sort(key=lambda x: x["ts_ns"])

    latest_status = None
    for event in reversed(all_events):
        status = extract_status(event["line"])
        if status:
            latest_status = status
            break

    if use_model:
        ai_summary = await summarize_with_ollama(payment_id, all_events)
        if ai_summary.startswith("AI summary unavailable"):
            ai_summary = f"{build_rule_based_summary(all_events)} | note: model fallback used."
    else:
        ai_summary = build_rule_based_summary(all_events)

    return {
        "paymentId": payment_id,
        "timeWindowHours": hours,
        "eventCount": len(all_events),
        "latestStatus": latest_status,
        "currentService": all_events[-1]["service"] if all_events else None,
        "paymentType": latest_payment_type(all_events),
        "aiSummary": ai_summary,
        "events": all_events[-60:],
    }


async def get_cached_payment_trace(payment_id: str, hours: int, use_model: bool = False) -> dict[str, Any]:
    cache_key = (payment_id, hours, use_model)
    now = time.monotonic()
    cached = _payment_trace_cache.get(cache_key)
    if cached and now - cached[0] < PAYMENT_TRACE_CACHE_TTL_SECONDS:
        return cached[1]

    async with _payment_trace_lock:
        cached = _payment_trace_cache.get(cache_key)
        now = time.monotonic()
        if cached and now - cached[0] < PAYMENT_TRACE_CACHE_TTL_SECONDS:
            return cached[1]

        result = await build_payment_trace_result(payment_id, hours, use_model)
        _payment_trace_cache[cache_key] = (time.monotonic(), result)
        return result


async def build_reconcile_result(hours: int) -> dict[str, Any]:
    start_ns = hours_ago_ns(hours)
    end_ns = now_ns()

    init_ack_events = await query_loki(
        '{service="payment-initiation-service"} |= "Kafka broker ACK confirmed"',
        start_ns,
        end_ns,
        limit=2000,
    )
    init_status_events = await query_loki(
        '{service="payment-initiation-service"} |= "PAIN 002 status updated"',
        start_ns,
        end_ns,
        limit=2000,
    )
    router_routed_events = await query_loki(
        '{service="payment-router"} |= "Payment routed successfully"',
        start_ns,
        end_ns,
        limit=2000,
    )
    router_rejected_events = await query_loki(
        '{service="payment-router"} |= "Payment rejected"',
        start_ns,
        end_ns,
        limit=2000,
    )
    handler_published_events = await query_loki(
        '{service="instant-payment-handler"} |= "PAIN 002 published"',
        start_ns,
        end_ns,
        limit=2000,
    )

    init_published = set()
    router_processed = set()
    router_processed_instant = set()
    router_processed_non_instant = set()
    handler_published = set()
    init_status_updated = set()

    for event in init_ack_events:
        payment_id = extract_payment_id(event["line"])
        if not payment_id:
            continue
        init_published.add(payment_id)

    for event in init_status_events:
        payment_id = extract_payment_id(event["line"])
        if not payment_id:
            continue
        init_status_updated.add(payment_id)

    for event in router_routed_events:
        payment_id = extract_payment_id(event["line"])
        if not payment_id:
            continue
        router_processed.add(payment_id)
        payment_type = (extract_payment_type(event["line"]) or "").lower()
        if "instant" in payment_type:
            router_processed_instant.add(payment_id)
        else:
            router_processed_non_instant.add(payment_id)

    for event in router_rejected_events:
        payment_id = extract_payment_id(event["line"])
        if not payment_id:
            continue
        router_processed.add(payment_id)

    for event in handler_published_events:
        payment_id = extract_payment_id(event["line"])
        if not payment_id:
            continue
        handler_published.add(payment_id)

    missing_in_router = sorted(init_published - router_processed)
    missing_in_handler = sorted(router_processed_instant - handler_published)
    missing_status_update = sorted(handler_published - init_status_updated)

    return {
        "timeWindowHours": hours,
        "counts": {
            "initiationPublished": len(init_published),
            "routerProcessed": len(router_processed),
            "routerProcessedInstant": len(router_processed_instant),
            "routerProcessedNonInstant": len(router_processed_non_instant),
            "handlerPublished": len(handler_published),
            "initiationStatusUpdated": len(init_status_updated),
            "missingInRouter": len(missing_in_router),
            "missingInHandler": len(missing_in_handler),
            "missingStatusUpdate": len(missing_status_update),
        },
        "samples": {
            "missingInRouter": missing_in_router[:20],
            "missingInHandler": missing_in_handler[:20],
            "missingStatusUpdate": missing_status_update[:20],
        },
    }


async def get_cached_reconcile(hours: int) -> dict[str, Any]:
    now = time.monotonic()
    cached = _reconcile_cache.get(hours)
    if cached and now - cached[0] < RECONCILE_CACHE_TTL_SECONDS:
        return cached[1]

    async with _reconcile_lock:
        cached = _reconcile_cache.get(hours)
        now = time.monotonic()
        if cached and now - cached[0] < RECONCILE_CACHE_TTL_SECONDS:
            return cached[1]

        result = await build_reconcile_result(hours)
        _reconcile_cache[hours] = (time.monotonic(), result)
        return result


async def ingestion_lag_hint(payment_id: str | None) -> str | None:
    if not payment_id:
        return None

    try:
        recent_activity = await query_loki(
            '{job="payment-services"} |= "paymentId="',
            seconds_ago_ns(120),
            now_ns(),
            limit=80,
        )
    except Exception:
        return "note: unable to verify log ingestion health at the moment."

    if recent_activity:
        return (
            "note: this payment may be very recent and log ingestion can lag by 15-60s; "
            "retry shortly."
        )

    return "note: no recent payment logs are being ingested; check Promtail/Loki status."


async def summarize_with_ollama(payment_id: str, events: list[dict[str, Any]]) -> str:
    if not events:
        return f"No logs found for paymentId={payment_id}."

    timeline = "\n".join(
        [f"{e['ts']} | {e['service']} | {classify_stage(e['line'])} | {e['line'][:400]}" for e in events[-120:]]
    )
    if len(timeline) > 12000:
        timeline = timeline[-12000:]

    prompt = (
        "You are a payment observability assistant. "
        "Given the event timeline, answer strictly with:\n"
        "1) Current Stage\n"
        "2) Current Service\n"
        "3) Final/Latest Status Code\n"
        "4) Failure Reason (if failed)\n"
        "5) Reconciliation Notes\n"
        "Keep it concise and evidence-based.\n\n"
        f"PaymentId: {payment_id}\n"
        f"Timeline:\n{timeline}"
    )

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }

    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            resp = await client.post(f"{OLLAMA_URL}/api/generate", json=payload)
        if resp.status_code != 200:
            return (
                "AI summary unavailable (model/service error). "
                "Fallback: timeline is returned in events; inspect latest status and service fields."
            )
        data = resp.json()
        return data.get("response", "No response from model")
    except Exception as exc:
        return (
            f"AI summary unavailable ({type(exc).__name__}: {str(exc)[:200]}). "
            "Fallback: timeline is returned in events; inspect latest status and service fields."
        )


async def ask_ollama(question: str, context: str) -> str:
    # Keep prompt size bounded to avoid model timeouts on very large timelines.
    if len(context) > 12000:
        context = context[-12000:]

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": (
            "You are a payment observability assistant. Use only the provided log context. "
            "If evidence is missing, explicitly say so. Keep answer concise and actionable.\n\n"
            f"Question:\n{question}\n\n"
            f"Log Context:\n{context}"
        ),
        "stream": False,
    }

    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            resp = await client.post(f"{OLLAMA_URL}/api/generate", json=payload)
        if resp.status_code != 200:
            return f"AI answer unavailable (model/service error): {resp.text[:400]}"
        data = resp.json()
        return data.get("response", "No response from model")
    except Exception as exc:
        return f"AI answer unavailable ({type(exc).__name__}: {str(exc)[:200]})."


class AiAskRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=2000)
    paymentId: str | None = Field(default=None, min_length=3)
    hours: int = Field(default=24, ge=1, le=168)
    services: list[str] | None = None
    maxEvents: int = Field(default=120, ge=10, le=500)
    includeEvents: bool = False
    useModel: bool = False


class AiLogqlRequest(BaseModel):
    query: str = Field(..., min_length=3, max_length=2000)
    question: str = Field(..., min_length=3, max_length=2000)
    hours: int = Field(default=24, ge=1, le=168)
    limit: int = Field(default=300, ge=10, le=2000)
    maxEvents: int = Field(default=120, ge=10, le=500)
    includeEvents: bool = False
    useModel: bool = False


@app.get("/ui", response_class=HTMLResponse)
async def ui() -> str:
        return """
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Payment AI Observability UI</title>
    <style>
        :root { color-scheme: light; }
        body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; margin: 24px; background: #f6f8fb; color: #1f2937; }
        h1 { margin-bottom: 8px; }
        .sub { margin-top: 0; color: #4b5563; }
        .grid { display: grid; grid-template-columns: 1fr; gap: 16px; max-width: 1100px; }
        .card { background: #fff; border: 1px solid #dbe3ef; border-radius: 10px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,.05); }
        .row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
        label { font-size: 13px; color: #374151; display: block; margin-bottom: 4px; }
        input, textarea, select { width: 100%; padding: 10px; border: 1px solid #cbd5e1; border-radius: 8px; font-size: 14px; box-sizing: border-box; }
        textarea { min-height: 90px; resize: vertical; }
        button { background: #0f766e; color: #fff; border: 0; border-radius: 8px; padding: 10px 14px; cursor: pointer; font-weight: 600; }
        button:hover { background: #0b5f59; }
        pre { background: #0b1020; color: #d1e0ff; border-radius: 8px; padding: 12px; overflow: auto; max-height: 360px; }
        .meta { font-size: 12px; color: #6b7280; }
        @media (max-width: 900px) { .row { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <h1>Payment AI Observability</h1>
    <p class="sub">Ask AI about a payment trace or any LogQL query.</p>

    <div class="grid">
        <div class="card">
            <h3>Ask by Payment</h3>
            <div class="row">
                <div>
                    <label>Payment ID</label>
                    <input id="payPaymentId" placeholder="PAY-LOAD-..." />
                </div>
                <div>
                    <label>Hours</label>
                    <input id="payHours" type="number" value="24" min="1" max="168" />
                </div>
            </div>
            <div class="row">
                <div>
                    <label>Max Events</label>
                    <input id="payMaxEvents" type="number" value="120" min="10" max="500" />
                </div>
                <div>
                    <label>Services (comma-separated, optional)</label>
                    <input id="payServices" placeholder="payment-initiation-service,payment-router,instant-payment-handler" />
                </div>
            </div>
            <div class="row">
                <div>
                    <label><input id="payUseModel" type="checkbox" style="width:auto; margin-right:8px;" /> Use Ollama model</label>
                </div>
                <div class="meta">Disabled by default for faster deterministic answers.</div>
            </div>
            <label>Question</label>
            <textarea id="payQuestion" placeholder="What is the current stage and any failure reason?"></textarea>
            <button onclick="askPayment()">Run /ai/ask</button>
            <p class="meta">POST /ai/ask</p>
            <pre id="payResult">Awaiting request...</pre>
        </div>

        <div class="card">
            <h3>Ask by LogQL</h3>
            <div class="row">
                <div>
                    <label>Hours</label>
                    <input id="logqlHours" type="number" value="24" min="1" max="168" />
                </div>
                <div>
                    <label>Limit</label>
                    <input id="logqlLimit" type="number" value="300" min="10" max="2000" />
                </div>
            </div>
            <div class="row">
                <div>
                    <label>Max Events</label>
                    <input id="logqlMaxEvents" type="number" value="120" min="10" max="500" />
                </div>
                <div>
                    <label><input id="logqlUseModel" type="checkbox" style="width:auto; margin-right:8px;" /> Use Ollama model</label>
                </div>
            </div>
            <label>LogQL Query</label>
            <textarea id="logqlQuery">{service=\"payment-router\"} |= \"Payment routed successfully\"</textarea>
            <label>Question</label>
            <textarea id="logqlQuestion" placeholder="Summarize anomalies and trend."></textarea>
            <button onclick="askLogql()">Run /ai/logql</button>
            <p class="meta">POST /ai/logql</p>
            <pre id="logqlResult">Awaiting request...</pre>
        </div>

        <div class="card">
            <h3>Reconciliation</h3>
            <p style="font-size:13px;color:#4b5563;margin-top:0;">Cross-service payment flow check — finds gaps between initiation, router, and handler.</p>
            <div class="row">
                <div>
                    <label>Hours</label>
                    <input id="reconcileHours" type="number" value="1" min="1" max="168" />
                </div>
                <div style="display:flex;align-items:flex-end;">
                    <button onclick="runReconcile()">Run /reconcile</button>
                </div>
            </div>
            <p class="meta">GET /reconcile?hours=N</p>
            <pre id="reconcileResult">Awaiting request...</pre>
        </div>
    </div>

    <script>
        async function postJson(url, payload, timeoutMs = 30000) {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), timeoutMs);
            try {
                const res = await fetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                    signal: controller.signal
                });
                const text = await res.text();
                try {
                    return { ok: res.ok, status: res.status, data: JSON.parse(text) };
                } catch {
                    return { ok: res.ok, status: res.status, data: text };
                }
            } catch (err) {
                return { ok: false, status: 0, data: String(err) };
            } finally {
                clearTimeout(timer);
            }
        }

        function splitServices(v) {
            return v.split(',').map(s => s.trim()).filter(Boolean);
        }

        async function askPayment() {
            const resultEl = document.getElementById('payResult');
            resultEl.textContent = 'Running...';

            try {
                const payload = {
                    question: document.getElementById('payQuestion').value || 'What is the latest status?',
                    paymentId: document.getElementById('payPaymentId').value || null,
                    hours: Number(document.getElementById('payHours').value || 24),
                    maxEvents: Number(document.getElementById('payMaxEvents').value || 120),
                    includeEvents: false,
                    useModel: document.getElementById('payUseModel').checked
                };

                const services = splitServices(document.getElementById('payServices').value || '');
                if (services.length) payload.services = services;

                const out = await postJson('/ai/ask', payload);
                resultEl.textContent = JSON.stringify(out, null, 2);
            } catch (err) {
                resultEl.textContent = JSON.stringify({ ok: false, status: 0, data: String(err) }, null, 2);
            }
        }

        async function runReconcile() {
            const resultEl = document.getElementById('reconcileResult');
            resultEl.textContent = 'Running...';
            try {
                const hours = Number(document.getElementById('reconcileHours').value || 1);
                const res = await fetch(`/reconcile?hours=${hours}`, { signal: AbortSignal.timeout(30000) });
                const data = await res.json();
                resultEl.textContent = JSON.stringify(data, null, 2);
            } catch (err) {
                resultEl.textContent = String(err);
            }
        }

        async function askLogql() {
            const resultEl = document.getElementById('logqlResult');
            resultEl.textContent = 'Running...';

            try {
                const payload = {
                    query: document.getElementById('logqlQuery').value,
                    question: document.getElementById('logqlQuestion').value || 'Summarize key findings.',
                    hours: Number(document.getElementById('logqlHours').value || 24),
                    limit: Number(document.getElementById('logqlLimit').value || 300),
                    maxEvents: Number(document.getElementById('logqlMaxEvents').value || 120),
                    includeEvents: false,
                    useModel: document.getElementById('logqlUseModel').checked
                };

                const out = await postJson('/ai/logql', payload);
                resultEl.textContent = JSON.stringify(out, null, 2);
            } catch (err) {
                resultEl.textContent = JSON.stringify({ ok: false, status: 0, data: String(err) }, null, 2);
            }
        }
    </script>
</body>
</html>
"""


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "UP", "service": "payment-ai-observability"}


@app.get("/payment/trace")
async def payment_trace(
    paymentId: str = Query(..., min_length=3),
    hours: int = Query(24, ge=1, le=168),
    useModel: bool = Query(False),
) -> dict[str, Any]:
    return await get_cached_payment_trace(paymentId, hours, useModel)


@app.get("/grafana/payment-trace/summary")
async def grafana_payment_trace_summary(
    paymentId: str = Query(..., min_length=3),
    hours: int = Query(24, ge=1, le=168),
) -> list[dict[str, Any]]:
    trace = await get_cached_payment_trace(paymentId, hours, False)
    return [
        {
            "paymentId": trace["paymentId"],
            "timeWindowHours": trace["timeWindowHours"],
            "eventCount": trace["eventCount"],
            "latestStatus": trace["latestStatus"],
            "currentService": trace["currentService"],
            "paymentType": trace["paymentType"],
            "aiSummary": trace["aiSummary"],
        }
    ]


@app.post("/ai/ask")
async def ai_ask(req: AiAskRequest) -> dict[str, Any]:
    start_ns = hours_ago_ns(req.hours)
    end_ns = now_ns()

    requested_services = req.services or SERVICES
    allowed = [s for s in requested_services if s in SERVICES]
    if not allowed:
        raise HTTPException(status_code=400, detail=f"No valid services provided. Allowed: {SERVICES}")

    all_events: list[dict[str, Any]] = []
    if req.paymentId:
        for service in allowed:
            query = f'{{service="{service}"}} |= "paymentId={req.paymentId}"'
            events = await query_loki(query, start_ns, end_ns, limit=250)
            all_events.extend(events)
    else:
        for service in allowed:
            query = f'{{service="{service}"}} |= "paymentId="'
            events = await query_loki(query, start_ns, end_ns, limit=250)
            all_events.extend(events)

    all_events.sort(key=lambda x: x["ts_ns"])
    context = format_timeline(all_events, max_events=req.maxEvents)
    fallback_answer = build_rule_based_summary(all_events)
    if not all_events and req.paymentId:
        hint = await ingestion_lag_hint(req.paymentId)
        if hint:
            fallback_answer = f"{fallback_answer} | {hint}"

    if req.useModel:
        try:
            answer = await asyncio.wait_for(
                ask_ollama(req.question, context if context else "No matching log events found."),
                timeout=20,
            )
        except asyncio.TimeoutError:
            answer = f"{fallback_answer} | note: model timed out after 20s; fallback used."

        if answer.startswith("AI answer unavailable"):
            answer = f"{fallback_answer} | note: model unavailable; fallback used."
    else:
        answer = fallback_answer

    latest_status = None
    for event in reversed(all_events):
        s = extract_status(event["line"])
        if s:
            latest_status = s
            break

    return {
        "question": req.question,
        "paymentId": req.paymentId,
        "services": allowed,
        "timeWindowHours": req.hours,
        "eventCount": len(all_events),
        "latestStatus": latest_status,
        "currentService": all_events[-1]["service"] if all_events else None,
        "answer": answer,
        "events": all_events[-60:] if req.includeEvents else [],
    }


@app.post("/ai/logql")
async def ai_logql(req: AiLogqlRequest) -> dict[str, Any]:
    start_ns = hours_ago_ns(req.hours)
    end_ns = now_ns()

    events = await query_loki(req.query, start_ns, end_ns, limit=req.limit)
    context = format_timeline(events, max_events=req.maxEvents)
    fallback_answer = build_rule_based_summary(events)
    if req.useModel:
        try:
            answer = await asyncio.wait_for(
                ask_ollama(req.question, context if context else "No matching log events found."),
                timeout=20,
            )
        except asyncio.TimeoutError:
            answer = f"{fallback_answer} | note: model timed out after 20s; fallback used."

        if answer.startswith("AI answer unavailable"):
            answer = f"{fallback_answer} | note: model unavailable; fallback used."
    else:
        answer = fallback_answer

    return {
        "question": req.question,
        "query": req.query,
        "timeWindowHours": req.hours,
        "eventCount": len(events),
        "answer": answer,
        "events": events[-60:] if req.includeEvents else [],
    }


@app.get("/component/metrics")
async def component_metrics(hours: int = Query(1, ge=1, le=168)) -> dict[str, Any]:
    start_ns = hours_ago_ns(hours)
    end_ns = now_ns()

    counters: dict[str, dict[str, int]] = {
        "payment-initiation-service": defaultdict(int),
        "payment-router": defaultdict(int),
        "instant-payment-handler": defaultdict(int),
    }

    for service in SERVICES:
        events = await query_loki(f'{{service="{service}"}} |= "paymentId="', start_ns, end_ns, limit=1000)
        for event in events:
            line = event["line"].lower()
            if service == "payment-initiation-service":
                if "post /api/v1/payments/initiate" in line:
                    counters[service]["requests_received"] += 1
                if "broker ack confirmed" in line:
                    counters[service]["pain001_ack"] += 1
                if "pain 002 status updated" in line:
                    counters[service]["pain002_updates"] += 1
            elif service == "payment-router":
                if "received pain 001" in line:
                    counters[service]["pain001_consumed"] += 1
                if "payment routed successfully" in line:
                    counters[service]["routed_success"] += 1
                if "payment rejected" in line:
                    counters[service]["rejected"] += 1
            elif service == "instant-payment-handler":
                if "received engine message" in line:
                    counters[service]["engine_consumed"] += 1
                if "pain 002 published" in line:
                    counters[service]["pain002_published"] += 1

    return {
        "timeWindowHours": hours,
        "components": counters,
    }


@app.get("/reconcile")
async def reconcile(hours: int = Query(1, ge=1, le=168)) -> dict[str, Any]:
    return await get_cached_reconcile(hours)


@app.get("/grafana/reconcile/summary")
async def grafana_reconcile_summary(hours: int = Query(1, ge=1, le=168)) -> list[dict[str, Any]]:
    data = await get_cached_reconcile(hours)
    row = dict(data["counts"])
    row["timeWindowHours"] = data["timeWindowHours"]
    return [row]


@app.get("/grafana/reconcile/samples")
async def grafana_reconcile_samples(
    kind: str = Query(..., pattern="^(missingInRouter|missingInHandler|missingStatusUpdate)$"),
    hours: int = Query(1, ge=1, le=168),
) -> list[dict[str, str]]:
    data = await get_cached_reconcile(hours)
    return [{"paymentId": payment_id} for payment_id in data["samples"].get(kind, [])]
