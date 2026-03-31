"""
GhostKitchen Portfolio Backend
================================
FastAPI: executes a real data pipeline, streams progress via SSE,
serves dashboard data from Railway PostgreSQL.
"""
import asyncio
import json
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

load_dotenv()

from db import get_pool, init_schema, get_sync_conn  # noqa: E402
from pipeline.generator import generate  # noqa: E402
from pipeline.bronze import load_bronze  # noqa: E402
from pipeline.silver import run_silver  # noqa: E402
from pipeline.gold import build_gold  # noqa: E402
from pipeline.quality import run_quality  # noqa: E402

# ── In-memory run state ───────────────────────────────────────────────────────
_run_events: dict = {}
_run_done: dict = {}
_run_lock = threading.Lock()

# ── Global pipeline semaphore: only 1 real run at a time ─────────────────────
_pipeline_semaphore = threading.Semaphore(1)
_active_run_id: str | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_schema()
    yield


app = FastAPI(title="GhostKitchen Portfolio API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Pipeline runner ───────────────────────────────────────────────────────────

def _emit(run_id: str, event: dict):
    with _run_lock:
        _run_events.setdefault(run_id, []).append(event)


def _run_pipeline(run_id: str):
    global _active_run_id
    conn = None
    acquired = _pipeline_semaphore.acquire(blocking=True, timeout=5)
    if not acquired:
        _emit(run_id, {
            "stage": "ERROR", "status": "error", "pct": 100,
            "error": "Another pipeline run is in progress. Please wait ~60s and try again.",
            "logs": ["Pipeline busy — only one concurrent run allowed to protect data integrity."],
        })
        with _run_lock:
            _run_done[run_id] = True
        return
    _active_run_id = run_id
    try:
        conn = get_sync_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pipeline_runs (run_id, started_at, status) VALUES (%s, NOW(), 'running')",
            (run_id,),
        )
        conn.commit()
        cur.close()

        t0 = time.time()

        # ── GENERATE ──────────────────────────────────────────────────────────
        _emit(run_id, {"stage": "GENERATE", "status": "running", "pct": 5, "logs": []})
        gen_logs = []

        def emit_gen(msg):
            gen_logs.append(msg)
            _emit(run_id, {"stage": "GENERATE", "status": "running", "pct": 12, "logs": [msg]})

        gen_t = time.time()
        data = generate(emit_gen)
        _emit(run_id, {
            "stage": "GENERATE", "status": "done", "pct": 20,
            "duration_s": round(time.time() - gen_t, 1),
            "metrics": {
                "raw_orders": data["counts"]["raw_orders"],
                "sensors": data["counts"]["sensors"],
                "gps_pings": data["counts"]["gps_pings"],
                "menu_cdc": len(data["menu_raw"]),
                "active_deliveries": data["counts"]["active_deliveries"],
            },
            "sample_json": data["samples"],
            "logs": gen_logs,
        })

        # ── BRONZE ────────────────────────────────────────────────────────────
        _emit(run_id, {"stage": "BRONZE", "status": "running", "pct": 25, "logs": []})
        bronze_logs = []

        def emit_bronze(msg):
            bronze_logs.append(msg)
            _emit(run_id, {"stage": "BRONZE", "status": "running", "pct": 35, "logs": [msg]})

        bronze_t = time.time()
        bronze_stats = load_bronze(data, conn, run_id, emit_bronze)
        _emit(run_id, {
            "stage": "BRONZE", "status": "done", "pct": 40,
            "duration_s": round(time.time() - bronze_t, 1),
            "metrics": bronze_stats,
            "logs": bronze_logs,
        })

        # ── SILVER ────────────────────────────────────────────────────────────
        _emit(run_id, {"stage": "SILVER", "status": "running", "pct": 45, "logs": []})
        silver_logs = []

        def emit_silver(msg):
            silver_logs.append(msg)
            _emit(run_id, {"stage": "SILVER", "status": "running", "pct": 60, "logs": [msg]})

        silver_t = time.time()
        silver_stats = run_silver(data, conn, run_id, emit_silver)
        _emit(run_id, {
            "stage": "SILVER", "status": "done", "pct": 65,
            "duration_s": round(time.time() - silver_t, 1),
            "metrics": {k: v for k, v in silver_stats.items() if isinstance(v, (int, float, str))},
            "sub_stages": [
                {
                    "name": "order_schema_alignment",
                    "in": data["counts"]["raw_orders"],
                    "out": silver_stats["orders_normalised"],
                    "note": f"{silver_stats['dupes_removed']} dupes removed",
                },
                {
                    "name": "data_vault_loader",
                    "in": silver_stats["orders_normalised"],
                    "out": silver_stats["hub_orders"],
                    "note": "hubs + satellites",
                },
                {
                    "name": "gps_validation",
                    "in": data["counts"]["gps_pings"],
                    "out": silver_stats["gps_validated"],
                    "note": "Texas bounds + speed anomaly",
                },
                {
                    "name": "sensor_anomaly_detection",
                    "in": data["counts"]["sensors"],
                    "out": silver_stats["sensor_anomalies"],
                    "note": "anomalies flagged",
                },
                {
                    "name": "identity_resolution",
                    "in": silver_stats["orders_normalised"],
                    "out": silver_stats["identity_exact"] + silver_stats["identity_fallback"],
                    "note": (
                        f"{silver_stats['identity_exact']} exact_email + "
                        f"{silver_stats['identity_fallback']} platform_fallback"
                    ),
                },
            ],
            "logs": silver_logs,
        })

        # ── GOLD ──────────────────────────────────────────────────────────────
        _emit(run_id, {"stage": "GOLD", "status": "running", "pct": 68, "logs": []})
        gold_logs = []

        def emit_gold(msg):
            gold_logs.append(msg)
            _emit(run_id, {"stage": "GOLD", "status": "running", "pct": 80, "logs": [msg]})

        gold_t = time.time()
        gold_stats = build_gold(silver_stats, conn, run_id, emit_gold)
        _emit(run_id, {
            "stage": "GOLD", "status": "done", "pct": 88,
            "duration_s": round(time.time() - gold_t, 1),
            "metrics": gold_stats,
            "logs": gold_logs,
        })

        # ── QUALITY ───────────────────────────────────────────────────────────
        _emit(run_id, {"stage": "QUALITY", "status": "running", "pct": 90, "logs": []})
        qual_logs = []

        def emit_quality(msg):
            qual_logs.append(msg)
            _emit(run_id, {"stage": "QUALITY", "status": "running", "pct": 95, "logs": [msg]})

        qual_t = time.time()
        checks = run_quality(conn, run_id, emit_quality)
        passed = sum(1 for c in checks if c["status"] == "pass")
        _emit(run_id, {
            "stage": "QUALITY", "status": "done", "pct": 99,
            "duration_s": round(time.time() - qual_t, 1),
            "checks": checks,
            "metrics": {
                "total": len(checks),
                "passed": passed,
                "warned": sum(1 for c in checks if c["status"] == "warn"),
                "failed": sum(1 for c in checks if c["status"] == "fail"),
            },
            "logs": qual_logs,
        })

        total_s = round(time.time() - t0, 1)
        _emit(run_id, {
            "stage": "DONE", "status": "done", "pct": 100,
            "duration_s": total_s,
            "healthy": passed >= max(len(checks) * 0.7, 1),
            "stats": {
                "raw_orders": data["counts"]["raw_orders"],
                "orders_normalised": silver_stats["orders_normalised"],
                "gps_pings": silver_stats["gps_validated"],
                "sensor_anomalies": silver_stats["sensor_anomalies"],
                "identity_resolved": silver_stats["identity_exact"],
                "ge_checks": len(checks),
                "ge_passed": passed,
                "total_gold_rows": sum(v for v in gold_stats.values() if isinstance(v, int)),
            },
        })

        cur = conn.cursor()
        cur.execute(
            "UPDATE pipeline_runs SET completed_at=NOW(), status='completed' WHERE run_id=%s",
            (run_id,),
        )
        conn.commit()
        cur.close()

    except Exception as exc:
        _emit(run_id, {
            "stage": "ERROR", "status": "error", "pct": 100,
            "error": str(exc),
            "logs": [f"Pipeline failed: {exc}"],
        })
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(
                    "UPDATE pipeline_runs SET status='failed' WHERE run_id=%s", (run_id,)
                )
                conn.commit()
            except Exception:
                pass
    finally:
        if conn:
            conn.close()
        with _run_lock:
            _run_done[run_id] = True
        _pipeline_semaphore.release()
        global _active_run_id
        _active_run_id = None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/pipeline/status")
async def pipeline_status():
    busy = _active_run_id is not None
    return {"busy": busy, "active_run_id": _active_run_id}


@app.post("/run")
async def trigger_run():
    run_id = str(uuid.uuid4())
    with _run_lock:
        _run_events[run_id] = []
        _run_done[run_id] = False
    t = threading.Thread(target=_run_pipeline, args=(run_id,), daemon=True)
    t.start()
    return {"run_id": run_id, "busy_check": "/pipeline/status"}


@app.get("/run/{run_id}/stream")
async def stream_run(run_id: str):
    async def generator() -> AsyncGenerator[str, None]:
        sent = 0
        while True:
            with _run_lock:
                events = list(_run_events.get(run_id, []))
                done = _run_done.get(run_id, False)
            while sent < len(events):
                yield f"data: {json.dumps(events[sent])}\n\n"
                sent += 1
            if done and sent >= len(events):
                yield 'data: {"stage":"STREAM_END"}\n\n'
                break
            await asyncio.sleep(0.15)

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/run/{run_id}/status")
async def run_status(run_id: str):
    with _run_lock:
        events = list(_run_events.get(run_id, []))
        done = _run_done.get(run_id, False)
    last = events[-1] if events else {}
    return {"run_id": run_id, "done": done, "event_count": len(events), "last_event": last}


@app.get("/health")
async def health():
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT MAX(completed_at) FROM pipeline_runs WHERE status='completed'"
            )
            last_run = row[0].isoformat() if row and row[0] else None
    except Exception:
        last_run = None
    return {"status": "ok", "last_run": last_run}


@app.get("/dashboard/kpis")
async def dashboard_kpis():
    pool = await get_pool()
    async with pool.acquire() as conn:
        revenue = await conn.fetchval("SELECT COALESCE(SUM(total_cents),0) FROM fact_order") or 0
        orders = await conn.fetchval("SELECT COUNT(*) FROM fact_order") or 0
        avg_del = await conn.fetchval(
            "SELECT COALESCE(AVG(duration_minutes),0) FROM fact_delivery_trip"
        ) or 0
        sla_pct = await conn.fetchval(
            "SELECT COALESCE(AVG(CASE WHEN sla_breach_flag THEN 1.0 ELSE 0.0 END),0) FROM fact_delivery_trip"
        ) or 0
        multi = await conn.fetchval("""
            SELECT COUNT(*) FROM (
                SELECT customer_key FROM fact_order
                GROUP BY customer_key HAVING COUNT(DISTINCT platform) >= 2
            ) x
        """) or 0
    return {
        "revenue_cents": int(revenue),
        "order_count": int(orders),
        "avg_delivery_min": round(float(avg_del), 1),
        "sla_breach_pct": round(float(sla_pct) * 100, 1),
        "multi_platform_customers": int(multi),
    }


@app.get("/dashboard/revenue-by-day")
async def revenue_by_day():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT d.full_date::text AS date,
                   SUM(fo.total_cents) AS revenue_cents,
                   COUNT(*) AS order_count,
                   AVG(fo.total_cents)::integer AS aov_cents
            FROM fact_order fo
            JOIN dim_date d ON fo.date_key = d.date_key
            GROUP BY d.full_date
            ORDER BY d.full_date DESC
            LIMIT 30
        """)
    return [dict(r) for r in rows]


@app.get("/dashboard/orders-by-platform")
async def orders_by_platform():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT platform,
                   COUNT(*) AS order_count,
                   SUM(total_cents) AS revenue_cents
            FROM fact_order
            GROUP BY platform
            ORDER BY order_count DESC
        """)
    return [dict(r) for r in rows]


@app.get("/dashboard/delivery-by-zone")
async def delivery_by_zone():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT dz.zone_id, dz.city, dz.zone_type,
                   ROUND(AVG(fdt.duration_minutes)::numeric, 1) AS avg_duration_min,
                   COUNT(*) AS trip_count,
                   SUM(CASE WHEN fdt.sla_breach_flag THEN 1 ELSE 0 END) AS sla_breaches,
                   ROUND(AVG(fdt.distance_km)::numeric, 2) AS avg_distance_km
            FROM fact_delivery_trip fdt
            JOIN dim_delivery_zone dz ON fdt.zone_key = dz.zone_key
            GROUP BY dz.zone_id, dz.city, dz.zone_type
            ORDER BY avg_duration_min DESC
            LIMIT 20
        """)
    return [dict(r) for r in rows]


@app.get("/dashboard/sensor-anomalies")
async def sensor_anomalies():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT dk.kitchen_id, dk.city, fsh.sensor_type,
                   SUM(fsh.anomaly_count) AS anomaly_count,
                   SUM(fsh.reading_count) AS reading_count,
                   ROUND(AVG(fsh.avg_value)::numeric, 2) AS avg_value
            FROM fact_sensor_hourly fsh
            JOIN dim_kitchen dk ON fsh.kitchen_key = dk.kitchen_key
            GROUP BY dk.kitchen_id, dk.city, fsh.sensor_type
            ORDER BY anomaly_count DESC
            LIMIT 25
        """)
    return [dict(r) for r in rows]


@app.get("/dashboard/top-customers")
async def top_customers():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT dc.customer_hk,
                   COUNT(fo.order_key) AS order_count,
                   SUM(fo.total_cents) AS ltv_cents,
                   COUNT(DISTINCT fo.platform) AS platform_count,
                   STRING_AGG(DISTINCT fo.platform, ', ') AS platforms
            FROM fact_order fo
            JOIN dim_customer dc ON fo.customer_key = dc.customer_key
            GROUP BY dc.customer_hk
            ORDER BY ltv_cents DESC
            LIMIT 10
        """)
    return [dict(r) for r in rows]


@app.get("/dashboard/kitchen-capacity")
async def kitchen_capacity():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT dk.kitchen_id, dk.city,
                   COUNT(fo.order_key) AS order_count,
                   dk.capacity_per_hour,
                   ROUND(
                       (COUNT(fo.order_key)::numeric /
                        NULLIF(dk.capacity_per_hour * 24, 0)) * 100, 1
                   ) AS utilization_pct
            FROM fact_order fo
            JOIN dim_kitchen dk ON fo.kitchen_key = dk.kitchen_key
            GROUP BY dk.kitchen_id, dk.city, dk.capacity_per_hour
            ORDER BY utilization_pct DESC
            LIMIT 20
        """)
    return [dict(r) for r in rows]
