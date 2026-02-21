"""
services/automation_scheduler.py
=================================
Production-ready WhatsApp Follow-Up Scheduler.

Architecture
------------
MongoDB is the single source of truth for all scheduled jobs.
APScheduler (AsyncIOScheduler) is the in-memory clock that fires callbacks.

On every server restart, `restore_pending_jobs()` is called from the app
lifespan and replays all pending jobs from MongoDB back into APScheduler,
so no jobs are lost across deployments.

Job lifecycle
-------------
  pending → running → completed
                    → failed
  pending → cancelled   (new message arrived / flow unpublished)

Collection
----------
  automation_scheduled_jobs
"""

import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

from database import get_mongo_db
from core.logger import get_logger

logger = get_logger(__name__)
db = get_mongo_db()

JOBS_COLLECTION = "automation_scheduled_jobs"


# ─────────────────────────────────────────────────────────────────────────────
# Index bootstrap (runs once at import time)
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_indexes():
    col = db[JOBS_COLLECTION]
    col.create_index("job_id", unique=True)
    col.create_index([("org_id", 1), ("conversation_id", 1), ("status", 1)])
    col.create_index([("org_id", 1), ("flow_id", 1), ("status", 1)])
    col.create_index("scheduled_at")
    col.create_index("status")


_ensure_indexes()


# ─────────────────────────────────────────────────────────────────────────────
# Flow-graph helpers
# ─────────────────────────────────────────────────────────────────────────────

def _next_node_ids(node_id: str, connections: List[Dict]) -> List[str]:
    """Return IDs of nodes directly downstream of node_id."""
    return [c["target"] for c in connections if c.get("source") == node_id]


def _resolve_action_start_node(
    trigger_start_node_id: str,
    nodes: Dict,
    connections: List[Dict],
) -> str:
    """
    Walk the graph to find where real execution should begin.

    Expected flow pattern:
        [whatsapp_followup trigger]  →  [smart_delay]  →  [action nodes ...]

    The scheduler already handles the delay, so we skip the smart_delay node
    and return the first action node after it.

    Falls back to trigger_start_node_id if the graph doesn't match.
    """
    children = _next_node_ids(trigger_start_node_id, connections)
    if not children:
        return trigger_start_node_id

    first_child_id = children[0]
    first_child = nodes.get(first_child_id, {})

    if first_child.get("type") == "smart_delay":
        # Skip the delay node
        after_delay = _next_node_ids(first_child_id, connections)
        return after_delay[0] if after_delay else first_child_id

    # No delay node → start from the first child directly
    return first_child_id


def _extract_delay_seconds(
    trigger_start_node_id: str,
    nodes: Dict,
    connections: List[Dict],
) -> int:
    """
    Read delay amount + unit from the smart_delay node connected to the
    trigger.  Returns 300 (5 minutes) when no delay node is found.
    """
    children = _next_node_ids(trigger_start_node_id, connections)
    if not children:
        return 300

    first_child = nodes.get(children[0], {})
    if first_child.get("type") != "smart_delay":
        return 300

    cfg = first_child.get("config", {})
    amount = int(cfg.get("amount", 5))
    unit = cfg.get("unit", "minutes")

    unit_to_seconds = {
        "seconds": 1,
        "minutes": 60,
        "hours": 3600,
        "days": 86400,
    }
    return amount * unit_to_seconds.get(unit, 60)


# ─────────────────────────────────────────────────────────────────────────────
# Public scheduling API
# ─────────────────────────────────────────────────────────────────────────────

async def schedule_whatsapp_followup(
    org_id: str,
    flow_id: str,
    conversation_id: str,
    flow_data: Optional[Dict] = None,
    delay_seconds: Optional[int] = None,
    action_start_node_id: Optional[str] = None,
) -> str:
    """
    Persist a follow-up job to MongoDB and register it with APScheduler.

    Parameters
    ----------
    org_id                : Organization ID
    flow_id               : Automation flow ID
    conversation_id       : Target conversation ID  (e.g. "whatsapp_+91...")
    flow_data             : Full flow_data dict — if provided, delay and
                            action_start_node_id are derived automatically
    delay_seconds         : Explicit delay override (used when flow_data absent)
    action_start_node_id  : Node to begin execution from when the job fires.
                            If None, execution starts at the flow's trigger
                            start_node (which re-walks the graph).

    Returns
    -------
    job_id : str
    """
    from services.automation_scheduler_worker import get_scheduler

    # ── Auto-derive delay / action node from flow graph ────────────────────
    if flow_data:
        nodes = flow_data.get("nodes", {})
        connections = flow_data.get("connections", [])
        triggers = flow_data.get("triggers", [])

        followup_trigger = next(
            (t for t in triggers if t.get("type") == "whatsapp_followup"),
            None,
        )

        if followup_trigger:
            t_start = followup_trigger.get("start_node_id")
            if t_start:
                if delay_seconds is None:
                    delay_seconds = _extract_delay_seconds(t_start, nodes, connections)
                if action_start_node_id is None:
                    action_start_node_id = _resolve_action_start_node(
                        t_start, nodes, connections
                    )

    if delay_seconds is None:
        delay_seconds = 300  # 5-minute default

    now = datetime.now(timezone.utc)
    scheduled_at = now + timedelta(seconds=delay_seconds)
    job_id = f"followup_{uuid.uuid4().hex}"

    # ── Persist to MongoDB ─────────────────────────────────────────────────
    job_doc = {
        "job_id": job_id,
        "org_id": org_id,
        "flow_id": flow_id,
        "conversation_id": conversation_id,
        "action_start_node_id": action_start_node_id,
        "delay_seconds": delay_seconds,
        "scheduled_at": scheduled_at,
        "status": "pending",
        "created_at": now,
        "executed_at": None,
        "cancelled_at": None,
        "cancel_reason": None,
        "retry_count": 0,
        "error": None,
    }

    db[JOBS_COLLECTION].insert_one(job_doc)
    logger.info(f"📝 Persisted job {job_id} | fires at {scheduled_at.isoformat()}")

    # ── Register with APScheduler ──────────────────────────────────────────
    scheduler = get_scheduler()
    if scheduler and scheduler.running:
        scheduler.add_job(
            _execute_scheduled_followup,
            trigger="date",
            run_date=scheduled_at,
            args=[job_id],
            id=job_id,
            misfire_grace_time=600,   # 10-min grace for misfires
            replace_existing=True,
            coalesce=True,
        )
        logger.success(
            f"✅ APScheduler job registered: {job_id} | "
            f"conv={conversation_id} | delay={delay_seconds}s"
        )
    else:
        logger.warning(
            "⚠️ APScheduler not running — job is in DB only. "
            "restore_pending_jobs() will register it on next startup."
        )

    return job_id


async def cancel_conversation_followups(
    org_id: str,
    conversation_id: str,
    reason: str = "new_message_received",
) -> int:
    """
    Cancel all pending follow-up jobs for a conversation.

    Called whenever a new message arrives so we don't send a follow-up
    to a customer who has already responded.

    Returns number of jobs cancelled.
    """
    from services.automation_scheduler_worker import get_scheduler

    now = datetime.now(timezone.utc)

    pending = list(
        db[JOBS_COLLECTION].find(
            {
                "org_id": org_id,
                "conversation_id": conversation_id,
                "status": "pending",
            },
            {"job_id": 1},
        )
    )

    if not pending:
        return 0

    job_ids = [j["job_id"] for j in pending]

    # Mark cancelled in DB (atomic)
    db[JOBS_COLLECTION].update_many(
        {"job_id": {"$in": job_ids}},
        {
            "$set": {
                "status": "cancelled",
                "cancelled_at": now,
                "cancel_reason": reason,
            }
        },
    )

    # Remove from APScheduler
    scheduler = get_scheduler()
    if scheduler and scheduler.running:
        for jid in job_ids:
            try:
                scheduler.remove_job(jid)
            except Exception:
                pass  # Already fired or not registered — safe

    count = len(job_ids)
    logger.info(
        f"🚫 Cancelled {count} follow-up job(s) | "
        f"conv={conversation_id} reason={reason}"
    )
    return count


async def cancel_flow_followups(
    org_id: str,
    flow_id: str,
    reason: str = "flow_unpublished",
) -> int:
    """
    Cancel all pending follow-up jobs for an entire automation flow.
    Called when a flow is unpublished/deleted.
    """
    from services.automation_scheduler_worker import get_scheduler

    now = datetime.now(timezone.utc)

    pending = list(
        db[JOBS_COLLECTION].find(
            {"org_id": org_id, "flow_id": flow_id, "status": "pending"},
            {"job_id": 1},
        )
    )

    if not pending:
        return 0

    job_ids = [j["job_id"] for j in pending]

    db[JOBS_COLLECTION].update_many(
        {"job_id": {"$in": job_ids}},
        {
            "$set": {
                "status": "cancelled",
                "cancelled_at": now,
                "cancel_reason": reason,
            }
        },
    )

    scheduler = get_scheduler()
    if scheduler and scheduler.running:
        for jid in job_ids:
            try:
                scheduler.remove_job(jid)
            except Exception:
                pass

    count = len(job_ids)
    logger.info(f"🚫 Cancelled {count} job(s) for flow={flow_id} reason={reason}")
    return count


async def restore_pending_jobs() -> int:
    """
    Re-register all pending DB jobs into APScheduler.

    Must be called from the application lifespan startup.
    Jobs overdue by > 10 minutes are marked cancelled to prevent
    sending stale follow-ups.

    Returns number of jobs successfully restored.
    """
    from services.automation_scheduler_worker import get_scheduler

    scheduler = get_scheduler()
    if not scheduler:
        logger.warning("restore_pending_jobs: scheduler unavailable")
        return 0

    now = datetime.now(timezone.utc)
    pending = list(db[JOBS_COLLECTION].find({"status": "pending"}))

    restored = 0
    for job in pending:
        job_id = job["job_id"]
        scheduled_at = job["scheduled_at"]

        # Ensure timezone-aware
        if scheduled_at.tzinfo is None:
            scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)

        overdue_secs = (now - scheduled_at).total_seconds()

        if overdue_secs > 600:
            # Too late — don't send a stale follow-up
            db[JOBS_COLLECTION].update_one(
                {"job_id": job_id},
                {
                    "$set": {
                        "status": "cancelled",
                        "cancelled_at": now,
                        "cancel_reason": "missed_overdue_on_restart",
                    }
                },
            )
            logger.warning(
                f"⏩ Dropped overdue job {job_id} ({overdue_secs:.0f}s late)"
            )
            continue

        # If slightly past due, fire ASAP (within grace window)
        run_date = scheduled_at if scheduled_at > now else now + timedelta(seconds=5)

        try:
            scheduler.add_job(
                _execute_scheduled_followup,
                trigger="date",
                run_date=run_date,
                args=[job_id],
                id=job_id,
                misfire_grace_time=600,
                replace_existing=True,
                coalesce=True,
            )
            restored += 1
        except Exception as e:
            logger.error(f"Failed to restore job {job_id}: {e}")

    logger.success(f"♻️ Restored {restored}/{len(pending)} pending follow-up jobs")
    return restored


# ─────────────────────────────────────────────────────────────────────────────
# APScheduler callback — executed when the timer fires
# ─────────────────────────────────────────────────────────────────────────────

async def _execute_scheduled_followup(job_id: str):
    """
    Called by APScheduler when a follow-up timer fires.

    Steps:
      1. Re-check job status (guard against race conditions)
      2. Build trigger_data from the live conversation
      3. Execute the automation flow starting from the action node
      4. Update job status in MongoDB
    """
    # Lazy import to avoid circular dependency at module load time
    from services.automation_execution import execute_automation_flow_from_node

    logger.info(f"🔔 Follow-up job fired: {job_id}")

    job = db[JOBS_COLLECTION].find_one({"job_id": job_id})

    if not job:
        logger.error(f"Job {job_id} not found — may have been deleted")
        return

    if job["status"] != "pending":
        logger.info(
            f"Job {job_id} already in status '{job['status']}' — skipping"
        )
        return

    org_id = job["org_id"]
    flow_id = job["flow_id"]
    conversation_id = job["conversation_id"]
    action_start_node_id = job.get("action_start_node_id")

    # ── Mark running ────────────────────────────────────────────────────────
    db[JOBS_COLLECTION].update_one(
        {"job_id": job_id, "status": "pending"},   # atomic: only if still pending
        {"$set": {"status": "running", "executed_at": datetime.now(timezone.utc)}},
    )

    # Re-fetch to confirm we won the race
    refreshed = db[JOBS_COLLECTION].find_one({"job_id": job_id})
    if refreshed and refreshed["status"] != "running":
        logger.info(f"Job {job_id} was taken by another process, skipping")
        return

    try:
        # ── Build trigger_data from the live conversation ───────────────────
        trigger_data = await _build_trigger_data(org_id, conversation_id)

        if not trigger_data:
            raise RuntimeError(
                f"Could not resolve trigger_data for conversation {conversation_id}. "
                "It may have been deleted."
            )

        # ── Execute the flow ────────────────────────────────────────────────
        result = await execute_automation_flow_from_node(
            org_id=org_id,
            flow_id=flow_id,
            trigger_type="whatsapp_followup",
            trigger_data=trigger_data,
            start_node_id=action_start_node_id,
        )

        # ── Mark completed ──────────────────────────────────────────────────
        db[JOBS_COLLECTION].update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "status": "completed",
                    "result": {
                        "execution_id": result.get("execution_id"),
                        "execution_status": result.get("status"),
                    },
                }
            },
        )
        logger.success(f"✅ Follow-up job {job_id} completed")

    except Exception as exc:
        error_msg = str(exc)
        logger.error(f"❌ Follow-up job {job_id} failed: {error_msg}", exc_info=True)

        db[JOBS_COLLECTION].update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "status": "failed",
                    "error": {
                        "message": error_msg,
                        "timestamp": datetime.now(timezone.utc),
                    },
                }
            },
        )


# ─────────────────────────────────────────────────────────────────────────────
# Trigger data builder
# ─────────────────────────────────────────────────────────────────────────────

async def _build_trigger_data(
    org_id: str, conversation_id: str
) -> Optional[Dict]:
    """
    Construct the trigger_data dict that FlowExecutionContext expects,
    sourced from the live conversation document.
    """
    conv = db[f"conversations_{org_id}"].find_one(
        {"conversation_id": conversation_id}, {"_id": 0}
    )

    if not conv:
        return None

    # Get the org's WhatsApp Business Account ID for sending
    org_doc = db.organizations.find_one({"org_id": org_id}, {"wa_id": 1})
    wa_id = (org_doc or {}).get("wa_id") or conv.get("whatsapp_id")

    if not wa_id:
        logger.warning(f"⚠️ No wa_id found for org={org_id}")

    return {
        "platform": "whatsapp",
        "platform_id": wa_id,
        "conversation_id": conversation_id,
        "customer_id": conv.get("customer_id"),
        "customer_name": conv.get("customer_name", ""),
        "last_message": conv.get("last_message", ""),
        "trigger_type": "whatsapp_followup",
        "is_followup": True,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Admin / introspection helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_pending_jobs(org_id: str) -> List[Dict]:
    """Return all pending jobs for an org (used by admin API)."""
    jobs = list(
        db[JOBS_COLLECTION]
        .find({"org_id": org_id, "status": "pending"}, {"_id": 0})
        .sort("scheduled_at", 1)
    )
    for job in jobs:
        for f in ("scheduled_at", "created_at", "executed_at", "cancelled_at"):
            if job.get(f) and isinstance(job[f], datetime):
                job[f] = job[f].isoformat()
    return jobs


def get_job_history(org_id: str, limit: int = 100) -> List[Dict]:
    """Return recent job history for an org."""
    jobs = list(
        db[JOBS_COLLECTION]
        .find({"org_id": org_id}, {"_id": 0})
        .sort("created_at", -1)
        .limit(limit)
    )
    for job in jobs:
        for f in ("scheduled_at", "created_at", "executed_at", "cancelled_at"):
            if job.get(f) and isinstance(job[f], datetime):
                job[f] = job[f].isoformat()
    return jobs