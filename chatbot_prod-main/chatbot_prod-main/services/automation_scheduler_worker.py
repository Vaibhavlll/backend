"""
automation_scheduler_worker.py
==============================
Background worker that polls MongoDB for due WhatsApp follow-up jobs and
executes them.

Architecture
------------
Implements a polling-based scheduler backed entirely by MongoDB.
No Redis, Celery, or external queue required.

Startup (in main.py):
    from services.automation_scheduler_worker import startup_scheduler, shutdown_scheduler

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await startup_scheduler()
        yield
        await shutdown_scheduler()
"""

import asyncio
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional

from database import get_mongo_db
from services.automation_scheduler import (
    _ensure_indexes,
    claim_due_followups,
    mark_followup_completed,
    mark_followup_failed,
)
from core.logger import get_logger

logger = get_logger(__name__)
db = get_mongo_db()

POLL_INTERVAL_SECONDS: int = 30
BATCH_SIZE: int = 10
MAX_EXECUTION_TIME_SECONDS: int = 60

_worker_task: Optional[asyncio.Task] = None


async def startup_scheduler() -> None:
    global _worker_task
    logger.info("🚀 Starting WhatsApp Follow-Up Scheduler Worker …")
    _ensure_indexes()
    _recover_stuck_jobs()
    _worker_task = asyncio.create_task(_polling_loop(), name="whatsapp_followup_scheduler")
    logger.success("✅ Scheduler Worker started")


async def shutdown_scheduler() -> None:
    global _worker_task
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
    logger.info("🛑 Scheduler Worker stopped")


async def _polling_loop() -> None:
    logger.info(f"⏱ Polling loop active (interval={POLL_INTERVAL_SECONDS}s, batch={BATCH_SIZE})")
    while True:
        try:
            await _poll_and_execute()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(f"❌ Polling error: {exc}\n{traceback.format_exc()}")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def _poll_and_execute() -> None:
    jobs = claim_due_followups(batch_size=BATCH_SIZE)
    if not jobs:
        logger.debug("⏳ No due follow-ups this cycle")
        return
    logger.info(f"📬 Found {len(jobs)} due follow-up(s)")
    await asyncio.gather(
        *[_execute_with_timeout(job) for job in jobs],
        return_exceptions=True,
    )


async def _execute_with_timeout(job: dict) -> None:
    schedule_id = job.get("schedule_id", "unknown")
    try:
        async with asyncio.timeout(MAX_EXECUTION_TIME_SECONDS):
            await _execute_followup(job)
    except asyncio.TimeoutError:
        error_msg = f"Timed out after {MAX_EXECUTION_TIME_SECONDS}s"
        logger.error(f"⏰ [{schedule_id}] {error_msg}")
        _handle_failure(job, error_msg, retry=False)


async def _execute_followup(job: dict) -> None:
    from services.wa_service import send_whatsapp_message

    schedule_id     = job["schedule_id"]
    org_id          = job["org_id"]
    flow_id         = job["flow_id"]
    conversation_id = job["conversation_id"]
    message_config  = job.get("message_config", {})
    retry_count     = job.get("retry_count", 0)
    max_retries     = job.get("max_retries", 3)

    logger.info(f"▶️  [{schedule_id}] conv={conversation_id} retry={retry_count}")

    try:
        # 1. Validate conversation
        conversation = _get_conversation(org_id, conversation_id)
        if not conversation:
            logger.warning(f"⚠️  [{schedule_id}] Conversation not found – aborting")
            mark_followup_failed(schedule_id, "Conversation not found", retry=False)
            return

        if conversation.get("status") == "closed":
            logger.info(f"ℹ️  [{schedule_id}] Conversation closed – skipping")
            mark_followup_completed(schedule_id)
            return

        # 2. Get WhatsApp sender ID
        whatsapp_sender_id = _get_whatsapp_sender_id(org_id)
        if not whatsapp_sender_id:
            raise RuntimeError(f"No active WhatsApp connection for org {org_id}")

        # 3. Resolve message text
        message_text = _resolve_message_text(message_config, conversation)
        if not message_text:
            raise RuntimeError("Follow-up message text is empty")

        # 4. Send the message
        result = await send_whatsapp_message(
            message={
                "conversation_id": conversation_id,
                "content"        : message_text,
                "sender_id"      : whatsapp_sender_id,
            },
            org_id=org_id,
            mode="automation",
        )

        if not result or not result.get("message_id"):
            raise RuntimeError(f"No message_id in send response: {result}")

        # 5. Save execution log + mark complete
        _save_execution_log(
            org_id=org_id, flow_id=flow_id, schedule_id=schedule_id,
            conversation_id=conversation_id, message_text=message_text,
            message_id=result["message_id"], status="success",
        )
        mark_followup_completed(schedule_id)
        logger.success(f"✅ [{schedule_id}] Sent (msg_id={result['message_id']})")

    except Exception as exc:
        error_msg = str(exc)
        logger.error(f"❌ [{schedule_id}] Failed (retry {retry_count}): {error_msg}")
        can_retry = retry_count < max_retries
        _handle_failure(job, error_msg, retry=can_retry)
        _save_execution_log(
            org_id=org_id, flow_id=flow_id, schedule_id=schedule_id,
            conversation_id=conversation_id, message_text=message_config.get("text", ""),
            message_id=None, status="failed", error=error_msg,
        )


def _get_conversation(org_id: str, conversation_id: str) -> Optional[dict]:
    return db[f"conversations_{org_id}"].find_one({"conversation_id": conversation_id}, {"_id": 0})


def _get_whatsapp_sender_id(org_id: str) -> Optional[str]:
    org = db.organizations.find_one({"org_id": org_id}, {"wa_id": 1})
    return org.get("wa_id") if org else None


def _resolve_message_text(message_config: dict, conversation: dict) -> str:
    text = message_config.get("text", "").strip()
    customer_name = conversation.get("customer_name", "there")
    return text.replace("{{customer_name}}", customer_name)


def _handle_failure(job: dict, error: str, retry: bool = True) -> None:
    schedule_id = job["schedule_id"]
    retry_count = job.get("retry_count", 0)
    max_retries = job.get("max_retries", 3)
    can_retry = retry and retry_count < max_retries
    mark_followup_failed(schedule_id, error, retry=can_retry)
    if can_retry:
        logger.info(f"🔁 [{schedule_id}] Retry {retry_count + 1}/{max_retries} in 5 min")
    else:
        logger.error(f"💀 [{schedule_id}] Permanently failed")


def _recover_stuck_jobs() -> None:
    """On startup, reset jobs stuck in 'executing' from a prior crash."""
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=10)
    result = db["automation_scheduled_followups"].update_many(
        {"status": "executing", "executed_at": {"$lt": cutoff}},
        {"$set": {"status": "pending"}, "$inc": {"retry_count": 1}},
    )
    if result.modified_count:
        logger.warning(f"🔧 Recovered {result.modified_count} stuck job(s) → pending")


def _save_execution_log(
    org_id: str, flow_id: str, schedule_id: str, conversation_id: str,
    message_text: str, message_id: Optional[str], status: str, error: Optional[str] = None,
) -> None:
    try:
        now = datetime.now(timezone.utc)
        db["automation_executions"].insert_one({
            "execution_id"   : f"followup_{schedule_id}",
            "flow_id"        : flow_id,
            "org_id"         : org_id,
            "trigger_type"   : "whatsapp_followup",
            "status"         : status,
            "conversation_id": conversation_id,
            "message_text"   : (message_text or "")[:200],
            "message_id"     : message_id,
            "schedule_id"    : schedule_id,
            "error"          : {"message": error} if error else None,
            "created_at"     : now,
            "completed_at"   : now,
        })
    except Exception as exc:
        logger.error(f"⚠️  Could not save execution log for {schedule_id}: {exc}")