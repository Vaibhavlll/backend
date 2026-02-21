from fastapi import FastAPI
from contextlib import asynccontextmanager
from core.scheduler import start_scheduler
from services.ai_chatbot import load_agents, clear_agents, load_prompts_into_cache
from core.logger import get_logger

logger = get_logger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event for the FastAPI application.
    """
    # Startup
    load_agents()
    load_prompts_into_cache()
    start_scheduler()

    # Start WhatsApp follow-up scheduler (non-fatal if it fails)
    try:
        from services.automation_scheduler_worker import startup_scheduler
        await startup_scheduler()
    except Exception as exc:
        logger.error(f"⚠️ Could not start WhatsApp follow-up scheduler: {exc}")

    yield

    # Shutdown
    clear_agents()

    try:
        from services.automation_scheduler_worker import shutdown_scheduler
        await shutdown_scheduler()
    except Exception as exc:
        logger.error(f"⚠️ Error stopping WhatsApp follow-up scheduler: {exc}")
