from fastapi import FastAPI
from contextlib import asynccontextmanager
from core.scheduler import start_scheduler
from services.ai_chatbot import load_agents, clear_agents, load_prompts_into_cache
from services.automation_scheduler import startup_scheduler, shutdown_scheduler

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event for the FastAPI application.
    """
    # Startup
    load_agents()
    load_prompts_into_cache()
    start_scheduler()
    await startup_scheduler()
    yield
    # Shutdown
    await shutdown_scheduler()
    clear_agents()
