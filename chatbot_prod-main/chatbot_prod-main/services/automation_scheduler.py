# chatbot_prod-main/chatbot_prod-main/services/automation_scheduler.py

"""
WhatsApp Follow-Up Scheduler

This module handles scheduled execution of WhatsApp follow-up automations.
It monitors published flows and executes them at the scheduled time.
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from pymongo import UpdateOne

from database import get_mongo_db
from core.logger import get_logger
from services.automation_execution import execute_automation_flow

logger = get_logger(__name__)
db = get_mongo_db()


async def schedule_whatsapp_followup(
    org_id: str,
    flow_id: str,
    conversation_id: str,
    delay_seconds: int
) -> str:
    """
    Schedule a WhatsApp follow-up message
    
    Args:
        org_id: Organization ID
        flow_id: Flow ID to execute
        conversation_id: WhatsApp conversation ID
        delay_seconds: Delay in seconds before sending
    
    Returns:
        schedule_id: Unique schedule ID
    """
    schedule_id = f"schedule_{flow_id}_{conversation_id}_{int(datetime.now(timezone.utc).timestamp())}"
    
    execute_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
    
    schedule_doc = {
        "schedule_id": schedule_id,
        "org_id": org_id,
        "flow_id": flow_id,
        "conversation_id": conversation_id,
        "trigger_type": "whatsapp_followup",
        "execute_at": execute_at,
        "status": "pending",
        "created_at": datetime.now(timezone.utc),
        "attempts": 0,
        "max_attempts": 3,
    }
    
    db.automation_schedules.insert_one(schedule_doc)
    
    logger.info(f"✅ Scheduled WhatsApp follow-up: {schedule_id} for {execute_at}")
    
    return schedule_id


async def process_scheduled_followups():
    """
    Background task to process scheduled follow-ups
    
    This should be run as a periodic task (e.g., every minute)
    """
    now = datetime.now(timezone.utc)
    
    # Find schedules that are due
    pending_schedules = db.automation_schedules.find({
        "status": "pending",
        "execute_at": {"$lte": now},
        "attempts": {"$lt": 3}
    })
    
    for schedule in pending_schedules:
        try:
            logger.info(f"🚀 Executing scheduled follow-up: {schedule['schedule_id']}")
            
            # Mark as running
            db.automation_schedules.update_one(
                {"schedule_id": schedule["schedule_id"]},
                {"$set": {"status": "running", "started_at": now}}
            )
            
            # Get conversation details
            conversations_collection = db[f"conversations_{schedule['org_id']}"]
            conversation = conversations_collection.find_one({
                "conversation_id": schedule["conversation_id"]
            })
            
            if not conversation:
                logger.error(f"❌ Conversation not found: {schedule['conversation_id']}")
                db.automation_schedules.update_one(
                    {"schedule_id": schedule["schedule_id"]},
                    {
                        "$set": {
                            "status": "failed",
                            "error": "Conversation not found",
                            "completed_at": now
                        }
                    }
                )
                continue
            
            # Build trigger data
            trigger_data = {
                "platform": "whatsapp",
                "platform_id": conversation.get("whatsapp_id"),
                "conversation_id": schedule["conversation_id"],
                "customer_id": conversation.get("customer_id"),
                "customer_name": conversation.get("customer_name"),
                "customer_username": conversation.get("customer_username"),
                "trigger_type": "whatsapp_followup",
                "scheduled_at": schedule["created_at"].isoformat(),
                "executed_at": now.isoformat(),
            }
            
            # Execute the flow
            result = await execute_automation_flow(
                org_id=schedule["org_id"],
                flow_id=schedule["flow_id"],
                trigger_type="whatsapp_followup",
                trigger_data=trigger_data
            )
            
            if result.get("status") == "success":
                logger.success(f"✅ Follow-up executed successfully: {schedule['schedule_id']}")
                db.automation_schedules.update_one(
                    {"schedule_id": schedule["schedule_id"]},
                    {
                        "$set": {
                            "status": "completed",
                            "execution_id": result.get("execution_id"),
                            "completed_at": now
                        }
                    }
                )
            else:
                raise Exception(result.get("error", {}).get("message", "Unknown error"))
                
        except Exception as e:
            logger.error(f"❌ Error executing scheduled follow-up: {str(e)}")
            
            # Increment attempt counter
            db.automation_schedules.update_one(
                {"schedule_id": schedule["schedule_id"]},
                {
                    "$inc": {"attempts": 1},
                    "$set": {
                        "status": "pending",  # Retry
                        "last_error": str(e),
                        "last_attempt_at": now
                    }
                }
            )
            
            # If max attempts reached, mark as failed
            if schedule.get("attempts", 0) + 1 >= 3:
                db.automation_schedules.update_one(
                    {"schedule_id": schedule["schedule_id"]},
                    {
                        "$set": {
                            "status": "failed",
                            "error": f"Max attempts reached: {str(e)}",
                            "completed_at": now
                        }
                    }
                )


async def cancel_scheduled_followup(schedule_id: str) -> bool:
    """
    Cancel a scheduled follow-up
    
    Args:
        schedule_id: Schedule ID to cancel
    
    Returns:
        True if cancelled, False otherwise
    """
    result = db.automation_schedules.update_one(
        {"schedule_id": schedule_id, "status": "pending"},
        {
            "$set": {
                "status": "cancelled",
                "cancelled_at": datetime.now(timezone.utc)
            }
        }
    )
    
    return result.modified_count > 0


async def get_scheduled_followups(org_id: str, flow_id: Optional[str] = None) -> List[Dict]:
    """
    Get all scheduled follow-ups for an organization
    
    Args:
        org_id: Organization ID
        flow_id: Optional flow ID to filter by
    
    Returns:
        List of scheduled follow-ups
    """
    query = {"org_id": org_id}
    if flow_id:
        query["flow_id"] = flow_id
    
    schedules = list(db.automation_schedules.find(query).sort("execute_at", 1))
    
    return schedules