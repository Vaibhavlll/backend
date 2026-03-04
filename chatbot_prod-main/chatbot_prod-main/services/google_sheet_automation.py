"""
google_sheet_automation.py

Handles Google Sheet → WhatsApp automation flows.

Key behaviors:
  - On publish: read all rows in range, send WhatsApp message to each phone number ONCE
  - On new rows (via scheduler): send message to newly added numbers only
  - Deduplication: tracks sent numbers per (org_id, flow_id) in google_sheet_sent_numbers collection
  - Variables: {{phone_number}}, {{row.ColumnName}}, {{customer_name}} supported in message text
"""

import re
import requests
from datetime import datetime, timezone
from typing import Optional

from database import get_mongo_db
from core.logger import get_logger

logger = get_logger(__name__)
db = get_mongo_db()

SENT_COLLECTION = "google_sheet_sent_numbers"


# =============================================================================
# PUBLIC API
# =============================================================================

async def process_google_sheet_trigger(
    org_id: str,
    flow_id: str,
    trigger_config: dict,
    message_text: str,
) -> dict:
    """
    Called on flow publish and by the scheduler when new rows are detected.

    Reads rows from the sheet, finds new phone numbers (not yet messaged for
    this flow), and sends WhatsApp messages.

    Args:
        org_id:         Organization ID
        flow_id:        Automation flow ID
        trigger_config: {sheet_id, sheet_name, column_name, row_start?, row_end?}
        message_text:   WhatsApp message body (may contain {{variables}})

    Returns:
        {"sent": int, "skipped": int, "errors": int}
    """
    sheet_id = trigger_config.get("sheet_id")
    column_name = trigger_config.get("column_name")
    row_start = trigger_config.get("row_start")  # 1-indexed, inclusive, optional
    row_end = trigger_config.get("row_end")       # 1-indexed, inclusive, optional

    if not sheet_id or not column_name:
        logger.error(f"[GoogleSheetAuto] Missing sheet_id or column_name for flow {flow_id}")
        return {"sent": 0, "skipped": 0, "errors": 1}

    # 1. Get Google access token
    try:
        from api.google_sheets_routes import refresh_google_token
        access_token = await refresh_google_token(org_id)
    except Exception as e:
        logger.error(f"[GoogleSheetAuto] Failed to get access token for org {org_id}: {e}")
        return {"sent": 0, "skipped": 0, "errors": 1}

    # 2. Fetch headers (row 1) to find the column index
    sheet_doc = db.google_sheets_connections.find_one({"org_id": org_id, "sheet_id": sheet_id})
    tab_name = sheet_doc.get("tab_name", "Sheet1") if sheet_doc else "Sheet1"

    headers = _fetch_headers(sheet_id, tab_name, access_token)
    if not headers:
        logger.error(f"[GoogleSheetAuto] Could not fetch headers for sheet {sheet_id}")
        return {"sent": 0, "skipped": 0, "errors": 1}

    # Find phone column index (0-based)
    try:
        col_index = headers.index(column_name)
    except ValueError:
        logger.error(f"[GoogleSheetAuto] Column '{column_name}' not found in sheet headers: {headers}")
        return {"sent": 0, "skipped": 0, "errors": 1}

    # 3. Fetch rows within range
    rows_with_data = _fetch_rows(sheet_id, tab_name, access_token, row_start, row_end)

    # 4. Get WhatsApp connection for this org
    wa_connection = _get_wa_connection(org_id)
    if not wa_connection:
        logger.error(f"[GoogleSheetAuto] No WhatsApp connection found for org {org_id}")
        return {"sent": 0, "skipped": 0, "errors": 1}

    # 5. Ensure deduplication index exists
    _ensure_sent_index()

    # 6. Process each row
    stats = {"sent": 0, "skipped": 0, "errors": 0}

    for row_values in rows_with_data:
        if col_index >= len(row_values):
            continue  # Row doesn't have a value for this column

        phone_raw = str(row_values[col_index]).strip()
        if not phone_raw:
            continue

        phone = _normalize_phone(phone_raw)
        if not phone:
            logger.warning(f"[GoogleSheetAuto] Invalid phone number '{phone_raw}' — skipping")
            stats["skipped"] += 1
            continue

        # Check deduplication
        if _already_sent(org_id, flow_id, phone):
            stats["skipped"] += 1
            continue

        # Build row context dict for variable replacement
        row_context = {
            headers[i]: row_values[i] if i < len(row_values) else ""
            for i in range(len(headers))
        }

        # Resolve variables in message text
        resolved_text = _resolve_variables(message_text, {
            "phone_number": phone,
            "customer_name": row_context.get("Name", row_context.get("name", phone)),
            "row": row_context,
        })

        # Send WhatsApp message
        success = await _send_whatsapp_message(
            org_id=org_id,
            phone=phone,
            message_text=resolved_text,
            wa_connection=wa_connection,
        )

        if success:
            _mark_sent(org_id, flow_id, phone, sheet_id)
            stats["sent"] += 1
            logger.success(f"[GoogleSheetAuto] Sent to {phone} for flow {flow_id}")
        else:
            stats["errors"] += 1
            logger.error(f"[GoogleSheetAuto] Failed to send to {phone} for flow {flow_id}")

    logger.info(
        f"[GoogleSheetAuto] Flow {flow_id}: sent={stats['sent']}, "
        f"skipped={stats['skipped']}, errors={stats['errors']}"
    )
    return stats


async def process_new_sheet_rows(org_id: str, sheet_id: str):
    """
    Called by the Google Sheets scheduler after new rows are synced.
    Finds all published google_sheet flows that reference this sheet
    and sends messages to any new phone numbers.
    """
    # Find active google_sheet triggers referencing this sheet
    triggers = list(db.automation_triggers.find({
        "org_id": org_id,
        "trigger_type": "google_sheet",
        "status": "active",
        "filters.sheet_id": sheet_id,
    }))

    if not triggers:
        return

    logger.info(f"[GoogleSheetAuto] {len(triggers)} active flow(s) to check for new rows in sheet {sheet_id}")

    for trigger in triggers:
        flow_id = trigger.get("flow_id")
        trigger_config = trigger.get("filters", {})
        message_text = trigger.get("message_text", "")

        if not message_text:
            # Fetch message text from flow nodes
            message_text = _get_flow_message_text(org_id, flow_id, trigger.get("start_node_id"))

        if not message_text:
            logger.warning(f"[GoogleSheetAuto] No message text found for flow {flow_id} — skipping")
            continue

        await process_google_sheet_trigger(
            org_id=org_id,
            flow_id=flow_id,
            trigger_config=trigger_config,
            message_text=message_text,
        )


def cancel_flow_sent_records(org_id: str, flow_id: str):
    """
    Remove sent number tracking when a flow is unpublished,
    so re-publishing starts fresh.
    """
    result = db[SENT_COLLECTION].delete_many({"org_id": org_id, "flow_id": flow_id})
    logger.info(f"[GoogleSheetAuto] Cleared {result.deleted_count} sent records for flow {flow_id}")


# =============================================================================
# INTERNAL HELPERS
# =============================================================================

def _fetch_headers(sheet_id: str, tab_name: str, access_token: str) -> list:
    """Fetch the first row (headers) from the Google Sheet."""
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{tab_name}!A1:ZZ1"
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=15)
    if resp.status_code != 200:
        return []
    values = resp.json().get("values", [[]])
    return values[0] if values else []


def _fetch_rows(
    sheet_id: str,
    tab_name: str,
    access_token: str,
    row_start: Optional[int],
    row_end: Optional[int],
) -> list:
    """
    Fetch data rows from the sheet (skipping header row 1).
    row_start/row_end are 1-indexed relative to data rows (row 2 = data row 1).
    """
    # Sheet row 2 is the first data row (row 1 is headers)
    sheet_start = (row_start + 1) if row_start else 2
    sheet_end = (row_end + 1) if row_end else 10001  # Max 10k rows

    url = (
        f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
        f"/values/{tab_name}!A{sheet_start}:ZZ{sheet_end}"
    )
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=15)
    if resp.status_code != 200:
        logger.error(f"[GoogleSheetAuto] Sheet API error fetching rows: {resp.text}")
        return []
    return resp.json().get("values", [])


def _get_wa_connection(org_id: str) -> Optional[dict]:
    """Get active WhatsApp connection for an org."""
    org = db.organizations.find_one({"org_id": org_id}, {"wa_id": 1})
    if not org or not org.get("wa_id"):
        return None
    wa_id = org["wa_id"]
    return db.whatsapp_connections.find_one({"wa_id": wa_id, "org_id": org_id})


def _normalize_phone(phone: str) -> Optional[str]:
    """
    Normalize phone number to E.164 format.
    Strips spaces, dashes, parentheses.
    Adds + prefix if missing (expects country code to already be present).
    Returns None if the number looks invalid.
    """
    # Remove common formatting
    cleaned = re.sub(r"[\s\-\(\)\.]", "", phone)

    # Already has + prefix
    if cleaned.startswith("+"):
        digits = cleaned[1:]
    elif cleaned.startswith("00"):
        digits = cleaned[2:]
    else:
        digits = cleaned

    # Basic validation: 7–15 digits
    if not digits.isdigit() or len(digits) < 7 or len(digits) > 15:
        return None

    return "+" + digits if not cleaned.startswith("+") else cleaned


def _already_sent(org_id: str, flow_id: str, phone: str) -> bool:
    """Check if a message has already been sent to this phone for this flow."""
    return db[SENT_COLLECTION].find_one(
        {"org_id": org_id, "flow_id": flow_id, "phone": phone}
    ) is not None


def _mark_sent(org_id: str, flow_id: str, phone: str, sheet_id: str):
    """Record that a message was sent to this phone number for this flow."""
    db[SENT_COLLECTION].update_one(
        {"org_id": org_id, "flow_id": flow_id, "phone": phone},
        {"$set": {
            "org_id": org_id,
            "flow_id": flow_id,
            "phone": phone,
            "sheet_id": sheet_id,
            "sent_at": datetime.now(timezone.utc),
        }},
        upsert=True,
    )


def _ensure_sent_index():
    """Create indexes for deduplication collection."""
    try:
        db[SENT_COLLECTION].create_index(
            [("org_id", 1), ("flow_id", 1), ("phone", 1)],
            unique=True,
            name="dedup_idx",
        )
    except Exception:
        pass  # Index already exists


async def _send_whatsapp_message(
    org_id: str,
    phone: str,
    message_text: str,
    wa_connection: dict,
) -> bool:
    """
    Send a WhatsApp message to a phone number via Meta API directly.

    Why we don't use send_whatsapp_message() wrapper:
    - That wrapper calls store_whatsapp_message() with status="sending" which returns
      None early, making mid=None, causing the success check to always fail even
      when the Meta API call succeeded.
    - We handle the API call + storage ourselves here for full control.
    """
    try:
        from services.wa_service import create_or_update_whatsapp_conversation, store_whatsapp_message
        from schemas.models import MessageRole
        from config.settings import IG_VERSION

        wa_id = wa_connection.get("wa_id")
        access_token = wa_connection.get("access_token")
        phone_id = wa_connection.get("phone_id")

        if not all([wa_id, access_token, phone_id]):
            logger.error(f"[GoogleSheetAuto] Incomplete WA connection for org {org_id}")
            return False

        # Step 1: Create/update conversation in DB
        conversation_id = await create_or_update_whatsapp_conversation(
            org_id=org_id,
            recipient_id=wa_id,
            customer_phone_no=phone,
            customer_name=phone,  # Will be updated when customer replies
            last_message=message_text,
            last_sender="agent",
            mode="reply",
        )

        # Step 2: Send via Meta WhatsApp Cloud API
        url = f"https://graph.facebook.com/{IG_VERSION}/{phone_id}/messages"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        payload = {
            "messaging_product": "whatsapp",
            "to": phone,
            "type": "text",
            "text": {"body": message_text},
        }

        resp = requests.post(url, headers=headers, json=payload, timeout=15)

        if resp.status_code != 200:
            logger.error(
                f"[GoogleSheetAuto] Meta API error for {phone}: "
                f"status={resp.status_code} body={resp.text}"
            )
            return False

        resp_data = resp.json()
        message_id = resp_data.get("messages", [{}])[0].get("id", "")

        # Step 3: Store message in DB (status="delivered" so store doesn't return early)
        await store_whatsapp_message(
            org_id=org_id,
            content=message_text,
            conversation_id=conversation_id,
            customer_phone_no=phone,
            type="text",
            payload=None,
            sender_name="Agent",
            recipient_id=wa_id,
            role=MessageRole.AGENT,
            mode="reply",
            message_id=message_id,
            raw_data=resp_data,
            status="delivered",
        )

        logger.success(f"[GoogleSheetAuto] ✅ Sent to {phone}, message_id={message_id}")
        return True

    except Exception as e:
        logger.error(f"[GoogleSheetAuto] WhatsApp send error to {phone}: {e}")
        return False


def _get_flow_message_text(org_id: str, flow_id: str, start_node_id: Optional[str]) -> str:
    """
    Walk flow nodes to find the first WhatsApp message text.
    """
    if not start_node_id:
        return ""

    try:
        collection = db[f"automation_flows_{org_id}"]
        flow = collection.find_one({"flow_id": flow_id}, {"flow_data": 1})
        if not flow:
            return ""

        nodes = flow.get("flow_data", {}).get("nodes", {})
        connections = flow.get("flow_data", {}).get("connections", [])

        # Walk from start_node_id to find message node
        visited = set()
        queue = [start_node_id]

        while queue:
            node_id = queue.pop(0)
            if node_id in visited:
                continue
            visited.add(node_id)

            node = nodes.get(node_id, {})
            if node.get("type") == "message":
                content = node.get("content", {})
                return content.get("text", "")

            # Follow connections
            next_ids = [c["target"] for c in connections if c.get("source") == node_id]
            queue.extend(next_ids)

    except Exception as e:
        logger.error(f"[GoogleSheetAuto] Error getting message text for flow {flow_id}: {e}")

    return ""


def _resolve_variables(text: str, variables: dict) -> str:
    """
    Replace {{variable}} and {{row.ColumnName}} placeholders.

    Supported:
      {{phone_number}}      → the phone number being messaged
      {{customer_name}}     → Name column value (or phone if not found)
      {{row.ColumnName}}    → any column value by name
    """
    if not text:
        return text

    def replacer(match):
        key = match.group(1).strip()
        parts = key.split(".", 1)

        if len(parts) == 2 and parts[0] == "row":
            # {{row.ColumnName}}
            row_data = variables.get("row", {})
            return str(row_data.get(parts[1], match.group(0)))

        val = variables.get(key)
        return str(val) if val is not None else match.group(0)

    return re.sub(r"\{\{([^}]+)\}\}", replacer, text)