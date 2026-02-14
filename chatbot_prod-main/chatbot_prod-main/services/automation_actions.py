# === Third-party Modules ===
from typing import Dict, Any

# === Internal Modules ===
from database import get_mongo_db
from services.ig_service import send_ig_message, send_ig_media_message
from services.wa_service import send_whatsapp_message
from core.logger import get_logger

logger = get_logger(__name__)
db = get_mongo_db()


# automation_actions.py - Line ~40

async def execute_action_node(context, node_id: str, node_type: str, config: Dict):
    """Execute an action node based on its type"""
    try:
        trigger_data = context.trigger_data
        platform = trigger_data.get("platform")
        
        # CRITICAL FIX: Check config.action_type for action nodes
        action_type = config.get("action_type")
        
        # If node_type is generic "action", use action_type from config
        if node_type == "action" and action_type:
            node_type = action_type
        
        # Now check the actual action type
        if node_type == "instagram_message":
            await execute_instagram_message(context, node_id, config, trigger_data)
            
        elif node_type == "whatsapp_message":
            await execute_whatsapp_message(context, node_id, config, trigger_data)
            
        elif node_type == "add_tag":
            await execute_add_tag(context, node_id, config, trigger_data)
            
        elif node_type == "remove_tag":
            await execute_remove_tag(context, node_id, config, trigger_data)
            
        elif node_type == "set_custom_field" or node_type == "set_field":
            await execute_set_custom_field(context, node_id, config, trigger_data)
            
        elif node_type == "http_request" or node_type == "api":
            await execute_http_request(context, node_id, config, trigger_data)
        
        # Instagram-specific actions
        elif node_type == "reply_to_comment":
            await execute_reply_to_comment(context, node_id, config, trigger_data)
            
        elif node_type == "send_dm":
            await execute_send_dm(context, node_id, config, trigger_data)
        
        elif node_type == "delay" or node_type == "smart_delay":
            await _execute_delay_node(context, node_id, config)
            
        else:
            context.log(node_id, node_type, "unknown", f"Unknown action type: {node_type}", success=False)
            
    except Exception as e:
        context.log(node_id, node_type, "error", f"Error executing action: {str(e)}", success=False)
        logger.error(f"Error executing action node {node_id}: {str(e)}", exc_info=True)

# ADD THESE NEW FUNCTIONS AT THE END OF THE FILE (after line 230)

async def execute_reply_to_comment(context, node_id: str, config: Dict, trigger_data: Dict):
    """Reply to an Instagram comment"""
    from services.ig_service import reply_to_comment
    
    comment_id = trigger_data.get("comment_id")
    ig_account_id = trigger_data.get("platform_id")
    
    if not comment_id or not ig_account_id:
        context.log(node_id, "reply_to_comment", "error", 
                   "Missing comment_id or ig_account_id", success=False)
        return
    
    reply_text = config.get("text", "")
    reply_text = replace_variables(reply_text, context.variables)
    
    context.log(node_id, "reply_to_comment", "executing", 
               f"Replying to comment {comment_id}")
    
    try:
        await reply_to_comment(comment_id, reply_text, ig_account_id)
        context.log(node_id, "reply_to_comment", "success", 
                   "Comment reply sent successfully", success=True)
        context.set_variable("reply_sent", True)
    except Exception as e:
        context.log(node_id, "reply_to_comment", "error", 
                   f"Failed to reply: {str(e)}", success=False)


async def execute_send_dm(context, node_id: str, config: Dict, trigger_data: Dict):
    """Send a DM to the user who triggered the automation"""
    from services.ig_service import send_private_reply
    
    comment_id = trigger_data.get("comment_id")
    ig_account_id = trigger_data.get("platform_id")
    
    if not comment_id or not ig_account_id:
        context.log(node_id, "send_dm", "error", 
                   "Missing comment_id or ig_account_id", success=False)
        return
    
    message_text = config.get("text", "")
    message_text = replace_variables(message_text, context.variables)
    
    link_url = config.get("link_url")
    button_title = config.get("button_title", "View Link")
    
    # Validate link_url if provided
    has_valid_link = (
        link_url and 
        isinstance(link_url, str) and 
        link_url.strip() and 
        (link_url.startswith('http://') or link_url.startswith('https://'))
    )
    
    context.log(node_id, "send_dm", "executing", 
               f"Sending DM via comment {comment_id}")
    
    try:
        result = await send_private_reply(
            comment_id=comment_id,
            ig_account_id=ig_account_id,
            message_text=message_text,
            link_url=link_url if has_valid_link else None,
            button_title=button_title if has_valid_link else None
        )
        
        if result:
            context.log(node_id, "send_dm", "success", 
                       "DM sent successfully", success=True)
            context.set_variable("dm_sent", True)
        else:
            context.log(node_id, "send_dm", "error", 
                       "Failed to send DM", success=False)
    except Exception as e:
        context.log(node_id, "send_dm", "error", 
                   f"Failed to send DM: {str(e)}", success=False)


async def _execute_delay_node(context, node_id: str, config: Dict):
    """Execute a delay node"""
    import asyncio
    
    amount = config.get("amount", 1)
    unit = config.get("unit", "seconds")
    
    # Convert to seconds
    if unit == "minutes":
        delay_seconds = amount * 60
    elif unit == "hours":
        delay_seconds = amount * 3600
    elif unit == "days":
        delay_seconds = amount * 86400
    else:
        delay_seconds = amount
    
    # Cap delay at 5 minutes for safety in production
    delay_seconds = min(delay_seconds, 300)
    
    context.log(node_id, "delay", "executing", 
               f"Delaying for {delay_seconds} seconds", success=True)
    
    await asyncio.sleep(delay_seconds)
    
    context.log(node_id, "delay", "success", "Delay completed", success=True)

    
async def execute_instagram_message(context, node_id: str, config: Dict, trigger_data: Dict):
    """Send an Instagram message"""
    message_type = config.get("messageType", "text")
    
    # Get recipient from trigger data
    customer_id = trigger_data.get("customer_id")
    instagram_id = trigger_data.get("platform_id")  # Instagram business account ID
    
    if not customer_id or not instagram_id:
        context.log(node_id, "Missing customer_id or instagram_id", level="error")
        return
    
    if message_type == "text":
        # Send text message
        text = config.get("text", "")
        
        # Replace variables in text
        text = replace_variables(text, context.variables)
        
        context.log(node_id, f"Sending Instagram text message to {customer_id}")
        
        try:
            result = await send_ig_message(
                id=customer_id,
                message_text=text,
                instagram_id=instagram_id,
                mode="reply",
                org_id=context.org_id
            )
            
            if result and result.get("message_id"):
                context.log(node_id, f"Instagram message sent successfully: {result.get('message_id')}")
                context.set_variable("last_message_id", result.get("message_id"))
            else:
                context.log(node_id, "Failed to send Instagram message", level="error")
                
        except Exception as e:
            context.log(node_id, f"Error sending Instagram message: {str(e)}", level="error")
    
    elif message_type == "media":
        # Send media message
        media_url = config.get("mediaUrl")
        caption = config.get("caption", "")
        
        if not media_url:
            context.log(node_id, "No media URL provided", level="error")
            return
        
        # Replace variables in caption
        caption = replace_variables(caption, context.variables)
        
        context.log(node_id, f"Sending Instagram media message to {customer_id}")
        
        try:
            result = await send_ig_media_message(
                recipient=customer_id,
                media_url=media_url,
                media_type="image",  # You might want to detect this from URL
                instagram_id=instagram_id,
                mode="reply",
                org_id=context.org_id
            )
            
            if result and result.get("message_id"):
                context.log(node_id, f"Instagram media message sent successfully")
            else:
                context.log(node_id, "Failed to send Instagram media message", level="error")
                
        except Exception as e:
            context.log(node_id, f"Error sending Instagram media message: {str(e)}", level="error")
    
    elif message_type == "buttons":
        # Send message with buttons (Instagram doesn't natively support buttons in the same way)
        # You might want to send text with instructions instead
        text = config.get("text", "")
        buttons = config.get("buttons", [])
        
        # Create button text
        button_text = "\n\n".join([f"• {btn.get('text')}" for btn in buttons if btn.get("text")])
        full_text = f"{text}\n\n{button_text}"
        
        # Replace variables
        full_text = replace_variables(full_text, context.variables)
        
        try:
            result = await send_ig_message(
                id=customer_id,
                message_text=full_text,
                instagram_id=instagram_id,
                mode="reply",
                org_id=context.org_id
            )
            
            if result and result.get("message_id"):
                context.log(node_id, f"Instagram button message sent successfully")
            else:
                context.log(node_id, "Failed to send Instagram button message", level="error")
                
        except Exception as e:
            context.log(node_id, f"Error sending Instagram button message: {str(e)}", level="error")


async def execute_whatsapp_message(context, node_id: str, config: Dict, trigger_data: Dict):
    """Send a WhatsApp message"""
    message_type = config.get("messageType", "text")
    
    # Get conversation ID from trigger data
    conversation_id = trigger_data.get("conversation_id")
    whatsapp_id = trigger_data.get("platform_id")
    
    if not conversation_id or not whatsapp_id:
        context.log(node_id, "Missing conversation_id or whatsapp_id", level="error")
        return
    
    if message_type == "text":
        # Send text message
        text = config.get("text", "")
        
        # Replace variables in text
        text = replace_variables(text, context.variables)
        
        context.log(node_id, f"Sending WhatsApp text message to {conversation_id}")
        
        try:
            result = await send_whatsapp_message(
                message={
                    "conversation_id": conversation_id,
                    "content": text,
                    "sender_id": whatsapp_id
                },
                org_id=context.org_id,
                mode="reply"
            )
            
            if result and result.get("message_id"):
                context.log(node_id, f"WhatsApp message sent successfully: {result.get('message_id')}")
                context.set_variable("last_message_id", result.get("message_id"))
            else:
                context.log(node_id, "Failed to send WhatsApp message", level="error")
                
        except Exception as e:
            context.log(node_id, f"Error sending WhatsApp message: {str(e)}", level="error")


async def execute_add_tag(context, node_id: str, config: Dict, trigger_data: Dict):
    """Add a tag to a contact"""
    tag = config.get("tag", "")
    conversation_id = trigger_data.get("conversation_id")
    
    if not tag or not conversation_id:
        context.log(node_id, "Missing tag or conversation_id", level="error")
        return
    
    # Replace variables in tag
    tag = replace_variables(tag, context.variables)
    
    context.log(node_id, f"Adding tag '{tag}' to conversation {conversation_id}")
    
    try:
        # Get the contact
        contacts_collection = db[f"contacts_{context.org_id}"]
        
        result = contacts_collection.update_one(
            {"conversation_id": conversation_id},
            {"$addToSet": {"categories": tag}}
        )
        
        if result.modified_count > 0:
            context.log(node_id, f"Tag '{tag}' added successfully")
        else:
            context.log(node_id, f"Tag '{tag}' was already present or contact not found")
            
    except Exception as e:
        context.log(node_id, f"Error adding tag: {str(e)}", level="error")


async def execute_remove_tag(context, node_id: str, config: Dict, trigger_data: Dict):
    """Remove a tag from a contact"""
    tag = config.get("tag", "")
    conversation_id = trigger_data.get("conversation_id")
    
    if not tag or not conversation_id:
        context.log(node_id, "Missing tag or conversation_id", level="error")
        return
    
    # Replace variables in tag
    tag = replace_variables(tag, context.variables)
    
    context.log(node_id, f"Removing tag '{tag}' from conversation {conversation_id}")
    
    try:
        contacts_collection = db[f"contacts_{context.org_id}"]
        
        result = contacts_collection.update_one(
            {"conversation_id": conversation_id},
            {"$pull": {"categories": tag}}
        )
        
        if result.modified_count > 0:
            context.log(node_id, f"Tag '{tag}' removed successfully")
        else:
            context.log(node_id, f"Tag '{tag}' was not present or contact not found")
            
    except Exception as e:
        context.log(node_id, f"Error removing tag: {str(e)}", level="error")


async def execute_set_custom_field(context, node_id: str, config: Dict, trigger_data: Dict):
    """Set a custom field on a contact"""
    field_name = config.get("fieldName", "")
    field_value = config.get("fieldValue", "")
    conversation_id = trigger_data.get("conversation_id")
    
    if not field_name or not conversation_id:
        context.log(node_id, "Missing fieldName or conversation_id", level="error")
        return
    
    # Replace variables
    field_name = replace_variables(field_name, context.variables)
    field_value = replace_variables(field_value, context.variables)
    
    context.log(node_id, f"Setting custom field '{field_name}' = '{field_value}'")
    
    try:
        contacts_collection = db[f"contacts_{context.org_id}"]
        
        result = contacts_collection.update_one(
            {"conversation_id": conversation_id},
            {"$set": {f"custom_fields.{field_name}": field_value}}
        )
        
        if result.matched_count > 0:
            context.log(node_id, f"Custom field '{field_name}' set successfully")
        else:
            context.log(node_id, f"Contact not found for conversation {conversation_id}", level="warning")
            
    except Exception as e:
        context.log(node_id, f"Error setting custom field: {str(e)}", level="error")


async def execute_http_request(context, node_id: str, config: Dict, trigger_data: Dict):
    """Make an HTTP request (webhook)"""
    import httpx
    
    url = config.get("url", "")
    method = config.get("method", "POST").upper()
    headers = config.get("headers", {})
    body = config.get("body", {})
    
    if not url:
        context.log(node_id, "Missing webhook URL", level="error")
        return
    
    # Replace variables in URL, headers, and body
    url = replace_variables(url, context.variables)
    
    # Replace variables in headers
    if isinstance(headers, dict):
        headers = {k: replace_variables(str(v), context.variables) for k, v in headers.items()}
    
    # Replace variables in body
    if isinstance(body, dict):
        body = {k: replace_variables(str(v), context.variables) for k, v in body.items()}
    elif isinstance(body, str):
        body = replace_variables(body, context.variables)
    
    context.log(node_id, f"Making {method} request to {url}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            if method == "GET":
                response = await client.get(url, headers=headers)
            elif method == "POST":
                response = await client.post(url, headers=headers, json=body)
            elif method == "PUT":
                response = await client.put(url, headers=headers, json=body)
            elif method == "DELETE":
                response = await client.delete(url, headers=headers)
            else:
                context.log(node_id, f"Unsupported HTTP method: {method}", level="error")
                return
            
            # Store response in variables
            context.set_variable("http_status_code", response.status_code)
            
            try:
                response_data = response.json()
                context.set_variable("http_response", response_data)
            except:
                context.set_variable("http_response", response.text)
            
            if response.status_code >= 200 and response.status_code < 300:
                context.log(node_id, f"HTTP request successful: {response.status_code}")
            else:
                context.log(
                    node_id,
                    f"HTTP request returned status {response.status_code}",
                    level="warning"
                )
                
    except Exception as e:
        context.log(node_id, f"Error making HTTP request: {str(e)}", level="error")


def replace_variables(text: str, variables: Dict[str, Any]) -> str:
    """
    Replace {{variable}} placeholders in text with actual values
    
    Example:
        text = "Hello {{customer_name}}!"
        variables = {"customer_name": "John"}
        result = "Hello John!"
    """
    import re
    
    if not isinstance(text, str):
        return str(text)
    
    # Find all {{variable}} patterns
    pattern = r'\{\{([^}]+)\}\}'
    
    def replacer(match):
        var_name = match.group(1).strip()
        
        # Support nested access like {{trigger_data.customer_name}}
        parts = var_name.split('.')
        value = variables
        
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return match.group(0)  # Return original if can't access
        
        return str(value) if value is not None else match.group(0)
    
    return re.sub(pattern, replacer, text)