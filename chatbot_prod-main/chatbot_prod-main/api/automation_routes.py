from typing import Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

# Internal Modules
from database import get_mongo_db
from auth.dependencies import CurrentUser
from services.automation_service import (
    create_automation_flow,
    list_automation_flows,
    get_automation_flow,
    update_automation_flow,
    delete_automation_flow,
    publish_automation_flow,
    validate_flow_structure
)
from core.logger import get_logger

logger = get_logger(__name__)
db = get_mongo_db()

router = APIRouter(prefix="/api/automation_flows", tags=["Automation Flows"])


# Request/Response Models
class FlowNodePayload(BaseModel):
    """Represents a node's configuration in the flow"""
    type: str
    app: Optional[str] = None
    config: dict = Field(default_factory=dict)
    position: Optional[dict] = None


class FlowDocument(BaseModel):
    """Complete flow document structure"""
    nodes: dict = Field(default_factory=dict)
    connections: list = Field(default_factory=list)
    triggers: list = Field(default_factory=list)


class CreateFlowRequest(BaseModel):
    name: str = Field(..., min_length=3, max_length=255)
    description: Optional[str] = Field(None, max_length=500)
    flow_data: Optional[FlowDocument] = None
    status: str = Field(default="draft")


class UpdateFlowRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=3, max_length=255)
    description: Optional[str] = Field(None, max_length=500)
    flow_data: Optional[FlowDocument] = None
    status: Optional[str] = None


# CREATE FLOW ENDPOINT
@router.post("")
async def create_flow(payload: CreateFlowRequest, user: CurrentUser):
    """
    Create a new automation flow
    
    """
    try:
        org_id = user.org_id
        if not org_id:
            raise HTTPException(status_code=400, detail="Organization ID not found")

        # Validate flow structure if flow_data is provided
        if payload.flow_data:
            is_valid, error_message = validate_flow_structure(payload.flow_data.dict())
            if not is_valid:
                raise HTTPException(status_code=400, detail=f"Invalid flow structure: {error_message}")

        flow = await create_automation_flow(
            org_id=org_id,
            name=payload.name,
            description=payload.description,
            flow_data=payload.flow_data.dict() if payload.flow_data else None,
            created_by=user.user_id
        )

        return {"flow": flow}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating automation flow: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create automation flow: {str(e)}")
    
@router.get("")
async def list_flows(user: CurrentUser, status: Optional[str] = None):
    """
    List all automation flows for the organization
    
    Query parameters:
    - status: Filter by status ("draft" or "published")

    """
    try:
        org_id = user.org_id
        if not org_id:
            raise HTTPException(status_code=400, detail="Organization ID not found")

        flows = await list_automation_flows(org_id=org_id, status=status)

        return {"flows": flows}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing automation flows: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list automation flows: {str(e)}")
    

# GET SINGLE 
@router.get("/{flow_id}")
async def get_flow(flow_id: str, user: CurrentUser):
    """Get a specific automation flow by ID"""
    try:
        org_id = user.org_id
        if not org_id:
            raise HTTPException(status_code=400, detail="Organization ID not found")

        flow = await get_automation_flow(org_id=org_id, flow_id=flow_id)

        if not flow:
            raise HTTPException(status_code=404, detail="Automation flow not found")

        return {"flow": flow}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting automation flow: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get automation flow: {str(e)}")


# UPDATE FLOW
@router.patch("/{flow_id}")
async def update_flow(flow_id: str, payload: UpdateFlowRequest, user: CurrentUser):
    """Update an existing automation flow"""
    try:
        org_id = user.org_id
        if not org_id:
            raise HTTPException(status_code=400, detail="Organization ID not found")

        if payload.flow_data:
            is_valid, error_message = validate_flow_structure(payload.flow_data.dict())
            if not is_valid:
                raise HTTPException(status_code=400, detail=f"Invalid flow structure: {error_message}")

        update_data = {}
        if payload.name is not None:
            update_data["name"] = payload.name
        if payload.description is not None:
            update_data["description"] = payload.description
        if payload.flow_data is not None:
            update_data["flow_data"] = payload.flow_data.dict()
        if payload.status is not None:
            update_data["status"] = payload.status

        flow = await update_automation_flow(
            org_id=org_id,
            flow_id=flow_id,
            update_data=update_data,
            updated_by=user.user_id
        )

        if not flow:
            raise HTTPException(status_code=404, detail="Automation flow not found")

        return {"flow": flow}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating automation flow: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to update automation flow: {str(e)}")


@router.delete("/{flow_id}")
async def delete_flow(flow_id: str, user: CurrentUser):
    """Delete an automation flow"""
    try:
        org_id = user.org_id
        if not org_id:
            raise HTTPException(status_code=400, detail="Organization ID not found")

        success = await delete_automation_flow(org_id=org_id, flow_id=flow_id)

        if not success:
            raise HTTPException(status_code=404, detail="Automation flow not found")

        return {"message": "Automation flow deleted successfully"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting automation flow: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete automation flow: {str(e)}")


# PUBLISH FLOW
@router.post("/{flow_id}/publish")
async def publish_flow(flow_id: str, user: CurrentUser):
    """Publish (activate) an automation flow"""
    try:
        org_id = user.org_id
        if not org_id:
            raise HTTPException(status_code=400, detail="Organization ID not found")

        flow = await publish_automation_flow(org_id=org_id, flow_id=flow_id)

        if not flow:
            raise HTTPException(status_code=404, detail="Automation flow not found")

        return {"flow": flow, "message": "Automation flow published successfully"}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error publishing automation flow: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to publish automation flow: {str(e)}")
















