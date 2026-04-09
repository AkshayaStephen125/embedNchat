import uuid
from pydantic import BaseModel

class TenantAgentLogin(BaseModel):
    username: str
    password: str


class AcceptRequest(BaseModel):
    session_id: str
    tenant_id: int
    tenant_brand_id: int