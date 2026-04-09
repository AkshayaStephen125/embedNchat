import enum
from sqlalchemy import Column, Integer, String, Boolean, Enum, ForeignKey, Text, JSON, DateTime
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from db_config import Base
from sqlalchemy.dialects.postgresql import UUID
import uuid


class Status(enum.Enum):
    Uploaded = "UPLOADED"
    Processed = "PROCESSED"
    Failed = "FAILED"

class Tone(enum.Enum):
    Professional = "PROFESSIONAL"
    Friendly = "FRIENDLY"
    Casual = "CASUAL"

class Sender(enum.Enum):
    User = "USER"
    Bot = "BOT"
    Agent = "AGENT"

class Takeover(enum.Enum):
    Auto = "AUTO"
    Manual = "MANUAL"

class TicketStatus(enum.Enum):
    Open = "OPEN"
    Inprogress = "INPROGRESS"
    Closed = "Closed"


class Tenant(Base):
    __tablename__ = "tenant"   

    tenant_id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String(50), unique=True, nullable=False)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String, unique=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    refresh_token = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(),nullable=False)


class TenantAgent(Base):
    __tablename__ = "tenant_agent"   

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(Integer, ForeignKey("tenant.tenant_id"))
    agent_name = Column(String(50), unique=True, nullable=False)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String, unique=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    refresh_token = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(),nullable=False)


class TenantBrand(Base):
    __tablename__ = "tenant_brand"

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(Integer, ForeignKey("tenant.tenant_id"))
    brand_name = Column(String(50), nullable=False)
    logo = Column(String(250), nullable=False)
    color_theme = Column(Integer, ForeignKey("tenant_brand_color_theme.id"))
    welcome_message = Column(String(200), nullable=True)
    tone = Column(Enum(Tone), default=Tone.Professional)
    api_key = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(),nullable=False)

class TenantBrandColorTheme(Base):
    __tablename__ = "tenant_brand_color_theme"

    id = Column(Integer, primary_key=True, index=True)
    tenant_brand_id = Column(Integer, ForeignKey("tenant_brand.id"))
    background_color = Column(String(50), nullable=False)
    header_color = Column(String(50), nullable=False)
    user_message_color = Column(String(50), nullable=False)
    bot_message_color = Column(String(50), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(),nullable=False)


class TenantDocumentBrand(Base):
    __tablename__ = "tenant_document_brand"

    document_id = Column(Integer, ForeignKey("tenant_document.document_id", ondelete="CASCADE"), primary_key=True)
    brand_id = Column(Integer, ForeignKey("tenant_brand.id", ondelete="CASCADE"), primary_key=True )

class TenantDocument(Base):
    __tablename__ = "tenant_document"

    tenant_id = Column(Integer, ForeignKey("tenant.tenant_id"))
    brands = relationship("TenantBrand", secondary="tenant_document_brand")
    document_id = Column(Integer, primary_key=True, index=True)
    document_name = Column(String(50), unique=True, nullable=False)
    document_path = Column(String, unique=True, nullable=False)
    document_status = Column(Enum(Status), default=Status.Uploaded)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(),nullable=False)

class UserSession(Base):
    __tablename__ = "user_session"

    id = Column(UUID(as_uuid=True), primary_key=True)
    tenant_id = Column(Integer, ForeignKey("tenant.tenant_id"), nullable=False)
    tenant_brand_id = Column(Integer, ForeignKey("tenant_brand.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class ChatHistory(Base):
    __tablename__ = "chat_history"

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(Integer, ForeignKey("tenant.tenant_id"), nullable=False)
    session_id = Column(UUID(as_uuid=True), ForeignKey("user_session.id"), nullable=False)
    message = Column(Text, nullable=False)
    sender = Column(Enum(Sender), nullable=False)
    takeover = Column(Enum(Takeover), default=Takeover.Auto)
    ticket_id = Column(Integer, ForeignKey("agent_chat_tickets.ticket_id"), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class TockenUsage(Base):
    __tablename__="token_usage"

    id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(Integer, ForeignKey("tenant.tenant_id"), nullable=False)
    session_id = Column(UUID(as_uuid=True), ForeignKey("user_session.id"), nullable=False)
    token_count = Column(Integer, default=0)


class AgentChatTickets(Base):
    __tablename__ = "agent_chat_tickets"

    ticket_id = Column(Integer, primary_key=True, index=True)
    tenant_id = Column(Integer, ForeignKey("tenant.tenant_id"), nullable=False)
    session_id = Column(UUID(as_uuid=True), ForeignKey("user_session.id"), nullable=False)
    agent_id = Column(Integer, ForeignKey("tenant_agent.id"), nullable=True)
    takeover = Column(Enum(TicketStatus), default=TicketStatus.Open)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

# class AuditLog(Base):
#     __tablename__ = "audit_logs"

#     id = Column(Integer, primary_key=True, index=True)
#     tenant_id = Column(Integer, ForeignKey("tenants.id"), nullable=True)
#     user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
#     action = Column(String(100), nullable=False)
#     entity_type = Column(String(100), nullable=True) 
#     entity_id = Column(String(100), nullable=True)
#     metadata = Column(JSON, nullable=True)

#     created_at = Column(
#         DateTime(timezone=True),
#         server_default=func.now(),
#         nullable=False
#     )


