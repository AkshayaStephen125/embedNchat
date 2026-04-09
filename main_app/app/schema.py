from pydantic import BaseModel, EmailStr

class TenantCreate(BaseModel):
    company_name: str
    username: str
    email: EmailStr
    password: str

class TenantAgentCreate(BaseModel):
    name: str
    username: str
    email: EmailStr
    password: str
    
class TenantAgentEdit(BaseModel):
    name: str
    username: str
    email: EmailStr
    password: str

class TenantLogin(BaseModel):
    username: str
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class ChatRequest(BaseModel):
    message: str

class BrandCreate(BaseModel):
    brand_name: str
    logo: str
    background_color: str
    header_color: str
    user_message_color: str
    bot_message_color: str
    welcome_message: str
    tone: str

class BrandEdit(BaseModel):
    brand_id: int
    brand_name: str
    logo: str
    background_color: str
    header_color: str
    user_message_color: str
    bot_message_color: str
    welcome_message: str
    tone: str


