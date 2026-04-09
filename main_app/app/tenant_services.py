import os
import re
import auth
from shared import models
import schema
import secrets
import uuid
import shutil
import time
import kafka_producer
from jose import JWTError, jwt
from shared.logger import logger
from db_config import get_db
from fastapi import Request, HTTPException, Depends, Header, Response
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc
from pathlib import Path
from datetime import datetime, timedelta

SECRET_KEY = os.environ.get("SECRET_KEY")
ALGORITHM = os.environ.get("ALGORITHM")
SESSION_COOKIE_NAME = os.environ.get("SESSION_COOKIE_NAME")
BASE_URL = os.environ.get("BASE_URL")

ACCESS_TOKEN_EXPIRE_MINUTES = int(os.environ.get("ACCESS_TOKEN_EXPIRE_MINUTES"))


def generate_api_key():
    return "API_"+secrets.token_urlsafe(32)

def tenant_sign_up(tenant:schema.TenantCreate, db: Session = get_db() ):
    result = False
    message = ''
    try:
        existing_tenant = db.query(models.Tenant).filter(models.Tenant.email == tenant.email).first()
        if existing_tenant:
            message = 'Email already registered'
            return result, message

        existing_username = db.query(models.Tenant).filter(models.Tenant.username == tenant.username).first()
        if existing_username:
            message = 'Username already taken'
            return result, message
        
        new_tenant = models.Tenant(
            company_name = tenant.company_name,
            username = tenant.username,
            email = tenant.email,
            hashed_password = auth.hash_password(tenant.password),
            # api_key = generate_api_key()
        )
        db.add(new_tenant)
        db.commit()
        db.refresh(new_tenant)
        result = True
    except Exception as e:
        logger.info(str(e))
    return result, message


def create_tenant_agent(tenant_id, tenant:schema.TenantAgentCreate, db: Session = get_db() ):
    result = False
    message = ''
    try:
        existing_tenant = db.query(models.TenantAgent).filter(models.TenantAgent.email == tenant.email).first()
        if existing_tenant:
            message = 'Email already registered'
            return result, message

        existing_username = db.query(models.TenantAgent).filter(models.TenantAgent.username == tenant.username).first()
        if existing_username:
            message = 'Username already taken'
            return result, message
        
        new_tenant = models.TenantAgent(
            agent_name = tenant.name,
            tenant_id = tenant_id,
            username = tenant.username,
            email = tenant.email,
            hashed_password = auth.hash_password(tenant.password),
        )
        db.add(new_tenant)
        db.commit()
        db.refresh(new_tenant)
        result = True
    except Exception as e:
        logger.info(str(e))
    return result, message

def edit_tenant_agent(tenant_id, tenant:schema.TenantAgentEdit, db: Session = get_db() ):
    result = False
    message = ''
    try:
        existing_tenant = db.query(models.TenantAgent).filter(models.TenantAgent.id == tenant.id).first()
        if existing_tenant:
            existing_tenant.tenant_id = tenant_id,
            existing_tenant.agent_name = tenant.name,
            existing_tenant.username = tenant.username,
            existing_tenant.email = tenant.email,
            existing_tenant.hashed_password = auth.hash_password(tenant.password),
        db.commit()
        result = True
    except Exception as e:
        logger.info(str(e))
    return result, message

def get_current_tenant(request: Request):
    tenant = ''
    try:
        db: Session = Depends(get_db)
        token = request.cookies.get("access_token")
        if not token:
            logger.info("Not Authorized")
        
        payload = auth.verify_token(token, "access")
        tenant_id = payload.get("tenant_id")

        tenant = db.query(models.Tenant).filter(
            models.Tenant.tenant_id == tenant_id).first()

        if not tenant:
            logger.info("Invalid Tenant")
    except Exception as e:
        logger.info(str(e))
    return tenant

def sign_in_tenant(tenant_data, db):
    result = False
    message = ''
    api_response = ''
    try:
        tenant = db.query(models.Tenant).filter(models.Tenant.username == tenant_data.username).first()
        if not tenant or not auth.verify_password(tenant_data.password, tenant.hashed_password):
            message="Invalid credentials"
        else:
            access_token = auth.create_access_token({"tenant_id": tenant.tenant_id})
            refresh_token = auth.create_refresh_token({"tenant_id": tenant.tenant_id})

            tenant.refresh_token = refresh_token
            db.commit()
            api_response = RedirectResponse(url="/dashboard", status_code=303)
            
            api_response.set_cookie(key="access_token", value=access_token, httponly=True, max_age=900, secure=False, samesite="lax" )
            import time
            api_response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, max_age=604800, secure=False, samesite="lax" )
            api_response.set_cookie(key="access_expiry", value=str(int(time.time()) + ACCESS_TOKEN_EXPIRE_MINUTES * 60), httponly=False, max_age=604800, secure=False, samesite="lax" )
            
            result = True

    except Exception as e:
        logger.exception("Signup failed")
    return result, message, api_response


def get_tenant_folder(tenant_id):
    BASE_STORAGE = Path("files")
    BASE_STORAGE.mkdir(exist_ok=True)
    folder = BASE_STORAGE / f"tenant_{tenant_id}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder

def get_tenant_brand_logo_folder(tenant_id: int):
    BASE_STORAGE = Path("static/logo")  # inside static
    BASE_STORAGE.mkdir(parents=True, exist_ok=True)

    tenant_folder = BASE_STORAGE / f"tenant_{tenant_id}"
    tenant_folder.mkdir(parents=True, exist_ok=True)

    return tenant_folder

def tenant_file_upload(request: Request, file_name, brand_ids, file, db):
    result = False
    message = ''
    file_location = ""
    try:
        tenant_id = request.state.tenant.tenant_id
        tenant_folder = get_tenant_folder(tenant_id)
        file_location = f"{tenant_folder}/{file.filename}"
        with open(file_location, "wb+") as file_object:
            file_object.write(file.file.read())
        brands = db.query(models.TenantBrand).filter(models.TenantBrand.id.in_(brand_ids)).all()
        document = models.TenantDocument(tenant_id=tenant_id, 
                                         brands=brands,
                                         document_name=file_name, 
                                         document_path=file_location)
        db.add(document)
        db.commit()
        db.refresh(document)
        result = True
        message = 'File uploaded successfully'
    except Exception as e:
        logger.exception("File upload failed")
        message = str(e)
    return result, message, file_location

def upload_brand_detail(request, logo, tenant_brand_info:schema.BrandCreate, db: Session = get_db()):
    result = False
    message = ''
    try:
        tenant_id = request.state.tenant.tenant_id
        tenant_folder = get_tenant_brand_logo_folder(tenant_id)
        extension = Path(logo.filename).suffix
        unique_name = f"{uuid.uuid4().hex}{extension}"
        file_location = tenant_folder / unique_name
        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(logo.file, buffer)
        tenant_brand = models.TenantBrand(tenant_id=tenant_id,
                                        brand_name=tenant_brand_info.brand_name,
                                        logo=f"/static/logo/tenant_{tenant_id}/{unique_name}",
                                        welcome_message=tenant_brand_info.welcome_message,
                                        tone=tenant_brand_info.tone,
                                        api_key=generate_api_key()
                                        )
        db.add(tenant_brand)
        db.commit()
        db.refresh(tenant_brand)

        tenant_brand_color_theme = models.TenantBrandColorTheme(tenant_brand_id=tenant_brand.id,
                                                background_color=tenant_brand_info.background_color,
                                                header_color=tenant_brand_info.header_color,
                                                user_message_color=tenant_brand_info.user_message_color,
                                                bot_message_color=tenant_brand_info.bot_message_color
                                                )
        db.add(tenant_brand_color_theme)
        db.commit()
        db.refresh(tenant_brand_color_theme)

        tenant_brand_update = db.query(models.TenantBrand).filter(models.TenantBrand.id == tenant_brand.id).first()
        tenant_brand_update.color_theme = tenant_brand_color_theme.id
        db.commit()
        result = True
    except Exception as e:
        logger.exception("File upload failed")
        message = str(e)
    return result, message
        

def get_current_tenant_brand_by_api_key(
                x_api_key: str = Header(...),
                db: Session = Depends(get_db)
            ):
    tenant_brand = db.query(models.TenantBrand).filter(models.TenantBrand.api_key == x_api_key).first()
    if not tenant_brand:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return tenant_brand

def get_chat_theme(tenant_brand, db:Session = Depends(get_db)):
    result = False
    message = ''
    tenant_brand_theme = dict()
    try:
        colortheme = db.query(models.TenantBrandColorTheme).filter(models.TenantBrandColorTheme.tenant_brand_id==tenant_brand.id).first()
        tenant_brand_theme = {'id':tenant_brand.id,
                        'brand_name':tenant_brand.brand_name,
                        'logo':tenant_brand.logo,
                        'welcome_message':tenant_brand.welcome_message,
                        'tone':tenant_brand.tone,
                        'header_color': colortheme.header_color,
                        'background_color':colortheme.background_color,
                        'user_message_color':colortheme.user_message_color,
                        'bot_message_color':colortheme.bot_message_color
            }
        result = True
    except Exception as e:
        logger.exception("Getting brands failed")
        message = str(e)
    return result, message, tenant_brand_theme

def get_brands(request: Request, db: Session = get_db()):
    result = False
    message = ''
    brand_data = list()
    try:
        tenant_id = request.state.tenant.tenant_id
        brands = db.query(models.TenantBrand).filter(models.TenantBrand.tenant_id==tenant_id).filter(models.TenantBrand.is_active==True)
        for brand in brands:
            data = dict()
            colortheme = db.query(models.TenantBrandColorTheme).filter(models.TenantBrandColorTheme.tenant_brand_id==brand.id).first()
            data = {'id':brand.id,
                        'brand_name':brand.brand_name,
                        'logo':brand.logo,
                        'welcome_message':brand.welcome_message, 
                        'tone':brand.tone, 
                        'header_color': colortheme.header_color,
                        'background_color':colortheme.background_color,
                        'user_message_color':colortheme.user_message_color,
                        'bot_message_color':colortheme.bot_message_color,
                        'embed':f'<script src="http://127.0.0.1:8000/static/widget.js" data-api-key="{brand.api_key}"></script>'}
            brand_data.append(data)
            result = True
    except Exception as e:
        logger.exception("Getting brands failed")
        message = str(e)
    return result, message, brand_data

def get_tenant_agents(request: Request, db: Session = get_db()):
    result = False
    message = ''
    agent_list = list()
    try:
        tenant_id = request.state.tenant.tenant_id
        agents = db.query(models.TenantAgent).filter(models.TenantAgent.tenant_id==tenant_id).filter(models.TenantAgent.is_active==True)
        for agent in agents:
            data = dict()
            data = {'id':agent.id,
                        'name':agent.agent_name,
                        'username':agent.username,
                        'email':agent.email}
            agent_list.append(data)
            result = True
    except Exception as e:
        logger.exception("Getting brands failed")
        message = str(e)
    return result, message, agent_list

def get_tenant_agent(tenant_id, agent_id, db: Session = get_db()):
    result = False
    message = ''
    agent_dict = dict()
    try:
        agent = db.query(models.TenantAgent).filter(models.TenantAgent.tenant_id==tenant_id).filter(models.TenantAgent.id==agent_id).first()
        agent_dict = {'agent_id':agent.id,
                'name':agent.agent_name,
                'username':agent.username,
                'email':agent.email}
        result = True
    except Exception as e:
        logger.exception("Getting brands failed")
        message = str(e)
    return result, message, agent_dict

def get_brand_detail(brand_id: int, db: Session = get_db()):
    result = False
    message = ''
    brand_data = None
    try:
        brand = db.query(models.TenantBrand).filter(models.TenantBrand.id==brand_id).first()
        if brand:
            data = dict()
            colortheme = db.query(models.TenantBrandColorTheme).filter(models.TenantBrandColorTheme.tenant_brand_id==brand.id).first()
            data = {'id':brand.id,
                        'brand_name':brand.brand_name,
                        'logo':brand.logo,
                        'welcome_message':brand.welcome_message,
                        'tone':brand.tone,
                        'header_color': colortheme.header_color,
                        'background_color':colortheme.background_color,
                        'user_message_color':colortheme.user_message_color,
                        'bot_message_color':colortheme.bot_message_color,
                        'embed':f'<script src="{BASE_URL}/static/widget.js" data-api-key="APIEUxCDuHHXdEzEJqkcyM76Iftvs2XVp53IYnMd4_8v8k"></script>'}
            brand_data = data
            result = True
    except Exception as e:
        logger.exception("Getting brand details failed")
        message = str(e)
    return result, message, brand_data

def edit_brand_detail(request, brand_id: int, tenant_brand_info:schema.BrandCreate, db: Session = get_db(), logo=None):
    result = False
    message = ''
    try:
        tenant_id = request.state.tenant.tenant_id
        tenant_folder = get_tenant_brand_logo_folder(tenant_id)
        if logo.filename:
            extension = Path(logo.filename).suffix
            unique_name = f"{uuid.uuid4().hex}{extension}"
            file_location = tenant_folder / unique_name
            with open(file_location, "wb") as buffer:
                shutil.copyfileobj(logo.file, buffer)
        
        brand = db.query(models.TenantBrand).filter(models.TenantBrand.id == brand_id).first()
        brand.brand_name = tenant_brand_info.brand_name
        brand.welcome_message = tenant_brand_info.welcome_message
        brand.tone = tenant_brand_info.tone
        if logo.filename:
            brand.logo = f"/static/logo/tenant_{tenant_id}/{unique_name}"

        colortheme = db.query(models.TenantBrandColorTheme).filter(models.TenantBrandColorTheme.tenant_brand_id==brand_id).first()
        colortheme.background_color = tenant_brand_info.background_color
        colortheme.header_color = tenant_brand_info.header_color
        colortheme.user_message_color = tenant_brand_info.user_message_color
        colortheme.bot_message_color = tenant_brand_info.bot_message_color

        db.commit()
        result = True
    except Exception as e:
        logger.exception("Editing brand details failed")
        message = str(e)
    return result, message

def delete_brand(brand_id: int, db: Session = get_db()):
    result = False
    message = ''
    try:
        brand = db.query(models.TenantBrand).filter(models.TenantBrand.id == brand_id).first()
        brand.is_active = False
        db.commit()
        result = True
    except Exception as e:
        logger.exception("Deleting brand failed")
        message = str(e)
    return result, message

def reformat_content(text: str) -> str:
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = re.sub(r":\s*", ":\n\n", text, count=1)
    text = re.sub(r"(\d+)\.\s([^:]+):", r"\1. \2 –", text)
    text = re.sub(r"\s(\d+\.)", r"\n\1", text)
    return text.strip()


def brand_options(tenant_id, db: Session = get_db()):
    result = False
    message = ''
    brand_data = list()
    try:
        brands = db.query(models.TenantBrand).filter(models.TenantBrand.tenant_id == tenant_id).filter(models.TenantBrand.is_active==True)
        brand_data = [(brand.id, brand.brand_name) for brand in brands]
        result = True
    except Exception as e:
        logger.exception("Getting brand options failed")
        message = str(e)
    return result, message, brand_data


def save_message(session_id, message, sender, db, takeover='Auto'):
    result = False,
    try:
        chat_history = models.ChatHistory(
            session_id=uuid.UUID(session_id),
            message=message,
            sender=sender,
            takeover=takeover
        )
        db.add(chat_history)
        db.commit()
        db.refresh(chat_history)
        result = True
    except Exception as e:
        db.rollback()
        logger.exception("Saving chat history failed")
        message = str(e)
    return result


def publish_file_to_kafka(tenant_id, uploaded_file, brand_ids):
    result = False
    message = ''
    try:
        file_info = dict()
        file_info['tenant_id'] = tenant_id
        file_info['uploaded_file'] = uploaded_file
        file_info['brand_ids'] = brand_ids
        result = kafka_producer.publish_file_to_kafka(file_info)
    except Exception as e:
        logger.exception("Publishing failed")
        message = str(e)
    return result


    
def get_user_sessions(tenant_id, db):
    result = False
    session_list = list()
    try:
        sessions = db.query(models.UserSession).filter(models.UserSession.tenant_id==tenant_id).order_by(models.UserSession.id.desc())
        for session in sessions:
            session_dict = dict()
            last_message = db.query(models.ChatHistory).filter(models.ChatHistory.session_id == session.id).order_by(desc(models.ChatHistory.created_at)).first()
            if last_message:
                session_dict = {'session_id': session.id,
                                'last_message': last_message.message,
                                'created_at': session.created_at
                                }
                session_list.append(session_dict)
        result = True
    except Exception as e:
        db.rollback()
        logger.exception(f"Fetching chat sessions failed {e}")
    return result, session_list

def get_user_sessions_chats(session_id, db):
    result = False
    session_chat_list = list()
    try:
        messages = db.query(models.ChatHistory).filter(models.ChatHistory.session_id==session_id)
        for message in messages:
            message_dict = dict()
            message_dict = {'sender': message.sender.value,
                            'content': reformat_content(message.message),
                            'created_at': message.created_at
                            }
            session_chat_list.append(message_dict)
        result = True
    except Exception as e:
        db.rollback()
        logger.exception(f"Fetchong chat history failed {e}")
    return result, session_chat_list


# ***********************************API_SERVICES****************************************************

def create_session_token(tenant_brand):
    session_id = str(uuid.uuid4())
    payload = {
        "tenant_id": str(tenant_brand.tenant_id),
        "tenant_brand_id": str(tenant_brand.id),
        "api_key": str(tenant_brand.api_key),
        "session_id": session_id,
    }
    access_expiry = str(int(time.time()) + 15)
    token = auth.create_chat_access_token(payload)
    refresh_token = auth.create_chat_access_token(payload)
    return token, refresh_token, access_expiry

def verify_token(token: str):
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=ALGORITHM)
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    
def set_session_cookie(response: Response, token: str):
    result = False
    message = ''
    try:
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=token,
            httponly=True,
            secure=False,
            samesite="lax",
            max_age=2592000  # 30 days
        )
        result = True
        message = "Session initialized"
    except Exception as e:
        message = str(e)
    return result, message

def get_previous_messages(session_id, db):
    result = False
    message = ''
    previous_messages_list = []
    try:
        previous_messages = db.query(models.ChatHistory).filter(models.ChatHistory.session_id == session_id).all()
        for previous_message in previous_messages:
            previous_messages_dict = dict()
            previous_messages_dict = {'sender':previous_message.sender,
                                    'message':previous_message.message,
                                    'timestamp':previous_message.created_at
                                    }
            previous_messages_list.append(previous_messages_dict)
        result = True
    except Exception as e:
        message = str(e)
    return result, message, previous_messages_list

def get_user_sessions_requests(tenant_id, db):
    result = False
    message = ''
    session_list = list()
    try:
        chat_requests =  (
            db.query(models.AgentChatHistory.session_id)
            .join(
                models.UserSession,
                models.AgentChatHistory.session_id == models.UserSession.id
            )
            .filter(
                models.UserSession.tenant_id == tenant_id,
                models.AgentChatHistory.is_accepted == False
            )
        )
        if chat_requests.first():
            sessions = db.query(models.UserSession).filter(models.UserSession.tenant_id==tenant_id).filter(models.UserSession.id.in_(chat_requests)).order_by(models.UserSession.id.desc())
            for session in sessions:
                session_dict = dict()
                last_message = db.query(models.ChatHistory).filter(models.ChatHistory.session_id == session.id).order_by(desc(models.ChatHistory.created_at)).first()
                if last_message:
                    session_dict = {'session_id': session.id,
                                    'last_message': last_message.message,
                                    'created_at': session.created_at
                                    }
                    session_list.append(session_dict)
            result = True
        else:
            message = "No sessions found"

    except Exception as e:
        db.rollback()
        logger.exception(f"Fetching chat sessions failed {e}")
    return result, message, session_list

def refresh_tenant_token(refresh_token, tenant_id, response, db):
    result = False
    new_access = ''
    new_refresh = ''
    try:
        tenant = db.query(models.Tenant).filter(
            models.Tenant.tenant_id == tenant_id
        ).first()

        if not tenant or tenant.refresh_token != refresh_token:
            raise HTTPException(status_code=401, detail="Invalid refresh token")

        new_access = auth.create_access_token({"tenant_id": tenant.tenant_id})
        new_refresh = auth.create_refresh_token({"tenant_id": tenant.tenant_id})

        tenant.refresh_token = new_refresh
        db.commit()
        response = RedirectResponse(url="/dashboard", status_code=303)

        response.set_cookie("access_token", new_access, httponly=True)
        response.set_cookie("refresh_token", new_refresh, httponly=True)
        response.set_cookie(key="access_expiry", value=str(int(time.time()) + ACCESS_TOKEN_EXPIRE_MINUTES * 60), path="/"
)
        result = True
    except Exception as e:
        logger.exception(f"Refresh failed:  {e}")
    return result, response

def save_user_session(session_id, tenant_id, tenant_brand_id, db):
    result = False
    message = ''
    try:
        if user_session := db.query(models.UserSession).filter(models.UserSession.id == session_id).first():
           result = True
           message = 'Session already exists'
        else:
            user_session = models.UserSession(
                id= session_id,
                tenant_id=tenant_id,
                tenant_brand_id=tenant_brand_id
            )
            db.add(user_session)
            db.commit()
            db.refresh(user_session)
            message = 'New Session created'
        if user_session:
            result = True
    except Exception as e:
        logger.exception("Saving chat history failed")
        message = str(e)
        result = False
    logger.info(f"Session Status: {result}, Message: {message}")
    return result





