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
from sqlalchemy import desc, func
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
            if tenant.password:
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
    return result, message, file_location, document.document_id

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
                        'email':agent.email,
                        'created_at':agent.created_at.date().strftime("%Y-%m-%d")}
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
                'email':agent.email,
                'created_at':agent.created_at.strftime("%-d/%-m/%Y, %-I:%M:%S %p").lower()}
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

def tenant_documents(tenant_id, db):
    result = False
    message = ''
    document_data = list()
    try:
        documents = db.query(models.TenantDocument).filter(models.TenantDocument.tenant_id == tenant_id).order_by(models.TenantDocument.document_id.desc())
        for document in documents:
            brand_names = []
            if document.brands:
                brand_names = []

                for brand in document.brands:
                    brand_data = (
                        db.query(models.TenantBrand)
                        .filter(models.TenantBrand.id == brand.id)
                        .first()
                    )

                    if brand_data:
                        brand_names.append(brand_data.brand_name)

                        document.brand_names = ", ".join(brand_names)

                    else:
                        document.brand_names = ""
            data = dict()
            data = {'id':document.document_id,
                        'name':document.document_name,
                        'file_url':document.document_path,
                        'brand_name': document.brand_names,
                        'status':document.document_status.value,
                        'uploaded_at':document.created_at.strftime("%-d/%-m/%Y, %-I:%M:%S %p").lower()}
            document_data.append(data)
            result = True
    except Exception as e:
        logger.exception("Getting brand options failed")
        message = str(e)
    return result, message, document_data


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


def publish_file_to_kafka(tenant_id, name, uploaded_file, brand_ids, file_id):
    result = False
    message = ''
    try:
        file_info = dict()
        file_info['file_id'] = file_id
        file_info['tenant_id'] = tenant_id
        file_info['name'] = name
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
        sessions = db.query(models.UserSession).filter(models.UserSession.tenant_id==tenant_id).order_by(models.UserSession.created_at.desc())
        for session in sessions:
            session_dict = dict()
            last_message = db.query(models.ChatHistory).filter(models.ChatHistory.session_id == session.id).order_by(desc(models.ChatHistory.created_at)).first()
            if last_message:
                session_dict = {'session_id': session.id,
                                'tenant_brand_id': session.tenant_brand_id,
                                'last_message': last_message.message,
                                'created_at': session.created_at
                                }
                session_list.append(session_dict)
        result = True
    except Exception as e:
        db.rollback()
        logger.exception(f"Fetching chat sessions failed {e}")
    return result, session_list

def get_user_sessions_tickets(ticket_id, db):
    result = False
    ticket_status = ''
    session_info = dict()
    session_chat_list = list()
    try:
        ticket = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.ticket_id==ticket_id).first()
        session = db.query(models.UserSession).filter(models.UserSession.id==ticket.session_id).first()
        ticket_status = ticket.status.value
        session_info["session_id"] = session.id
        session_info["tenant_id"] = session.tenant_id
        session_info["tenant_brand_id"] = session.tenant_brand_id

        if session:
            messages = db.query(models.ChatHistory).filter(models.ChatHistory.ticket_id==ticket.ticket_id).order_by((models.ChatHistory.timestamp)).all()
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
        logger.exception(f"Fetching chat history failed {e}")
    return result, session_info, session_chat_list, ticket_status

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
        logger.exception(f"Fetching chat history failed {e}")
    return result, session_chat_list


# ***********************************API_SERVICES****************************************************

def create_session_token(tenant_brand):
    session_id = str(uuid.uuid4())
    payload = {
        "tenant_id": str(tenant_brand.tenant_id),
        "tenant_brand_id": str(tenant_brand.id),
        "brand_tone": tenant_brand.tone.value,
        "api_key": str(tenant_brand.api_key),
        "session_id": session_id,
    }
    access_expiry = str(int(time.time()) + ACCESS_TOKEN_EXPIRE_MINUTES * 60)
    token = auth.create_chat_access_token(payload)
    refresh_token = auth.create_chat_access_token(payload)
    return token, refresh_token, access_expiry

def create_session_token_from_payload(payload_data):
    payload = {
        "tenant_id": str(payload_data['tenant_id']),
        "tenant_brand_id": str(payload_data['tenant_brand_id']),
        "brand_tone": payload_data['brand_tone'],
        "api_key": str(payload_data['api_key']),
        "session_id": payload_data['session_id'],
    }
    access_expiry = str(int(time.time()) + ACCESS_TOKEN_EXPIRE_MINUTES * 60)
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

def get_user_tickets(tenant_id, db):
    result = False
    message = ''
    ticket_list = list()
    try:
        open_tickets = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.tenant_id==tenant_id).order_by(desc(models.AgentChatTickets.ticket_id)).all()
        if open_tickets:
            for ticket in open_tickets:
                session_dict = dict()
                last_message = db.query(models.ChatHistory).filter(models.ChatHistory.ticket_id == ticket.ticket_id).order_by(desc(models.ChatHistory.timestamp)).first()
                agent = db.query(models.TenantAgent).filter(models.TenantAgent.id==ticket.agent_id).first()
                session = db.query(models.UserSession).filter(models.UserSession.id==ticket.session_id).first()
                tenant_brand_id = db.query(models.TenantBrand).filter(models.TenantBrand.id==session.tenant_brand_id).first()
                if last_message:
                    session_dict = {'ticket_id': ticket.ticket_id,
                                    'tenant_brand_id': tenant_brand_id.id,
                                    'last_message': last_message.message,
                                    'created_at': ticket.created_at,
                                    'status': ticket.status.value,
                                    'agent': agent.agent_name if agent else ''
                                    }
                    ticket_list.append(session_dict)
            result = True
        else:
            message = "No sessions found"

    except Exception as e:
        db.rollback()
        logger.exception(f"Fetching chat sessions failed {e}")
    return result, message, ticket_list

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
        response.set_cookie(key="access_expiry", value=str(int(time.time()) + ACCESS_TOKEN_EXPIRE_MINUTES * 60), path="/")

        result = True
    except Exception as e:
        logger.exception(f"Refresh failed:  {e}")
    return result, response

def get_dashboard_data(tenant_id, db):
    result = False
    dashboard_info = dict()
    try:
        tenant = db.query(models.Tenant).filter(models.Tenant.tenant_id==tenant_id).first()
        total_sessions = db.query(models.UserSession).filter(models.UserSession.tenant_id == tenant_id).count()

        open_tickets = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.tenant_id == tenant_id).filter(models.AgentChatTickets.status == 'Open').count()
        active_tickets = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.tenant_id == tenant_id).filter(models.AgentChatTickets.status == 'Inprogress').count()
        closed_tickets = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.tenant_id == tenant_id).filter(models.AgentChatTickets.status == 'Closed').count()

        brands = db.query(models.TenantBrand).filter(models.TenantBrand.tenant_id == tenant_id).filter(models.TenantBrand.is_active==True).count()
        agents = db.query(models.TenantAgent).filter(models.TenantAgent.tenant_id == tenant_id).count()
        documents = db.query(models.TenantDocument).filter(models.TenantDocument.tenant_id == tenant_id).count()

        sessions_per_day = (
            db.query(
                func.date(models.UserSession.created_at).label("date"),
                func.count(models.UserSession.id).label("count")
            )
            .filter(models.UserSession.tenant_id == tenant_id)
            .group_by(func.date(models.UserSession.created_at))
            .all()
        )
        sessions_per_brand = db.query(models.TenantBrand.id.label("brand_id"),models.TenantBrand.brand_name,models.TenantBrand.logo,func.count(models.UserSession.id).label("session_count")
                        ).outerjoin(models.UserSession,models.UserSession.tenant_brand_id == models.TenantBrand.id
                        ).group_by(models.TenantBrand.id,models.TenantBrand.brand_name,models.TenantBrand.logo
                        ).filter(models.TenantBrand.tenant_id==tenant_id).filter(models.TenantBrand.is_active==True)
        session_dates = list()
        session_counts = list()
        for row in sessions_per_day:
            session_dates.append(row.date.strftime("%Y-%m-%d"))
            session_counts.append(row.count)

        token_usage_per_day = (
            db.query(
                func.date(models.TockenUsage.date).label("date"),
                func.sum(models.TockenUsage.token_count).label("count")
            )
            .filter(models.TockenUsage.tenant_id == tenant_id)
            .group_by(func.date(models.TockenUsage.date))
            .all()
        )
        token_dates = list()
        token_usage = list()
        for row in token_usage_per_day:
            token_dates.append(row.date.strftime("%Y-%m-%d"))
            token_usage.append(row.count)
        print("token_usage      ",token_usage)
        dashboard_info = {"tenant_name": tenant.company_name,
                          "username": tenant.username,
                          "total_sessions": total_sessions,
                          "total_brands": brands,
                          "total_agents": agents,
                          "total_files": documents,  
                          "total_tickets": open_tickets + active_tickets + closed_tickets,
                          "open_tickets": open_tickets,
                          "active_tickets": active_tickets,
                          "closed_tickets": closed_tickets,  
                          "session_dates": session_dates,
                          "session_counts": session_counts,  
                          "token_dates": token_dates,
                          "sessions_per_brand": sessions_per_brand,
                          "token_usage": token_usage
                             }
        result = True
    except Exception as e:
        logger.exception(f"Dashboard info fetch failed {e}")
    return result, dashboard_info

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

async def publish_to_services(payload, query, manager):
    message_info = dict()
    alert_info = dict()
    result = False
    message = ''
    try:
        message_info = {"tenant_id": payload['tenant_id'],
                        "tenant_brand_id": payload['tenant_brand_id'],
                        "brand_tone": payload['brand_tone'],
                        "session_id": payload['session_id'],
                        "sender": query["sender"],
                        "message": query["message"],
                        "timestamp": datetime.now().isoformat(), 
                        "ticket_id": query["ticket_id"]
                        }
        if 'human' in query.get("message", "").lower() or 'agent' in query.get("message", "").lower():
            message_info["event"] = "AGENT_MESSAGE"

            alert_info = {"session_id": payload['session_id'],
                        "event": "AGENT_MESSAGE",
                        "message": "The agent will contact you shortly. Please keep the patience.",
                        "sender": "Bot"}
            await manager.broadcast_session(alert_info)
        else:
            message_info["event"] = query["event"]

        if message_info.get("event" ) == "BOT_MESSAGE":
            bot_result, bot_message = kafka_producer.publish_message_to_ai_service(message_info)
            logger.info(f"Sucess: {bot_result} - Message: {bot_message}")
            result = True

        elif message_info.get("event") == "AGENT_MESSAGE":
            agent_result, agent_message = kafka_producer.publish_message_to_agent_service(message_info)
            logger.info(f"Sucess: {agent_result} - Message: {agent_message}") 
            result = True

        if message_info["sender"]!='System':
            db_result, db_message = kafka_producer.publish_message_to_db(message_info)
            logger.info(f"Sucess: {db_result} - Message: {db_message}") 
            result = True

        if result:
            message = "Published succesfully"

    except Exception as e:
        logger.exception("Saving chat history failed")
        message = str(e)
        result = False
    logger.info(f"Session Status: {result}, Message: {message}")
    return result, message


def get_chat_history(session_id, db):
    result = False
    chat_history_list = []
    try:
        chat_history = db.query(models.ChatHistory).filter(models.ChatHistory.session_id == session_id).order_by(models.ChatHistory.timestamp).all()
        for chat in chat_history:
            chat_dict = dict()
            chat_dict = {'sender': chat.sender.value,
                         'message': chat.message,
                         'timestamp': chat.timestamp
                         }
            chat_history_list.append(chat_dict)
        result = True
    except Exception as e:
        logger.exception(f"Fetching chat history failed {e}")
    return result, chat_history_list

def is_history_available(session_id, brand_id, db):
    try:
        return db.query(models.UserSession).filter(models.UserSession.id == session_id).filter(models.UserSession.tenant_brand_id == brand_id).first() is not None
    except Exception as e:
        logger.exception(f"Fetching chat history failed: {e}")
        return False




