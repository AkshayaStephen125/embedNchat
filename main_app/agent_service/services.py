import re
import auth
from fastapi.responses import RedirectResponse
from kafka_producer import publish_message_to_kafka, publish_message_to_db
from logger import logger
from shared import models
from sqlalchemy import desc


def sign_in_tenant_agent(tenant_data, db):
    result = False
    message = ''
    api_response = ''
    try:
        tenant_agent = db.query(models.TenantAgent).filter(models.TenantAgent.username == tenant_data.username).first()
        if not tenant_agent or not auth.verify_password(tenant_data.password, tenant_agent.hashed_password):
            message="Invalid credentials"
        else:
            access_token = auth.create_access_token({"agent_id": tenant_agent.id, "tenant_id": tenant_agent.tenant_id})
            refresh_token = auth.create_refresh_token({"agent_id": tenant_agent.id, "tenant_id": tenant_agent.tenant_id})

            tenant_agent.refresh_token = refresh_token
            db.commit()
            api_response = RedirectResponse(url="/", status_code=303)
            
            api_response.set_cookie(key="access_token", value=access_token, httponly=True, max_age=900, secure=False, samesite="lax" )

            api_response.set_cookie(key="refresh_token", value=refresh_token, httponly=True, max_age=604800, secure=False, samesite="lax" )
            result = True

    except Exception as e:
        logger.exception("Signup failed")
    return result, message, api_response

def set_to_publish(session_id, message_data):
    message_info = dict()
    result = False
    try:
        message_info = {"session_id": session_id,
                        "tenant_id": message_data["tenant_id"],
                        "tenant_brand_id": message_data["tenant_brand_id"],
                        "message": message_data["message"],
                        "sender": message_data["sender"],
                        "event": "AGENT_MESSAGE"
                        }
        result = publish_message_to_kafka(message_info)
        if message_info["sender"] == "Agent":
            result = publish_message_to_db(message_info)
    except Exception as e:
        logger.exception(f"Publishing failed: {e}")
    return result

def accept_agent_request(agent_id, agent_name, session_data, db):
    result = False
    message = ''
    message_info = dict()
    try:
        session_id = session_data.session_id
        agent_request = db.query(models.AgentChatHistory).filter(models.AgentChatHistory.session_id==session_id).filter(models.AgentChatHistory.is_accepted==False).first()
        if agent_request:
            agent_request.is_accepted = True
            agent_request.agent_id = agent_id
            db.commit()
            db.refresh(agent_request)

            message_info = {"message": f"Agent {agent_name} joined the chat.", 
                            "sender": "System",
                            "tenant_id": session_data.tenant_id,
                            "tenant_brand_id": session_data.tenant_brand_id
                            }
            result = set_to_publish(session_id, message_info)
            
            if result:
                message = "Request accepted"
        else:
            message = "Request Already Accepted"
    except Exception as e:
        message = "Accept agent request failed"
        logger.info(f"Result: {result}, Message: {message}, reason - {e}")
    return result, message

def get_user_sessions(tenant_id, db):
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

def get_user_sessions_from_history(agent_id, tenant_id, db):
    result = False
    message = ''
    session_list = list()
    try:
        chat_history =(
            db.query(models.AgentChatHistory.session_id)
            .join(
                models.UserSession,
                models.AgentChatHistory.session_id == models.UserSession.id
            )
            .filter(
                models.UserSession.tenant_id == tenant_id,
                models.AgentChatHistory.is_accepted == True,
                models.AgentChatHistory.agent_id == agent_id
            )
        )
        if chat_history.first():
            sessions = db.query(models.UserSession).filter(models.UserSession.tenant_id==tenant_id).filter(models.UserSession.id.in_(chat_history)).order_by(models.UserSession.id.desc())
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

def reformat_content(text: str) -> str:
    text = re.sub(r"\*\*(.*?)\*\*", r"\1", text)
    text = re.sub(r":\s*", ":\n\n", text, count=1)
    text = re.sub(r"(\d+)\.\s([^:]+):", r"\1. \2 –", text)
    text = re.sub(r"\s(\d+\.)", r"\n\1", text)
    return text.strip()

def verify_accepted_status(agent_id, session_id, db):
    result = False
    try:
        chat_request = db.query(models.AgentChatHistory).filter(models.AgentChatHistory.session_id==session_id).filter(models.AgentChatHistory.agent_id==agent_id).filter(models.AgentChatHistory.is_accepted==True).first()
        if chat_request:
            result = True
    except Exception as e:
        logger.exception(f"Fetchong chat history failed {e}")
    return result



def get_user_sessions_chats(session_id, db):
    result = False
    session_info = dict()
    session_chat_list = list()
    try:
        session = db.query(models.UserSession).filter(models.UserSession.id==session_id).first()
        session_info["tenant_id"] = session.tenant_id
        session_info["tenant_brand_id"] = session.tenant_brand_id

        if session:
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
    return result, session_info, session_chat_list
