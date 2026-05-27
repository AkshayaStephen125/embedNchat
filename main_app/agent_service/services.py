import re
import auth
from datetime import datetime
from fastapi.responses import RedirectResponse
from kafka_producer import publish_message_to_kafka, publish_message_to_db
from sqlalchemy import func
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

def get_dashboard_data(tenant_id, agent_id, db):
    result = False
    dashboard_info = dict()
    try:
        open_tickets = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.tenant_id==tenant_id).filter(models.AgentChatTickets.status=='Open').count()
        active_tickets = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.tenant_id==tenant_id).filter(models.AgentChatTickets.agent_id==agent_id).filter(models.AgentChatTickets.status=='Inprogress').count()
        closed_tickets = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.tenant_id==tenant_id).filter(models.AgentChatTickets.agent_id==agent_id).filter(models.AgentChatTickets.status=='Closed').count()


        tickets_per_brand = db.query(models.TenantBrand.id.label("brand_id"),models.TenantBrand.brand_name,models.TenantBrand.logo,func.count(models.AgentChatTickets.ticket_id).label("ticket_count")
                        ).outerjoin(models.UserSession,models.UserSession.tenant_brand_id == models.TenantBrand.id
                        ).outerjoin(models.AgentChatTickets,models.AgentChatTickets.session_id == models.UserSession.id
                        ).group_by(models.TenantBrand.id,models.TenantBrand.brand_name,models.TenantBrand.logo
                        ).filter(models.TenantBrand.tenant_id==tenant_id).filter(models.TenantBrand.is_active==True)
        
        recent_tickets = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.tenant_id==tenant_id).filter(models.AgentChatTickets.agent_id==agent_id).order_by(desc(models.AgentChatTickets.ticket_id))[:3]
        ticket_list = list()
        for ticket in recent_tickets:
                session_dict = dict()
                last_message = db.query(models.ChatHistory).filter(models.ChatHistory.ticket_id == ticket.ticket_id).order_by(desc(models.ChatHistory.timestamp)).first()
                if last_message:
                    session_dict = {'ticket_id': ticket.ticket_id,
                                    'last_message': last_message.message,
                                    'status': ticket.status.value,
                                    'created_at': ticket.created_at.date().strftime("%Y-%m-%d")
                                    }
                    ticket_list.append(session_dict)

        recent_open_tickets = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.tenant_id==tenant_id).filter(models.AgentChatTickets.status=='Open').order_by(desc(models.AgentChatTickets.ticket_id))[:3]
        open_ticket_list = list()
        for ticket in recent_open_tickets:
                session_dict = dict()
                last_message = db.query(models.ChatHistory).filter(models.ChatHistory.ticket_id == ticket.ticket_id).order_by(desc(models.ChatHistory.timestamp)).first()
                if last_message:
                    session_dict = {'ticket_id': ticket.ticket_id,
                                    'last_message': last_message.message,
                                    'status': ticket.status.value,
                                    'created_at': ticket.created_at.date().strftime("%Y-%m-%d")
                                    }
                    open_ticket_list.append(session_dict)

        dashboard_info ={"recent_tickets":ticket_list,         
                          "total": open_tickets + active_tickets + closed_tickets,
                          "open": open_tickets,
                          "active": active_tickets,
                          "closed": closed_tickets,
                          "tickets_per_brand": tickets_per_brand,
                          "recent_open_tickets": open_ticket_list
                          }
        result = True
    except Exception as e:
        logger.exception(f"Dashboard info fetch failed {e}")
    return result, dashboard_info

def tickets_per_day(tenant_id, agent_id, db):
    result = False
    ticket_data = dict()
    try:
        tickets_per_day = (
            db.query(
                func.date(models.AgentChatTickets.closed_time).label("date"),
                func.count(models.AgentChatTickets.ticket_id).label("count")
            )
            .filter(models.AgentChatTickets.tenant_id == tenant_id)
            .filter(models.AgentChatTickets.agent_id == agent_id)
            .filter(models.AgentChatTickets.status == 'Closed')
            .group_by(func.date(models.AgentChatTickets.closed_time))
            .all()
        )
        lables = list()
        counts = list()
        for row in tickets_per_day:
            lables.append(row.date.strftime("%Y-%m-%d"))
            counts.append(row.count)
        ticket_data = {"labels": lables,
                           "counts": counts
                           }
        result = True
    except Exception as e:
        logger.exception(f"Dashboard info fetch failed {e}")
    return result, ticket_data

def ticket_status_distribution(tenant_id, agent_id, db):
    result = False
    ticket_data = dict()
    try:
        open_tickets = (
            db.query(models.AgentChatTickets)
            .filter(models.AgentChatTickets.tenant_id == tenant_id)
            .filter(models.AgentChatTickets.status == 'Open')
            .count())
        active_tickets = (
            db.query(models.AgentChatTickets)
            .filter(models.AgentChatTickets.tenant_id == tenant_id)
            .filter(models.AgentChatTickets.agent_id == agent_id)
            .filter(models.AgentChatTickets.status == 'Inprogress')
            .count())
        closed_tickets = (
            db.query(models.AgentChatTickets)
            .filter(models.AgentChatTickets.tenant_id == tenant_id)
            .filter(models.AgentChatTickets.agent_id == agent_id)
            .filter(models.AgentChatTickets.status == 'Closed')
            .count())
        ticket_data = {"labels":  ["Open", "Active", "Closed"],
                           "counts": [open_tickets, active_tickets, closed_tickets]
                           }
        result = True
    except Exception as e:
        logger.exception(f"Dashboard info fetch failed {e}")
    return result, ticket_data

def set_to_publish(session_id, message_data):
    message_info = dict()
    result = False
    try:
        message_info = {"type": "message",
                        "session_id": session_id,
                        "tenant_id": message_data["tenant_id"],
                        "tenant_brand_id": message_data["tenant_brand_id"],
                        "message": message_data["message"],
                        "sender": message_data["sender"],
                        "event": "AGENT_MESSAGE",
                        "timestamp": datetime.now().isoformat()
                        }
        if message_info["sender"] == "Agent":
            message_info["ticket_id"] = message_data["ticket_id"]
            result = publish_message_to_db(message_info)
        result = publish_message_to_kafka(message_info)
    except Exception as e:
        logger.exception(f"Publishing failed: {e}")
    return result

def get_brands(request, db):
    result = False
    message = ''
    brand_data = list()
    try:
        tenant_id = request.state.tenant.tenant_id
        brands = db.query(models.TenantBrand).filter(models.TenantBrand.tenant_id==tenant_id).filter(models.TenantBrand.is_active==True)
        for brand in brands:
            data = dict()
            data = {'id':brand.id,
                        'brand_name':brand.brand_name,
                        'logo':brand.logo}
            brand_data.append(data)
            result = True
    except Exception as e:
        logger.exception("Getting brands failed")
        message = str(e)
    return result, message, brand_data

def accept_agent_request(agent_id, agent_name, session_data, db):
    result = False
    message = ''
    message_info = dict()
    try:
        ticket_id = session_data.ticket_id
        session_id = session_data.session_id

        agent_request = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.ticket_id==ticket_id).filter(models.AgentChatTickets.status=='Open').first()
        if agent_request:
            agent_request.status = 'Inprogress'
            agent_request.agent_id = agent_id
            agent_request.accepted_time = datetime.now()
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

def close_agent_request(agent_id, agent_name, session_data, db):
    result = False
    message = ''
    message_info = dict()
    try:
        ticket_id = session_data.ticket_id
        session_id = session_data.session_id

        agent_request = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.ticket_id==ticket_id).filter(models.AgentChatTickets.status=='Inprogress').first()
        if agent_request:
            agent_request.status = 'Closed'
            agent_request.agent_id = agent_id
            agent_request.closed_time = datetime.now()
            db.commit()
            db.refresh(agent_request)

            message_info = {"message": f"Agent {agent_name} left the chat.", 
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

def get_tickets(tenant_id, status, db):
    result = False
    message = ''
    ticket_list = list()
    try:
        status_info = {'open': 'Open',
                       'active': 'Inprogress',
                       'closed': 'Closed'}
        open_tickets = db.query(models.AgentChatTickets).filter(models.AgentChatTickets.tenant_id==tenant_id).filter(models.AgentChatTickets.status==status_info[status]).order_by(desc(models.AgentChatTickets.ticket_id)).all()
        if open_tickets:
            for ticket in open_tickets:
                session_dict = dict()
                last_message = db.query(models.ChatHistory).filter(models.ChatHistory.ticket_id == ticket.ticket_id).order_by(desc(models.ChatHistory.timestamp)).first()
                session = db.query(models.UserSession).filter(models.UserSession.id==ticket.session_id).first()
                tenant_brand_id = db.query(models.TenantBrand).filter(models.TenantBrand.id==session.tenant_brand_id).first()
                if last_message:
                    session_dict = {'ticket_id': ticket.ticket_id,
                                    'tenant_brand_id': tenant_brand_id.id,
                                    'last_message': last_message.message,
                                    'created_at': ticket.created_at
                                    }
                    ticket_list.append(session_dict)
            result = True
        else:
            message = "No sessions found"
    except Exception as e:
        db.rollback()
        logger.exception(f"Fetching chat sessions failed {e}")
    return result, message, ticket_list




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
        logger.exception(f"Fetching chat history failed {e}")
    return result



def get_user_sessions_chats(ticket_id, db):
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
