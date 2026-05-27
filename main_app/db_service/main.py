import os
import json
import time
import uuid
from datetime import datetime
from shared import models
from logger import logger
from dotenv import load_dotenv
from db_config import get_db
from kafka import KafkaConsumer


load_dotenv()

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
STORE_MESSAGE_KAFKA_TOPIC = os.environ.get("STORE_MESSAGE_KAFKA_TOPIC")


def get_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                            STORE_MESSAGE_KAFKA_TOPIC, 
                            bootstrap_servers=KAFKA_BROKER_URL,
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                            auto_offset_reset='earliest',
                            enable_auto_commit=True
                        )
            return consumer
        except:
            print("Waiting for Kafka...")
            time.sleep(5)



consumer = get_consumer()



def save_message():
    logger.info("Listening on store_message_topic")
    db = next(get_db())

    result=False
    message_query = dict()
    ticket_id = None
    try:
        for message_session in consumer:
            message_query=message_session.value

            logger.info(f'Received from topic STORE_MESSAGE_KAFKA_TOPIC - {message_query}')
            session_id = message_query['session_id']
            tenant_id = message_query['tenant_id']
            tenant_brand_id = message_query['tenant_brand_id']
            sender = message_query['sender']
            event = message_query['event']
            message = message_query['message']
            timestamp = datetime.fromisoformat(message_query['timestamp'])

            result = save_user_session(db, session_id, tenant_id, tenant_brand_id)
            logger.info(f"Session Upload Status: {result}")
            if result:
                if event=='BOT_MESSAGE':
                    save_to_db(session_id, tenant_id, message, sender, timestamp, db)
                    if sender=='Bot':
                        token_used = message_query.get('token_used', 0)
                        if token_used:
                            add_token_usage(session_id, tenant_id, timestamp, token_used)
                else:
                    ticket_id = message_query.get('ticket_id','')
                    if not ticket_id:
                        ticket_id = add_agent_ticket(tenant_id, session_id, db)
                    d_result = save_to_db(session_id, tenant_id, message, sender, timestamp, db, 'Manual', ticket_id)
        result=True
    except Exception as e:
        logger.exception(f"Error in recieving from kafka : {e}")
    return result


def save_user_session(db, session_id, tenant_id, tenant_brand_id):
    result = False
    message = ''
    try:
        db = next(get_db())
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


def save_to_db(session_id, tenant_id, message, sender, timestamp, db, takeover='Auto',ticket_id=None):
    result = False,
    try:
        logger.info(f'sender - {sender}, takeover - {takeover}, ticket_id - {ticket_id}')
        chat_history = models.ChatHistory(
            session_id=uuid.UUID(session_id),
            tenant_id=tenant_id,
            message=message,
            sender=sender,
            takeover=takeover,
            timestamp=timestamp,
            ticket_id=ticket_id
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

def add_agent_ticket(tenant_id, session_id, db):
    result = False
    message = ''
    ticket_id = ''
    try:
        ticket = models.AgentChatTickets(
        tenant_id=tenant_id,
        session_id=uuid.UUID(session_id))
        db.add(ticket)
        db.commit()
        db.refresh(ticket)
        ticket_id = ticket.ticket_id
        result = True
        message = 'Ticket saved'
    except Exception as e:
        db.rollback()
        logger.exception(f"Saving ticket failed: {e}")
    logger.info(f"Ticket Status: {result}, Message: {message}")
    return ticket_id

def add_token_usage(session_id, tenant_id, timestamp, token_count):
    try:
        db = next(get_db())
        if token_usage := db.query(models.TockenUsage).filter(models.TockenUsage.session_id == session_id and models.TockenUsage.date == timestamp.date()).first():
            token_usage.token_count += token_count
            db.commit()
            db.refresh(token_usage)
        else:
            token_usage = models.TockenUsage(
                session_id=uuid.UUID(session_id),
                tenant_id=tenant_id,
                date=timestamp.date(),
                token_count=token_count
            )
            db.add(token_usage)
            db.commit()
            db.refresh(token_usage)
    except Exception as e:
        db.rollback()
        logger.exception(f"Saving token usage failed: {e}")



if __name__ == "__main__":
    save_message()

