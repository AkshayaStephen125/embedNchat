from fastapi import WebSocket, WebSocketDisconnect, APIRouter, Depends
from connection_manager import manager
import auth
from logger import logger
import tenant_services
from sqlalchemy.orm import Session
from db_config import get_db
import kafka_producer

router = APIRouter()



@router.websocket("/ws/chat")
async def websocket_endpoint(websocket: WebSocket, db: Session = Depends(get_db)):
    session_id = ''
    try:
        result = False
        message = ''
        token = websocket.query_params.get("token")
        if payload := auth.verify_token(token, "access"):
            session_id = payload["session_id"]
            await manager.connect(session_id, websocket)
        while True:
                query = await websocket.receive_json()
                payload.update({"sender": 'User', "message": query.get("message", ""), "event": query.get("event")})
                logger.info(f"payload      {payload}")
                if payload.get("event" ) == "BOT_MESSAGE":
                    bot_result, bot_message = kafka_producer.publish_message_to_ai_service(payload)
                    logger.info(f"Sucess: {bot_result} - Message: {bot_message}")                

                elif payload.get("event") == "AGENT_MESSAGE":
                    agent_result, agent_message = kafka_producer.publish_message_to_agent_service(payload)
                    logger.info(f"Sucess: {agent_result} - Message: {agent_message}") 
                result, message = kafka_producer.publish_message_to_db(payload)
                logger.info(f"Sucess: {result} - Message: {message}")                
    except WebSocketDisconnect:
        manager.disconnect(session_id, websocket)
        print("Client disconnected")

    except Exception as e:
        print("Error:", e)
        await websocket.close(code=1011)


@router.websocket("/ws/agent")
async def websocket_endpoint(websocket: WebSocket, db: Session = Depends(get_db)):
    session_id = ''
    try:
        session_id = websocket.query_params.get("session_id")
        await manager.connect(session_id, websocket)

        while True:
                message = await websocket.receive_json()
                q_result = tenant_services.save_message(session_id, message.get("message"), "Agent", db, 'Manual')
                    # else:
                    #     q_result = tenant_services.save_message(session_id, query.get("message"), "User", db)
                    #     if q_result:
                    #         relevant_chunks = rag.retrieve_relevant_chunks(int(tenant_id), int(tenant_brand_id), query.get("message"))
                    #         answer = rag.generate_answer(query, relevant_chunks)
                if q_result:
                    await manager.broadcast(session_id,
    {
        "message": message.get("message")
    }
)
                            # a_result = tenant_services.save_message(session_id, answer, "Bot", db)
                # await manager.broadcast(f"User says: {data}")
        # else:
        #     print("Nothing")
    except WebSocketDisconnect:
        manager.disconnect(session_id, websocket)
        print("Client disconnected")

    except Exception as e:
        logger.info(e)
        await websocket.close(code=1011)