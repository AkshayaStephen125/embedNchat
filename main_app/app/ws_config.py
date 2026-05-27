from fastapi import WebSocket, WebSocketDisconnect, APIRouter, Depends
from connection_manager import manager
import auth
from logger import logger
import tenant_services
from sqlalchemy.orm import Session
from db_config import get_db

router = APIRouter()



@router.websocket("/ws/chat")
async def websocket_endpoint(websocket: WebSocket):
    session_id = ''
    try:
        result = False
        message = ''
        token = websocket.query_params.get("token")
        if payload := auth.verify_token(token, "access"):
            session_id = payload["session_id"]
            await manager.connect_session(session_id, websocket)
        while True:
                query = await websocket.receive_json()
                result, message =  await tenant_services.publish_to_services(payload, query, manager)
                logger.info(f"Sucess: {result} - Message: {message}")                
    except WebSocketDisconnect:
        manager.disconnect_session(session_id, websocket)
        print("Client disconnected")

    except Exception as e:
        print("Error:", e)
        await websocket.close(code=1011)

@router.websocket("/ws/notify")
async def websocket_endpoint(websocket: WebSocket):
    tenant_id = websocket.query_params.get("tenant_id")
    try:
        await manager.connect_tenant(int(tenant_id), websocket)
        while True:
                query = await websocket.receive_json()
    except WebSocketDisconnect:
        manager.disconnect_tenant(tenant_id, websocket)
        print("Client disconnected")

    except Exception as e:
        print("Error:", e)
        await websocket.close(code=1011)
