from fastapi import WebSocket, WebSocketDisconnect, APIRouter
from connection_manager import manager
import auth
from logger import logger
import services
from db_config import get_db
import kafka_producer

router = APIRouter()


@router.websocket("/ws/agent")
async def websocket_endpoint(websocket: WebSocket):
    session_id = ''
    try:
        session_id = websocket.query_params.get("session_id")
        await manager.connect(session_id, websocket)
        
        while True:
                message_data = await websocket.receive_json()
                services.set_to_publish(session_id, message_data)
    except WebSocketDisconnect:
        manager.disconnect(session_id, websocket)

    except Exception as e:
        logger.info(e)
        await websocket.close(code=1011)