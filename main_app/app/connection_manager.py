from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, session_id: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.setdefault(session_id, []).append(websocket)

    def disconnect(self, session_id: str, websocket: WebSocket):
        if session_id in self.active_connections:
            self.active_connections[session_id].remove(websocket)

            if not self.active_connections[session_id]:
                del self.active_connections[session_id]

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)

    async def broadcast(self, message_info: dict):
        if message_info['session_id'] not in self.active_connections:
            print(f"No active connections for session {message_info['session_id']}")
            return
        for connection in self.active_connections.get(message_info['session_id'], []):
            print(f"Broadcasting to session {message_info['session_id']}: {message_info}")
            await connection.send_json(message_info)


manager = ConnectionManager()