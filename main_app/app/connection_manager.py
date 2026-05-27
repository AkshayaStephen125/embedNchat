from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        self.session_connections: dict[str, list[WebSocket]] = {}
        self.tenant_connections: dict[int, list[WebSocket]] = {}

    async def connect_session(self, session_id: str, websocket: WebSocket):
        await websocket.accept()
        self.session_connections.setdefault(session_id, []).append(websocket)

    def disconnect_session(self, session_id: str, websocket: WebSocket):
        if session_id in self.session_connections:
            self.session_connections[session_id].remove(websocket)

            if not self.session_connections[session_id]:
                del self.session_connections[session_id]

    async def send_personal_message_session(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)

    async def broadcast_session(self, message_info: dict):
        if message_info['session_id'] not in self.session_connections:
            print(f"No active connections for session {message_info['session_id']}")
            return
        for connection in self.session_connections.get(message_info['session_id'], []):
            print(f"Broadcasting to session {message_info['session_id']}: {message_info}")
            await connection.send_json(message_info)

    async def connect_tenant(self, tenant_id: int, websocket: WebSocket):
        await websocket.accept()
        self.tenant_connections.setdefault(tenant_id, []).append(websocket)

    def disconnect_tenant(self, tenant_id: int, websocket: WebSocket):
        if tenant_id in self.tenant_connections:
            self.tenant_connections[tenant_id].remove(websocket)

            if not self.tenant_connections[tenant_id]:
                del self.tenant_connections[tenant_id]

    async def send_personal_message_tenant(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)

    async def broadcast_tenant(self, message_info: dict):
        print("coming")

        tenant_id = message_info['tenant_id']

        if tenant_id not in self.tenant_connections:
            print(f"No active connections for tenant {tenant_id}")
            return

        alive_connections = []

        for connection in self.tenant_connections.get(tenant_id, []):
            try:
                print(f"Broadcasting to tenant {tenant_id}: {message_info}")
                await connection.send_json(message_info)
                alive_connections.append(connection)
            except Exception as e:
                print("Removing dead connection:", e)

        self.tenant_connections[tenant_id] = alive_connections


manager = ConnectionManager()