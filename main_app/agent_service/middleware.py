from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.responses import RedirectResponse
from fastapi import Request
from sqlalchemy.orm import Session
from db_config import SessionLocal
import auth
from shared import models
from logger import logger


class AuthMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next):

        public_exact_paths = {"/signin"}
        public_prefix_paths = ("/static")

        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path

        if path in public_exact_paths:
            return await call_next(request)

        if path.startswith(public_prefix_paths):
            return await call_next(request)
        
        token = request.cookies.get("access_token")


        if not token:
            return self._redirect_to_login(request)

        try:
            payload = auth.verify_token(token, "access")
            agent_id = payload.get("agent_id")
            tenant_id = payload.get("tenant_id")

            db: Session = SessionLocal()

            tenant_agent = db.query(models.TenantAgent).filter(models.TenantAgent.id == agent_id).first()
            tenant = db.query(models.Tenant).filter(models.Tenant.tenant_id == tenant_id).first()

            db.close()

            if not tenant_agent:
                return self._redirect_to_login(request)

            request.state.tenant_agent = tenant_agent
            request.state.tenant = tenant


        except Exception:
            logger.exception("Middleware auth failed")
            return self._redirect_to_login(request)

        return await call_next(request)

    def _redirect_to_login(self, request: Request):
        return RedirectResponse(url="/signin")
