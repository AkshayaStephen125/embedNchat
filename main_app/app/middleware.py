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

        public_exact_paths = {"/", "/signin", "/signup"}
        public_prefix_paths = ("/static", "/ws", "/api")

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
            tenant_id = payload.get("tenant_id")

            db: Session = SessionLocal()

            tenant = db.query(models.Tenant).filter(models.Tenant.tenant_id == tenant_id).first()

            db.close()

            if not tenant:
                return self._redirect_to_login(request)

            request.state.tenant = tenant

        except Exception:
            logger.exception("Middleware auth failed")
            return self._redirect_to_login(request)

        return await call_next(request)

    def _redirect_to_login(self, request: Request):
        return RedirectResponse(url="/signin")
