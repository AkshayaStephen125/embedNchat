import ws_config
import services
import uuid
import asyncio
import schema
from kafka_listener import response_listener
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI, Request, UploadFile, File, Form, Response, Depends, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import OAuth2PasswordRequestForm
from db_config import Base, engine, get_db
from middleware import AuthMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(response_listener())
    print("Listened")
    yield

app = FastAPI(lifespan=lifespan)


app.add_middleware(AuthMiddleware)

app.include_router(ws_config.router)


app.mount("/static", StaticFiles(directory="static"), name="static")

import os



templates = Jinja2Templates(directory="templates")

templates.env.auto_reload = True


@app.get("/", response_class=HTMLResponse)
async def index_page(request: Request):
    return templates.TemplateResponse(request, "index.html", {"request" : request})


@app.get("/signin", response_class=HTMLResponse)
async def signin_page(request: Request):
    return templates.TemplateResponse(request,"signin.html")

@app.post("/signin", response_class=HTMLResponse)
async def signin_page(request: Request,  response: Response,
                      form_data: OAuth2PasswordRequestForm = Depends(),
                      db: Session = Depends(get_db)):
    result, message, api_response = services.sign_in_tenant_agent(
        schema.TenantAgentLogin(
            username=form_data.username,
            password=form_data.password
        ),
        db
    )
    if result:
        return api_response
    else:
         return templates.TemplateResponse(
        request,     
        "signin.html",
        {
            "message": message
        }
    )

@app.get("/chat-requests", response_class=HTMLResponse)
async def sessions_page(request: Request, db=Depends(get_db)):
    tenant = request.state.tenant
    result, message, session_list = services.get_user_sessions(tenant.tenant_id, db)
    if result:  
        return templates.TemplateResponse(
            request,
            "chat_requests.html",
            {
                "tenant": tenant.company_name,
                "sessions": session_list
            }
        )
    else:
        return templates.TemplateResponse(
            request,
            "chat_requests.html",
            {
                "tenant": tenant.company_name,
                "message": message
            }
        )
    
@app.get("/chat-history", response_class=HTMLResponse)
async def sessions_page(request: Request, db=Depends(get_db)):
    tenant = request.state.tenant
    tenant_agent = request.state.tenant_agent

    result, message, session_list = services.get_user_sessions_from_history(tenant_agent.id, tenant.tenant_id, db)
    if result:  
        return templates.TemplateResponse(
            request,
            "chat_history.html",
            {
                "tenant": tenant.company_name,
                "sessions": session_list
            }
        )
    else:
        return templates.TemplateResponse(
            request,
            "chat_requests.html",
            {
                "tenant": tenant.company_name,
                "message": message
            }
        )
    
@app.get("/sessions/{session_id}", response_class=HTMLResponse)
async def session_detail_page(session_id: uuid.UUID, request: Request, db=Depends(get_db)):
    tenant = request.state.tenant
    is_accepted = services.verify_accepted_status(request.state.tenant_agent.id, session_id, db)
    result, session_data, session_message_list = services.get_user_sessions_chats(session_id, db)
    if result:
        return templates.TemplateResponse(
            request,
            "chat_session.html",
            {
                "tenant": tenant,
                "session_id": session_id,
                "is_accepted": is_accepted,
                "session_data": session_data,
                "messages": session_message_list,
            }
        )
    
@app.get("/profile", response_class=HTMLResponse)
async def profile_page(request: Request):
    return templates.TemplateResponse(
        request,
        "profile.html"
    )

@app.post("/accept-request")
async def accept_request(data: schema.AcceptRequest, request: Request, db=Depends(get_db)):
    tenant_agent = request.state.tenant_agent
    result, message = services.accept_agent_request(tenant_agent.id, tenant_agent.agent_name, data, db)
    return {
        "result": result,
        "message": message
    }