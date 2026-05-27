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
async def index_page(request: Request, db: Session = Depends(get_db)):
    tenant = request.state.tenant
    result, dashboard_data = services.get_dashboard_data(request.state.tenant.tenant_id, request.state.tenant_agent.id, db)
    dashboard_data.update({"tenant_name": tenant.company_name})
    return templates.TemplateResponse(request, "index.html", dashboard_data)

@app.get("/api/tickets-per-day")
async def messages_per_day(request: Request, db: Session = Depends(get_db)):
    result, ticket_data = services.tickets_per_day(request.state.tenant.tenant_id, request.state.tenant_agent.id, db)
    return ticket_data

@app.get("/api/ticket-status-distribution")
async def ticket_status_distribution(request: Request, db: Session = Depends(get_db)):
    result, ticket_data = services.ticket_status_distribution(request.state.tenant.tenant_id, request.state.tenant_agent.id, db)
    return ticket_data


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

@app.get("/chat-requests/{status}", response_class=HTMLResponse)
async def sessions_page(status:str, request: Request, db=Depends(get_db)):
    tenant = request.state.tenant
    result, message, brand_data = services.get_brands(request, db) 

    result, message, ticket_list = services.get_tickets(tenant.tenant_id, status, db)
    if result:  
        return templates.TemplateResponse(
            request,
            "chat_requests.html",
            {
                "tenant": tenant.company_name,
                "status": status,
                "brands": brand_data,
                "tickets": ticket_list
            }
        )
    
@app.get("/tickets/{ticket_id}", response_class=HTMLResponse)
async def session_detail_page(ticket_id: int, request: Request, db=Depends(get_db)):
    tenant = request.state.tenant
    result, session_data, session_message_list, ticket_status = services.get_user_sessions_chats(ticket_id, db)
    if result:
        return templates.TemplateResponse(
            request,
            "chat_session.html",
            {
                "tenant": tenant.company_name,
                "ticket_id": ticket_id,
                "ticket_status": ticket_status,
                "session_data": session_data,
                "messages": session_message_list,
            }
        )
    
@app.get("/profile", response_class=HTMLResponse)
async def profile_page(request: Request):
    tenant = request.state.tenant
    tenant_agent = request.state.tenant_agent
    return templates.TemplateResponse(
                request,
                "profile.html",
                {'tenant': tenant,
                 'tenant_agent': tenant_agent}
            )

@app.post("/accept-request")
async def accept_request(data: schema.AcceptRequest, request: Request, db=Depends(get_db)):
    tenant_agent = request.state.tenant_agent
    result, message = services.accept_agent_request(tenant_agent.id, tenant_agent.agent_name, data, db)
    return {
        "result": result,
        "message": message
    }

@app.post("/close-request")
async def accept_request(data: schema.AcceptRequest, request: Request, db=Depends(get_db)):
    tenant_agent = request.state.tenant_agent
    result, message = services.close_agent_request(tenant_agent.id, tenant_agent.agent_name, data, db)
    return {
        "result": result,
        "message": message
    }