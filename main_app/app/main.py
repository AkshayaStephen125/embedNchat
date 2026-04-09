import auth
import api
import ws_config
import schema
import tenant_services
import uuid
import asyncio
from kafka_producer import response_listener
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
from fastapi.middleware.cors import CORSMiddleware
from jinja2 import Environment, FileSystemLoader
import traceback

@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    print("Tables created!")
    asyncio.create_task(response_listener())
    print("Listened")
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*",],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(AuthMiddleware)

app.include_router(api.router)

app.include_router(ws_config.router)


app.mount("/static", StaticFiles(directory="static"), name="static")

import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory="templates")



@app.get("/", response_class=HTMLResponse)
async def index_page(request: Request):
    return templates.TemplateResponse(request, "index.html")




@app.get("/signin", response_class=HTMLResponse)
async def signin_page(request: Request):

    return templates.TemplateResponse(request,"signin.html")


@app.post("/signin", response_class=HTMLResponse)
async def signin_page(request: Request,  response: Response,
                      form_data: OAuth2PasswordRequestForm = Depends(),
                      db: Session = Depends(get_db)):
    result, message, api_response = tenant_services.sign_in_tenant(
        schema.TenantLogin(
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

    

@app.get("/signup", response_class=HTMLResponse)
async def signup_page(request: Request):
    return templates.TemplateResponse(request, "signup.html")

@app.post("/signup", response_class=HTMLResponse)
async def signup_page(request: Request,  db: Session = Depends(get_db),
    company_name: str = Form(...),
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...)
):
    result, message = tenant_services.tenant_sign_up(
        schema.TenantCreate(
            company_name=company_name,
            username=username,
            email=email,
            password=password
        ),
        db
    )

    if not result:
        return templates.TemplateResponse(
            request,
            "signup.html",
            {
                "message": message,
            }
        )
    else:
        return RedirectResponse(url="/signin", status_code=302)

@app.post("/refresh")
def refresh(request: Request, response: Response, db: Session = Depends(get_db)):
    tenant = request.state.tenant
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        raise HTTPException(status_code=401, detail="No refresh token")

    payload = auth.verify_token(refresh_token, "refresh")
    tenant_id = payload.get("tenant_id")

    result, api_reponse = tenant_services.refresh_tenant_token(refresh_token, tenant_id, response, db)
    if result:
        return api_reponse


@app.get("/dashboard", response_class=HTMLResponse, response_model=None)
async def dashboard(request: Request):
    tenant = request.state.tenant  

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "tenant": tenant.company_name
        }
    )

@app.get("/add_brand", response_class=HTMLResponse)
async def add_brand_get(request: Request):
    tenant = request.state.tenant
    return templates.TemplateResponse(request, "add_edit_brand.html", {"tenant": tenant.company_name})

@app.post("/add_brand", response_class=HTMLResponse)
async def add_brand_post(request: Request,  db: Session = Depends(get_db),
                    brand_name: str = Form(...),
                    logo: UploadFile = File(...),
                    background_color: str = Form(...),
                    header_color: str = Form(...),
                    user_message_color: str = Form(...),
                    bot_message_color: str = Form(...),
                    welcome_message: str = Form(...),
                    tone: str = Form(...)
                ):
    result, message = tenant_services.upload_brand_detail(request, logo, schema.BrandCreate(
        brand_name=brand_name,
        logo=logo.filename,
        background_color=background_color,
        header_color=header_color,
        user_message_color=user_message_color,
        bot_message_color=bot_message_color,
        welcome_message=welcome_message,
        tone=tone
    ), db)
    
    tenant = request.state.tenant

    if not result:
        return templates.TemplateResponse(
            request,
            "add_edit_brand.html",
            {
                "tenant": tenant.company_name,
                "message": message
            }
        )
    else:
        return RedirectResponse(url="/brands", status_code=302)


@app.get("/upload", response_class=HTMLResponse)
async def upload_page(request: Request, db:Session=Depends(get_db)):
    tenant = request.state.tenant
    result, message, brand_data = tenant_services.brand_options(tenant.tenant_id, db)
    return templates.TemplateResponse(
        request,
        "upload.html",
        {
            "tenant_name": tenant.company_name,
            "brands": brand_data
        }
    )

@app.post("/upload", response_class=HTMLResponse)
async def upload_page(request: Request, 
                    name: str = Form(...),
                    brand_ids: List[int] = Form(...),
                    file: UploadFile = File(...),
                    db: Session = Depends(get_db)):
    tenant = request.state.tenant
    result, message, file_location = tenant_services.tenant_file_upload(request, name, brand_ids, file, db)
    if result:
        result = tenant_services.publish_file_to_kafka(tenant.tenant_id, file_location, brand_ids)
    return templates.TemplateResponse(
        request,
        "upload.html",
        {
            "tenant_name": tenant
        }
    )

@app.get("/brands", response_class=HTMLResponse)
async def brands_page(request: Request, db=Depends(get_db)):
    tenant = request.state.tenant
    result, message, brand_data = tenant_services.get_brands(request, db)  
    return templates.TemplateResponse(
        request,
        "brands.html",
        {
            "tenant": tenant,
            "brands": brand_data
        }
    )

@app.get("/brand/{brand_id}", response_class=HTMLResponse)
async def brand_detail_page(brand_id: int, request: Request, db=Depends(get_db)):
    tenant = request.state.tenant
    result, message, brand_data = tenant_services.get_brand_detail(brand_id, db)
    return templates.TemplateResponse(
        request,
        "add_edit_brand.html",
        {
            "tenant": tenant,
            "brand": brand_data
        }
    )

@app.get("/sessions", response_class=HTMLResponse)
async def sessions_page(request: Request, db=Depends(get_db)):
    tenant = request.state.tenant
    result, session_list = tenant_services.get_user_sessions(tenant.tenant_id, db)
    if result:  
        return templates.TemplateResponse(
            request,
            "sessions.html",
            {
                "tenant": tenant,
                "sessions": session_list
            }
        )
    
    
@app.get("/sessions/{session_id}", response_class=HTMLResponse)
async def session_detail_page(session_id: uuid.UUID, request: Request, db=Depends(get_db)):
    tenant = request.state.tenant
    result, session_data = tenant_services.get_user_sessions_chats(session_id, db)
    if result:
        return templates.TemplateResponse(
            request,
            "session.html",
            {
                "tenant": tenant,
                "session_id": session_id,
                "messages": session_data
            }
        )
    
@app.get("/chat-requests", response_class=HTMLResponse)
async def sessions_page(request: Request, db=Depends(get_db)):
    tenant = request.state.tenant
    result, message, session_list = tenant_services.get_user_sessions_requests(tenant.tenant_id, db)
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
                "tenant": "tenant.company_name",
                "message": message
            }
        )

@app.post("/brand/{brand_id}", response_class=HTMLResponse)
async def brand_detail_page(brand_id: int, request: Request, db=Depends(get_db),
                    brand_name: str = Form(...),
                    logo: UploadFile = File(None),
                    background_color: str = Form(...),
                    header_color: str = Form(...),
                    user_message_color: str = Form(...),
                    bot_message_color: str = Form(...),
                    welcome_message: str = Form(...),
                    tone: str = Form(...)
                ):
    tenant = request.state.tenant
    result, message = tenant_services.edit_brand_detail(request, brand_id, schema.BrandEdit(
        brand_id=brand_id,
        brand_name=brand_name,
        logo=logo.filename,
        background_color=background_color,
        header_color=header_color,
        user_message_color=user_message_color,
        bot_message_color=bot_message_color,
        welcome_message=welcome_message,
        tone=tone
    ), db, logo)

    if not result:
        return templates.TemplateResponse(
            request,
            "add_edit_brand.html",
            {
                "tenant": tenant,
                "message": message
            }
        )
    else:
        return RedirectResponse(url="/brands", status_code=302)
    
@app.post("/brand_delete", response_class=HTMLResponse)
async def delete_brand(request: Request, db=Depends(get_db), brand_id: str = Form(...)):
    tenant = request.state.tenant
    result, message = tenant_services.delete_brand(brand_id, db)
    if not result:
        return templates.TemplateResponse(
            request,
            "brands.html",
            {
                "tenant": tenant,
                "message": message
            }
        )
    else:
        return RedirectResponse(url="/brands", status_code=302)
    

@app.get("/agents", response_class=HTMLResponse)
async def agents_page(request: Request,  db: Session = Depends(get_db)):
    result, message, agents = tenant_services.get_tenant_agents(request, db)
    return templates.TemplateResponse(request, "tenant_agents.html", {"result":result, "agents":agents, "message":message})

@app.get("/agents/create", response_class=HTMLResponse)
async def create_agent_page(request: Request):
   return templates.TemplateResponse(request, "create_agent.html")


@app.post("/agents/create")
async def create_agent(request: Request,  db: Session = Depends(get_db),
    name: str = Form(...),
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...)
):
    tenant = request.state.tenant
    result, message = tenant_services.create_tenant_agent(
        tenant.tenant_id,
        schema.TenantAgentCreate(
            name=name,
            username=username,
            email=email,
            password=password
        ),
        db
    )
    if result:
        return RedirectResponse(url="/agents", status_code=303)
    else:
        return templates.TemplateResponse(request, "create_agent.html", {"message": message})
    

@app.get("/agents/{id}", response_class=HTMLResponse)
async def agent_detail_page(request: Request, id: int):
    return templates.TemplateResponse(request, "agent_detail.html", {"request": request})

@app.get("/agents/edit/{id}", response_class=HTMLResponse)
async def edit_agent_page(id:int, request: Request,db: Session = Depends(get_db)):
    tenant = request.state.tenant
    result, message, agent_data = tenant_services.get_tenant_agent(
        tenant.tenant_id, id, db
       )
    return templates.TemplateResponse(request, "edit_agent.html", {"agent": agent_data})

@app.post("/agents/edit/{id}", response_class=HTMLResponse)
async def edit_agent_page(request: Request,db: Session = Depends(get_db),
    agent_id: int = Form(...),
    name: str = Form(...),
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...)
):
    tenant = request.state.tenant
    result, message = tenant_services.edit_tenant_agent(
        tenant.tenant_id,
        schema.TenantAgentEdid(
            id=agent_id,
            name=name,
            username=username,
            email=email,
            password=password
        ),
        db
    )
    if result:
        return RedirectResponse(url="/agents", status_code=303)
    else:
        return templates.TemplateResponse(request, "edit_agent.html", {"request": request})


