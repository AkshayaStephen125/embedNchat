import os
import auth
import time
from fastapi import APIRouter, Depends, Response, Body
import tenant_services
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")



router = APIRouter(prefix="/api", tags=["Secure Chat"])

producer = KafkaProducer(bootstrap_servers="kafka:9092")


@router.post("/config")
def config(response: Response, tenant_brand = Depends(tenant_services.get_current_tenant_brand_by_api_key), db=Depends(tenant_services.get_db)):
    result, message, brand_config = tenant_services.get_chat_theme(tenant_brand, db)
    if result:
        return {
            'result' : result,
            "theme": brand_config,
        }
    else:
        return {
            'result' : result,
            "message": message
        }

@router.post("/init-token")
def init_token(response: Response, tenant_brand = Depends(tenant_services.get_current_tenant_brand_by_api_key)):
    token, refresh_token, access_expiry = tenant_services.create_session_token(tenant_brand)
    return {
        'token': token,
        'refresh_token': refresh_token,
        'access_expiry': access_expiry
    }


@router.post("/refresh-token")
def refresh_token(data: dict = Body(...)):
    token = data.get("refresh_token")
    print("token        ",token)
    if payload := auth.verify_token(token, "access"):
        print("payload---------------   ",payload)
        # session_id = payload["session_id"]
        # new_access = auth.create_access_token(payload)
        # new_refresh = auth.create_refresh_token(payload)

    return {
        "result": True,
        "access_token": 'test',
        # "refresh_token": new_refresh,
        # "access_expiry": int(time.time()) + 15
    }

