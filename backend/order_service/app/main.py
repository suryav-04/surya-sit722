import asyncio
import json
import logging
import os
import sys
import time
from decimal import Decimal
from typing import List, Optional

import aio_pika
import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, joinedload

from .db import Base, SessionLocal, engine, get_db
from .models import Order, OrderItem
from .schemas import (
    OrderCreate,
    OrderItemResponse,
    OrderResponse,
    OrderStatusUpdate,
    OrderUpdate,
)

# --- Service URLs Configuration ---
PRODUCT_SERVICE_URL = os.getenv("PRODUCT_SERVICE_URL", "http://localhost:8002")
CUSTOMER_SERVICE_URL = os.getenv("CUSTOMER_SERVICE_URL", "http://localhost:8001")

# --- RabbitMQ Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

rabbitmq_connection: Optional[aio_pika.Connection] = None
rabbitmq_channel: Optional[aio_pika.Channel] = None
rabbitmq_exchange: Optional[aio_pika.Exchange] = None

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.INFO)

# --- FastAPI App Setup ---
app = FastAPI(
    title="Order Service API",
    description="Manages orders for mini-ecommerce app, with synchronous stock deduction.",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Root & Health Endpoints ---
@app.get("/", status_code=status.HTTP_200_OK, summary="Root endpoint")
async def read_root():
    return {"message": "Welcome to the Order Service!"}

@app.get("/health", status_code=status.HTTP_200_OK, summary="Health check endpoint")
async def health_check():
    return {"status": "ok", "service": "order-service"}


# --- RabbitMQ Helpers ---
async def connect_to_rabbitmq():
    global rabbitmq_connection, rabbitmq_channel, rabbitmq_exchange
    rabbitmq_url = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASS}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
    max_retries, retry_delay = 10, 5

    for i in range(max_retries):
        try:
            logger.info(f"Connecting to RabbitMQ (attempt {i+1}/{max_retries})...")
            rabbitmq_connection = await aio_pika.connect_robust(rabbitmq_url)
            rabbitmq_channel = await rabbitmq_connection.channel()
            rabbitmq_exchange = await rabbitmq_channel.declare_exchange(
                "ecomm_events", aio_pika.ExchangeType.DIRECT, durable=True
            )
            logger.info("Connected to RabbitMQ & declared 'ecomm_events' exchange.")
            return True
        except Exception as e:
            logger.warning(f"RabbitMQ connection failed: {e}")
            if i < max_retries - 1:
                await asyncio.sleep(retry_delay)
            else:
                logger.critical("RabbitMQ connection failed after retries.")
                return False
    return False

async def close_rabbitmq_connection():
    if rabbitmq_connection:
        logger.info("Closing RabbitMQ connection.")
        await rabbitmq_connection.close()

async def publish_event(routing_key: str, message_data: dict):
    if not rabbitmq_exchange:
        logger.error("RabbitMQ exchange not available.")
        return
    try:
        message_body = json.dumps(message_data).encode("utf-8")
        message = aio_pika.Message(
            body=message_body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await rabbitmq_exchange.publish(message, routing_key=routing_key)
        logger.info(f"Published event '{routing_key}': {message_data}")
    except Exception as e:
        logger.error(f"Failed to publish event '{routing_key}': {e}", exc_info=True)


# --- Startup / Shutdown ---
@app.on_event("startup")
async def startup_event():
    # Ensure DB tables exist
    max_retries, retry_delay = 10, 5
    for i in range(max_retries):
        try:
            Base.metadata.create_all(bind=engine)
            logger.info("Connected to PostgreSQL and ensured tables exist.")
            break
        except OperationalError as e:
            logger.warning(f"DB connection failed: {e}")
            if i < max_retries - 1:
                time.sleep(retry_delay)
            else:
                logger.critical("DB connection failed after retries.")
                sys.exit(1)
    if await connect_to_rabbitmq():
        asyncio.create_task(consume_stock_events(SessionLocal))

@app.on_event("shutdown")
async def shutdown_event():
    await close_rabbitmq_connection()


# --- Order Endpoints (create/list/get/update/delete) ---
@app.post("/orders/", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def create_order(order: OrderCreate, db: Session = Depends(get_db)):
    if not order.items:
        raise HTTPException(status_code=400, detail="Order must contain at least one item.")

    total_amount = sum(
        Decimal(str(item.quantity)) * Decimal(str(item.price_at_purchase))
        for item in order.items
    )

    db_order = Order(
        user_id=order.user_id,
        shipping_address=order.shipping_address,
        total_amount=total_amount,
        status="pending",
    )
    db.add(db_order)
    db.flush()

    for item in order.items:
        db_order_item = OrderItem(
            order_id=db_order.order_id,
            product_id=item.product_id,
            quantity=item.quantity,
            price_at_purchase=item.price_at_purchase,
            item_total=Decimal(str(item.quantity)) * Decimal(str(item.price_at_purchase)),
        )
        db.add(db_order_item)

    try:
        db.commit()
        db.refresh(db_order)
        db.refresh(db_order, attribute_names=["items"])
        await publish_event("order.placed", {
            "order_id": db_order.order_id,
            "user_id": db_order.user_id,
            "total_amount": float(db_order.total_amount),
            "items": [
                {
                    "product_id": i.product_id,
                    "quantity": i.quantity,
                    "price_at_purchase": float(i.price_at_purchase),
                }
                for i in db_order.items
            ],
            "order_date": db_order.order_date.isoformat(),
            "status": db_order.status,
        })
        return db_order
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating order: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Could not create order.")


@app.get("/orders/", response_model=List[OrderResponse])
def list_orders(
    db: Session = Depends(get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    user_id: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
):
    query = db.query(Order).options(joinedload(Order.items))
    if user_id:
        query = query.filter(Order.user_id == user_id)
    if status:
        query = query.filter(Order.status == status)
    return query.offset(skip).limit(limit).all()


@app.get("/orders/{order_id}", response_model=OrderResponse)
def get_order(order_id: int, db: Session = Depends(get_db)):
    order = db.query(Order).options(joinedload(Order.items)).filter(Order.order_id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@app.patch("/orders/{order_id}/status", response_model=OrderResponse)
async def update_order_status(order_id: int, new_status: OrderStatusUpdate, db: Session = Depends(get_db)):
    db_order = db.query(Order).filter(Order.order_id == order_id).first()
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")
    db_order.status = new_status.status
    try:
        db.add(db_order)
        db.commit()
        db.refresh(db_order)
        return db_order
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="Could not update order status.")


@app.delete("/orders/{order_id}", status_code=204)
def delete_order(order_id: int, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.order_id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    try:
        db.delete(order)
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="Error deleting order.")
    return Response(status_code=204)


@app.get("/orders/{order_id}/items", response_model=List[OrderItemResponse])
def get_order_items(order_id: int, db: Session = Depends(get_db)):
    order = db.query(Order).filter(Order.order_id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order.items
