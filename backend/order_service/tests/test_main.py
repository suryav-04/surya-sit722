import logging
import time
from unittest.mock import AsyncMock, patch

import pytest
from app.db import SessionLocal, engine, get_db
from app.main import app   # ✅ only import app now
from app.models import Base

from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

# Suppress noisy logs from SQLAlchemy/FastAPI/Uvicorn during tests
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
logging.getLogger("fastapi").setLevel(logging.WARNING)
logging.getLogger("app.main").setLevel(logging.WARNING)


# --- Pytest Fixtures ---
@pytest.fixture(scope="session", autouse=True)
def setup_database_for_tests():
    """Ensure test DB tables are dropped and recreated before tests."""
    max_retries = 10
    retry_delay_seconds = 3
    for i in range(max_retries):
        try:
            Base.metadata.drop_all(bind=engine)
            Base.metadata.create_all(bind=engine)
            break
        except OperationalError as e:
            logging.warning(f"DB connection failed: {e}. Retrying...")
            time.sleep(retry_delay_seconds)
            if i == max_retries - 1:
                pytest.fail(f"Could not set up test DB: {e}")

    yield


@pytest.fixture(scope="function")
def db_session_for_test():
    """Provide a fresh DB session for each test."""
    connection = engine.connect()
    transaction = connection.begin()
    db = SessionLocal(bind=connection)

    def override_get_db():
        yield db

    app.dependency_overrides[get_db] = override_get_db

    try:
        yield db
    finally:
        transaction.rollback()
        db.close()
        connection.close()
        app.dependency_overrides.pop(get_db, None)


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(scope="function")
def mock_httpx_client():
    """Mock external HTTP requests (to Customer/Product services)."""
    with patch("app.main.httpx.AsyncClient") as mock_async_client_cls:
        mock_client_instance = AsyncMock()
        mock_async_client_cls.return_value.__aenter__.return_value = mock_client_instance
        yield mock_client_instance


# --- Basic Service Tests ---
def test_read_root(client: TestClient):
    """Test the root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the Order Service!"}


def test_health_check(client: TestClient):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "order-service"}
