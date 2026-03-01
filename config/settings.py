"""
環境変数・設定の一元管理
========================
全モジュールはここから設定を取得する。
GCP Secret Manager → .env → 環境変数 の順で解決。
"""

import os
import logging
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# プロジェクトルート
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# .env を読み込み (ルート直下の1ファイルのみ)
load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# GCP
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "company-data-collector")
USE_SECRET_MANAGER = os.getenv("USE_SECRET_MANAGER", "false").lower() == "true"


def _get_secret(secret_id: str) -> str:
    """GCP Secret Manager からシークレットを取得する。"""
    try:
        from google.cloud.secretmanager import SecretManagerServiceClient

        client = SecretManagerServiceClient()
        name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8")
    except Exception as e:
        logger.warning(f"Secret Manager から {secret_id} の取得に失敗: {e}")
        return ""


def _resolve_secret(env_key: str, secret_id: str, default: str = "") -> str:
    """環境変数 → Secret Manager の順で値を解決する。"""
    val = os.getenv(env_key, "")
    if val:
        return val
    if USE_SECRET_MANAGER:
        return _get_secret(secret_id)
    return default


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/company_db")

# ---------------------------------------------------------------------------
# External APIs
# ---------------------------------------------------------------------------
SCRAPINGDOG_API_KEY = _resolve_secret("SCRAPINGDOG_API_KEY", "scrapingdog-api-key")
GEMINI_API_KEY = _resolve_secret("GEMINI_API_KEY", "gemini-api-key")

# ---------------------------------------------------------------------------
# HTTP共通定数
# ---------------------------------------------------------------------------
REQUEST_INTERVAL = float(os.getenv("REQUEST_INTERVAL", "1.5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BACKOFF_BASE = int(os.getenv("RETRY_BACKOFF_BASE", "2"))
SCRAPINGDOG_SCRAPE_ENDPOINT = "https://api.scrapingdog.com/scrape"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = DATA_DIR / "output"
LOG_DIR = DATA_DIR / "logs"
DEBUG_DIR = DATA_DIR / "debug"
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
SCHEMAS_DIR = PROJECT_ROOT / "schemas"
HR_OUTPUT_DIR = OUTPUT_DIR / "hr_services"

# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------
BQ_DATASET = os.getenv("BQ_DATASET", "company_data")
BQ_TABLE = os.getenv("BQ_TABLE", "companies")
UPLOAD_TO_BIGQUERY = os.getenv("UPLOAD_TO_BIGQUERY", "false").lower() == "true"

# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------
CSV_ENCODING = "utf-8-sig"

# ---------------------------------------------------------------------------
# HR Services
# ---------------------------------------------------------------------------
HR_SERVICES_CSV_COLUMNS = ["企業名", "タイトル", "掲載日"]
