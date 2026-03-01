"""
環境変数・設定の一元管理
========================
全モジュールはここから設定を取得する。
.env → 環境変数 → このモジュール の順で解決。
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# プロジェクトルート
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# .env を読み込み (ルート直下の1ファイルのみ)
load_dotenv(PROJECT_ROOT / ".env")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/company_db")

# ---------------------------------------------------------------------------
# External APIs
# ---------------------------------------------------------------------------
SCRAPINGDOG_API_KEY = os.getenv("SCRAPINGDOG_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

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
SCHEMAS_DIR = PROJECT_ROOT / "schemas"

# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------
CSV_ENCODING = "utf-8-sig"
