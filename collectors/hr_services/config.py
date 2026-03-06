"""サービス定義・共通定数・パス設定"""

import sys
import importlib.util
from pathlib import Path

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# 共通設定をファイルパスで直接ロード（ローカル config.py との名前衝突を回避）
_settings_path = Path(__file__).resolve().parent.parent.parent / "config" / "settings.py"
_spec = importlib.util.spec_from_file_location("_project_settings", _settings_path)
_settings = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_settings)

OUTPUT_DIR = _settings.HR_OUTPUT_DIR
LOG_DIR = _settings.LOG_DIR
REQUEST_INTERVAL = _settings.REQUEST_INTERVAL
MAX_RETRIES = _settings.MAX_RETRIES
RETRY_BACKOFF_BASE = _settings.RETRY_BACKOFF_BASE
USER_AGENT = _settings.USER_AGENT
CSV_ENCODING = _settings.CSV_ENCODING
SCRAPINGDOG_SCRAPE_ENDPOINT = _settings.SCRAPINGDOG_SCRAPE_ENDPOINT
CSV_COLUMNS = _settings.HR_SERVICES_CSV_COLUMNS

# ---------------------------------------------------------------------------
# HR Services 固有定数
# ---------------------------------------------------------------------------

# サービス定義
SERVICE_REGISTRY = {
    # --- 新卒 ---
    "labbase": {
        "name": "Labbase",
        "category": "新卒",
        "base_url": "https://compass.labbase.jp/search?mode=default",
        "method": "scrapingdog",
        "output_csv": "labbase.csv",
    },
    "talentbook": {
        "name": "タレントブック",
        "category": "新卒",
        "base_url": "https://www.talent-book.jp/companies",
        "method": "requests",
        "output_csv": "talentbook.csv",
    },
    "type_shinsotsu": {
        "name": "type就活",
        "category": "新卒",
        "base_url": "https://typeshukatsu.jp/company/",
        "method": "requests",
        "output_csv": "type_shinsotsu.csv",
    },
    "onecareer": {
        "name": "ワンキャリア",
        "category": "新卒",
        "base_url": "https://www.onecareer.jp/companies",
        "method": "requests",
        "output_csv": "onecareer.csv",
    },
    "levtech_rookie": {
        "name": "レバテックルーキー",
        "category": "新卒",
        "base_url": "https://rookie.levtech.jp/company/",
        "method": "requests",
        "output_csv": "levtech_rookie.csv",
    },
    "bizreach_campus": {
        "name": "ビズリーチキャンパス",
        "category": "新卒",
        "base_url": "https://br-campus.jp/events",
        "method": "requests",
        "output_csv": "bizreach_campus.csv",
    },
    "offerbox": {
        "name": "オファーボックス",
        "category": "新卒",
        "base_url": "https://app.offerbox.jp/v2/scompany",
        "method": "playwright",
        "output_csv": "offerbox.csv",
    },
    "en_tenshoku": {
        "name": "EN転職",
        "category": "新卒",
        "base_url": "https://employment.en-japan.com/a/kanto/s_setsubikanri-unyu/?caroute=1101&PK=F4DF97",
        "method": "requests",
        "output_csv": "en_tenshoku.csv",
    },
    "caritasu": {
        "name": "キャリタス",
        "category": "新卒",
        "base_url": "https://job.career-tasu.jp/condition-search/result/?corpOtherCheckCd=04-%E3%82%AD%E3%83%A3%E3%83%AA%E3%82%BF%E3%82%B9%E9%99%90%E5%AE%9A%E6%83%85%E5%A0%B1%E3%81%82%E3%82%8A",
        "method": "requests",
        "output_csv": "caritasu.csv",
    },
    "career_ticket": {
        "name": "キャリアチケット",
        "category": "新卒",
        "base_urls": [f"https://careerticket.jp/industry/{i}/" for i in range(1, 11)],
        "method": "requests",
        "output_csv": "career_ticket.csv",
    },
    # --- 中途 ---
    "bizreach": {
        "name": "ビズリーチ",
        "category": "中途",
        "base_url": "https://www.bizreach.jp/job/",
        "method": "playwright",
        "output_csv": "bizreach.csv",
    },
    "en_ambi": {
        "name": "アンビ",
        "category": "中途",
        "base_url": "https://en-ambi.com/search/?jobmerit=350&krt=top",
        "method": "requests",
        "output_csv": "en_ambi.csv",
    },
    "hitotore": {
        "name": "ヒトトレ",
        "category": "中途",
        "method": "csv_upload",
        "output_csv": "hitotore.csv",
    },
    "acaric": {
        "name": "アカリク",
        "category": "新卒",
        "method": "csv_upload",
        "output_csv": "acaric.csv",
    },
    "supporters": {
        "name": "サポーターズ",
        "category": "新卒",
        "method": "csv_upload",
        "output_csv": "supporters.csv",
    },
    "type_chuto": {
        "name": "type中途",
        "category": "中途",
        "base_urls": [
            "https://type.jp/job/search/?pathway=4&job3IdList=3&job3IdList=155&job3IdList=13&job3IdList=129&job3IdList=130&job3IdList=6&job3IdList=133&job3IdList=156&job3IdList=15&job3IdList=10&job3IdList=18&job3IdList=20&job3IdList=21&job3IdList=22&job3IdList=29&job3IdList=157&job3IdList=132&job3IdList=28&job3IdList=101&job3IdList=158&job3IdList=159&job3IdList=160&job3IdList=161&job3IdList=25&job3IdList=32&job3IdList=30",
            "https://type.jp/job/search/?pathway=4&job3IdList=1&job3IdList=7&job3IdList=162&job3IdList=163&job3IdList=164&job3IdList=16&job3IdList=134&job3IdList=23&job3IdList=24&job3IdList=26",
            "https://type.jp/job/search/?pathway=4",
            "https://type.jp/job/search/?pathway=4&job3IdList=122&job3IdList=123&job3IdList=124&job3IdList=168&job3IdList=169&job3IdList=170&job3IdList=150&job3IdList=152&job3IdList=125&job3IdList=151&job3IdList=153&job3IdList=127&job3IdList=126&job3IdList=154&job3IdList=128",
            "https://type.jp/job/search/?pathway=4&job3IdList=100&job3IdList=136&job3IdList=102&job3IdList=103&job3IdList=104&job3IdList=137",
            "https://type.jp/job/search/?pathway=4&job3IdList=70&job3IdList=71&job3IdList=75&job3IdList=72&job3IdList=45&job3IdList=47",
            "https://type.jp/job/search/?pathway=4&job3IdList=34&job3IdList=35&job3IdList=36&job3IdList=37&job3IdList=40&job3IdList=41&job3IdList=42&job3IdList=43&job3IdList=48&job3IdList=52&job3IdList=50&job3IdList=44&job3IdList=53&job3IdList=54&job3IdList=55&job3IdList=56&job3IdList=57&job3IdList=58&job3IdList=59&job3IdList=60&job3IdList=62&job3IdList=63",
            "https://type.jp/job/search/?pathway=4&job3IdList=64&job3IdList=65&job3IdList=66&job3IdList=67&job3IdList=68&job3IdList=69",
            "https://type.jp/job/search/?pathway=4&job3IdList=76&job3IdList=77&job3IdList=78&job3IdList=79&job3IdList=80&job3IdList=115&job3IdList=116&job3IdList=117&job3IdList=118&job3IdList=135&job3IdList=81&job3IdList=73&job3IdList=120&job3IdList=74&job3IdList=82",
        ],
        "method": "requests",
        "output_csv": "type_chuto.csv",
    },
}

# サービス名一覧（マスターDB列順）
SERVICE_NAMES = [cfg["name"] for cfg in SERVICE_REGISTRY.values()]
