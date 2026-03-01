"""
DB接続管理
==========
ローカル開発: PostgreSQL (localhost)
本番: Cloud SQL (接続文字列を環境変数で切り替え)
"""

import os
from contextlib import contextmanager

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from db.models import Base

load_dotenv()

# 環境変数 DATABASE_URL が未設定ならローカルPostgreSQLにフォールバック
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://localhost:5432/company_db",
)

engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)


def init_db() -> None:
    """全テーブルを作成する (開発用。本番は Alembic を使う)"""
    Base.metadata.create_all(engine)


def drop_db() -> None:
    """全テーブルを削除する (開発用)"""
    Base.metadata.drop_all(engine)


@contextmanager
def get_session() -> Session:
    """セッションのコンテキストマネージャ。自動 commit/rollback。"""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
