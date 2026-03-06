"""
BigQuery 書き込みヘルパー
========================
設計書の中間データ2に対応する BQ テーブルへアップロードする。

ポリシー:
- 企業マスターのみ WRITE_TRUNCATE（常に最新全社分で上書き）
- その他全テーブルは WRITE_APPEND（行を追加・累積保存）
- 入力は pandas DataFrame
"""

import logging

import pandas as pd
from google.cloud import bigquery

from config.settings import GCP_PROJECT_ID, BQ_DATASET

logger = logging.getLogger(__name__)


def _client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT_ID)


def _table_id(table_name: str) -> str:
    return f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"


def _upload(
    df: pd.DataFrame,
    table_name: str,
    write_disposition: bigquery.WriteDisposition,
) -> None:
    """DataFrame を BQ テーブルにアップロードする共通処理。"""
    if df.empty:
        logger.warning(f"BigQuery ({table_name}): アップロード対象データなし")
        return

    client = _client()
    tid = _table_id(table_name)

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, tid, job_config=job_config)
    job.result()

    logger.info(f"BigQuery ({table_name}) 完了: {len(df)}行 → {tid}")
    print(f"BigQuery アップロード完了: {len(df)}行 → {tid}")


# ---------------------------------------------------------------------------
# 企業マスター（WRITE_TRUNCATE）
# ---------------------------------------------------------------------------


def upload_company_master(df: pd.DataFrame) -> None:
    """
    企業マスターを BQ にアップロード。
    常に最新の全社分で上書き（WRITE_TRUNCATE）。

    期待カラム: original_id, name, name_normalized, stock_code
    """
    _upload(df, "company_master", bigquery.WriteDisposition.WRITE_TRUNCATE)


# ---------------------------------------------------------------------------
# 企業情報DB（WRITE_APPEND）
# ---------------------------------------------------------------------------


def upload_company_info(df: pd.DataFrame) -> None:
    """
    企業情報DB（中間データ2の突合結果）を BQ に追記。

    期待カラム: original_id, 本社所在地, 本社郵便番号, 設立, 代表者, 資本金,
               従業員数, 業種, 上場区分, 企業URL, 電話番号, 事業所, 関連会社,
               沿革, 売上高, 純利益, 事業内容, 企業理念, 公開求人数,
               採用実績校, 採用実績学部学科, みんかぶ財務各項目,
               エン評判各スコア, HRサービス掲載状況, ...
               + scraped_at（実行日時）
    """
    _upload(df, "company_info", bigquery.WriteDisposition.WRITE_APPEND)


# ---------------------------------------------------------------------------
# 連絡先（WRITE_APPEND）
# ---------------------------------------------------------------------------


def upload_phones(df: pd.DataFrame) -> None:
    """
    電話番号DB を BQ に追記。

    期待カラム: original_id, source_url, 拠点, 事業部, ラベル, 電話番号,
               担当者名リレーションキー, scraped_at
    """
    _upload(df, "phones", bigquery.WriteDisposition.WRITE_APPEND)


def upload_persons(df: pd.DataFrame) -> None:
    """
    担当者DB を BQ に追記。

    期待カラム: original_id, source_url, 拠点, 事業部, ラベル, 担当者名,
               電話番号リレーションキー, scraped_at
    """
    _upload(df, "persons", bigquery.WriteDisposition.WRITE_APPEND)


def upload_emails(df: pd.DataFrame) -> None:
    """
    メールアドレスDB を BQ に追記。

    期待カラム: original_id, 事業部, メールアドレス, scraped_at
    """
    _upload(df, "emails", bigquery.WriteDisposition.WRITE_APPEND)


def upload_phone_person_relation(df: pd.DataFrame) -> None:
    """
    連絡先×担当者リレーションDB を BQ に追記。

    期待カラム: phone_id, person_id, source, confirmed_at, call_log_id
    """
    _upload(df, "phone_person_relation", bigquery.WriteDisposition.WRITE_APPEND)


# ---------------------------------------------------------------------------
# 競合・類似企業（WRITE_APPEND）
# ---------------------------------------------------------------------------


def upload_competitors(df: pd.DataFrame) -> None:
    """
    競合・類似企業DB を BQ に追記。

    期待カラム: original_id, 類似企業1〜3, 競合企業1〜3, scraped_at
    """
    _upload(df, "competitors", bigquery.WriteDisposition.WRITE_APPEND)


# ---------------------------------------------------------------------------
# HRサービス（WRITE_APPEND）
# ---------------------------------------------------------------------------


def upload_hr_services(df: pd.DataFrame) -> None:
    """
    競合HRサービスDB（14サービス縦持ち）を BQ に追記。

    期待カラム: original_id, service_name, 企業名_掲載名, 掲載日, scraped_at
    """
    _upload(df, "hr_services", bigquery.WriteDisposition.WRITE_APPEND)


# ---------------------------------------------------------------------------
# 架電ログ（WRITE_APPEND）
# ---------------------------------------------------------------------------


def upload_call_logs(df: pd.DataFrame) -> None:
    """
    架電ログDB を BQ に追記。

    期待カラム: original_id, company_name, sales_rep_name, called_at,
               phone_number, phone_status, product_name, phone_status_memo,
               discovered_number, discovered_number_memo, call_result,
               spoke_with, discovered_person_chuto, discovered_person_shinsotsu,
               notes
    """
    _upload(df, "call_logs", bigquery.WriteDisposition.WRITE_APPEND)


# ---------------------------------------------------------------------------
# ログ情報（WRITE_APPEND）
# ---------------------------------------------------------------------------


def upload_logs(df: pd.DataFrame) -> None:
    """
    実行ログ・充填率・エラー・API使用量等を BQ に追記。

    期待カラム: run_id, run_at, table_name, field_name, filled_count,
               total_count, fill_rate, error_type, api_calls, ... 等
    """
    _upload(df, "logs", bigquery.WriteDisposition.WRITE_APPEND)
