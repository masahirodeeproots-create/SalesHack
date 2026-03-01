"""
BigQuery 書き込みヘルパー
========================
collect_company_data のパイプライン結果を BigQuery に投入する。
テーブルが存在しない場合は自動作成する。
"""

import logging
from datetime import datetime, timezone

from google.cloud import bigquery

from config.settings import GCP_PROJECT_ID, BQ_DATASET, BQ_TABLE

logger = logging.getLogger(__name__)


def _get_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT_ID)


def _build_table_id() -> str:
    return f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"


def upload_company_data(
    final_data: dict[str, dict[str, str]],
    sorted_canonicals: list[str],
    company_order: list[str],
) -> None:
    """
    企業データを BigQuery にアップロードする。
    既存データは全件置換（WRITE_TRUNCATE）。

    Args:
        final_data:        {企業名: {canonical: value}} の辞書
        sorted_canonicals: 出力するフィールド名の順序付きリスト
        company_order:     企業の出力順
    """
    client = _get_client()
    table_id = _build_table_id()

    # スキーマ定義（全フィールドを STRING として格納）
    schema = [
        bigquery.SchemaField("企業名", "STRING", mode="REQUIRED"),
    ]
    for canonical in sorted_canonicals:
        schema.append(bigquery.SchemaField(canonical, "STRING", mode="NULLABLE"))
    schema.append(
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED")
    )

    # テーブルが存在しなければ作成
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    logger.info(f"BigQuery テーブル準備完了: {table_id}")

    # 行データを構築
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for company in company_order:
        row = {"企業名": company, "updated_at": now}
        for canonical in sorted_canonicals:
            row[canonical] = final_data.get(company, {}).get(canonical, "")
        rows.append(row)

    if not rows:
        logger.warning("BigQuery: アップロード対象のデータがありません")
        return

    # アップロード（全件置換）
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()  # 完了を待つ

    logger.info(f"BigQuery アップロード完了: {len(rows)}行")
    print(f"BigQuery アップロード完了: {len(rows)}行 → {table_id}")


_HR_SERVICE_USAGES_SCHEMA = [
    bigquery.SchemaField("企業名", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("サービス名", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("タイトル", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("掲載日", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("カテゴリ", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
]


def truncate_hr_service_usages() -> None:
    """
    hr_service_usages テーブルを空にする。
    run_all.py の開始時に呼び出し、14サービスの WRITE_APPEND が重複しないようにする。
    テーブルが存在しない場合は何もしない。
    """
    client = _get_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.hr_service_usages"
    try:
        client.get_table(table_id)
    except Exception:
        return  # テーブルがなければスキップ

    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    logger.info(f"BigQuery (hr_service_usages) truncate 完了: {table_id}")


def upload_hr_service_usages(rows: list[dict]) -> None:
    """
    HR サービス利用状況データを BigQuery に追記する（WRITE_APPEND）。
    run_all.py 開始時に truncate_hr_service_usages() を呼び出してからこの関数を使う。

    Args:
        rows: [{"企業名": str, "サービス名": str, "タイトル": str, "掲載日": str, "カテゴリ": str}, ...]
    """
    if not rows:
        logger.warning("BigQuery (hr_service_usages): アップロード対象のデータがありません")
        return

    client = _get_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.hr_service_usages"

    table = bigquery.Table(table_id, schema=_HR_SERVICE_USAGES_SCHEMA)
    client.create_table(table, exists_ok=True)

    now = datetime.now(timezone.utc).isoformat()
    bq_rows = [{**row, "updated_at": now} for row in rows]

    job_config = bigquery.LoadJobConfig(
        schema=_HR_SERVICE_USAGES_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json(bq_rows, table_id, job_config=job_config)
    job.result()

    logger.info(f"BigQuery (hr_service_usages) アップロード完了: {len(bq_rows)}行")
    print(f"BigQuery アップロード完了: {len(bq_rows)}行 → {table_id}")


def upload_contacts(rows: list[dict]) -> None:
    """
    連絡先情報を BigQuery にアップロードする。
    既存データは全件置換（WRITE_TRUNCATE）。

    Args:
        rows: [{"企業名": str, "電話番号": str, "ラベル": str, "status": str, "source": str}, ...]
    """
    if not rows:
        logger.warning("BigQuery (contacts): アップロード対象のデータがありません")
        return

    client = _get_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.contacts"

    schema = [
        bigquery.SchemaField("企業名", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("電話番号", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ラベル", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    now = datetime.now(timezone.utc).isoformat()
    bq_rows = [{**row, "updated_at": now} for row in rows]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(bq_rows, table_id, job_config=job_config)
    job.result()

    logger.info(f"BigQuery (contacts) アップロード完了: {len(bq_rows)}行")
    print(f"BigQuery アップロード完了: {len(bq_rows)}行 → {table_id}")


def upload_call_logs(rows: list[dict]) -> None:
    """
    架電ログデータを BigQuery にアップロードする。
    既存データは全件追記（WRITE_APPEND）。

    Args:
        rows: CallRow の辞書リスト（csv_importer.CallRow.__dict__ 相当）
    """
    if not rows:
        logger.warning("BigQuery (call_logs): アップロード対象のデータがありません")
        return

    client = _get_client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.call_logs"

    schema = [
        bigquery.SchemaField("company_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("sales_rep_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("called_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("phone_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("phone_status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("phone_status_memo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("discovered_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("discovered_number_memo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("call_result", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("spoke_with", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("discovered_person_chuto", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("discovered_person_shinsotsu", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("notes", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    now = datetime.now(timezone.utc).isoformat()
    bq_rows = []
    for row in rows:
        bq_row = {
            "company_name": row.get("company_name", ""),
            "sales_rep_name": row.get("sales_rep_name"),
            "called_at": row["called_at"].isoformat() if row.get("called_at") else None,
            "phone_number": row.get("phone_number"),
            "phone_status": row.get("phone_status"),
            "product_name": row.get("product_name"),
            "phone_status_memo": row.get("phone_status_memo"),
            "discovered_number": row.get("discovered_number"),
            "discovered_number_memo": row.get("discovered_number_memo"),
            "call_result": row.get("call_result"),
            "spoke_with": row.get("spoke_with"),
            "discovered_person_chuto": row.get("discovered_person_chuto"),
            "discovered_person_shinsotsu": row.get("discovered_person_shinsotsu"),
            "notes": row.get("notes"),
            "updated_at": now,
        }
        bq_rows.append(bq_row)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json(bq_rows, table_id, job_config=job_config)
    job.result()

    logger.info(f"BigQuery (call_logs) アップロード完了: {len(bq_rows)}行")
    print(f"BigQuery アップロード完了: {len(bq_rows)}行 → {table_id}")
