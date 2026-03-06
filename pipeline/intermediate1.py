"""
中間データ1: rawdata → 項目選定 + クレンジング
===============================================
DATABASE_DESIGN.md Section 3 に準拠。

設計方針:
- rawdataテーブルごとに load 関数を定義
- 各関数は pandas DataFrame を返す（インメモリ）
- 項目選定は COLUMN_CONFIG で制御（将来の追加・除外に対応）
- クレンジングは pipeline/cleansing.py の型別関数を使用
- HR 14テーブルは企業名 → original_id を company_resolver で解決
- competitors は企業名カラムはそのまま（中間2で company_resolver により original_id 変換）
- call_logs は company_name → original_id を company_resolver で解決
- 未実装スクレイパーのテーブルが空 → 空 DataFrame を返す
"""

import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy.orm import Session

from db.models import (
    Company,
    RawdataCallLogs,
    RawdataCaritasu,
    RawdataCompanyInfo,
    RawdataCompetitors,
    RawdataEmails,
    RawdataEnHyouban,
    RawdataHrAcaric,
    RawdataHrBizreach,
    RawdataHrBizreachCampus,
    RawdataHrCareerTicket,
    RawdataHrCaritasu,
    RawdataHrEnAmbi,
    RawdataHrEnTenshoku,
    RawdataHrHitotore,
    RawdataHrKimisuka,
    RawdataHrLabbase,
    RawdataHrLevtechRookie,
    RawdataHrOfferbox,
    RawdataHrOnecareer,
    RawdataHrSupporters,
    RawdataHrTalentbook,
    RawdataHrTypeChuto,
    RawdataHrTypeShinsotsu,
    RawdataMinkabu,
    RawdataMynavi,
    RawdataPersons,
    RawdataPhones,
    RawdataPrtimes,
    RawdataRaCompany,
    RawdataRaKyujin,
    RawdataRikunabiCompany,
    RawdataRikunabiEmploy,
)
from pipeline.cleansing import (
    clean_address,
    clean_date,
    clean_email,
    clean_json,
    clean_long_text,
    clean_money,
    clean_money_million,
    clean_numeric_value,
    clean_people_count,
    clean_phone,
    clean_ratio,
    clean_salary,
    clean_short_text,
    clean_url,
    clean_zipcode,
    parse_childcare_leave,
    parse_female_ratio,
    parse_retention,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 項目選定設定（カラム名 → クレンジング関数）
# 将来、項目を除外したい場合はここから削除するだけで対応可能
# ---------------------------------------------------------------------------

COLUMNS_COMPANY_INFO = {
    "企業名": clean_short_text,
    "本社都道府県": clean_address,
    "代表者名": clean_short_text,
    "従業員数": clean_people_count,
    "企業規模": clean_short_text,
    "業種": clean_short_text,
    "業種詳細": clean_short_text,
    "代表電話番号": clean_phone,
}

COLUMNS_RA_COMPANY = {
    "本社所在地": clean_address,
    "設立": clean_date,
    "資本金": clean_money,
    "事業所": clean_long_text,
    "関連会社": clean_long_text,
    "株主": clean_long_text,
    "株主公開": clean_short_text,
    "決算情報": clean_long_text,
    "備考": clean_long_text,
    "公開求人数": clean_people_count,   # 「10件」→「10」
}

COLUMNS_RA_KYUJIN = {
    "想定年収": clean_short_text,       # 「690万円～1,235万円」
    "仕事の特徴": clean_long_text,      # カンマ区切りタグ
}

COLUMNS_MYNAVI = {
    "本社郵便番号": clean_zipcode,
    "採用実績校": clean_long_text,
    "採用実績学部学科": clean_long_text,
}

COLUMNS_RIKUNABI_COMPANY = {
    "業種": clean_short_text,
    "設立": clean_date,
    "代表者": clean_short_text,
    "資本金": clean_money,
    "従業員数": clean_people_count,
    "売上高": clean_money,
    "純利益": clean_money,
    "地域別売上高": clean_long_text,
    "連結子会社数": clean_people_count,  # 「15社」→「15」
    "連結研究開発費": clean_money,
    "事業内容": clean_long_text,
    "事業所": clean_long_text,
    "企業理念": clean_long_text,
    "関連会社": clean_long_text,
    "リクナビ限定情報": clean_long_text,
    "プレエントリー候補リスト登録人数": clean_people_count,  # 「100名」→「100人」
}

COLUMNS_RIKUNABI_EMPLOY = {
    "主な募集職種": clean_long_text,
    "主な勤務地": clean_long_text,
    "応募資格": clean_long_text,
    "積極採用対象": clean_long_text,
    "採用予定学科": clean_long_text,
    "採用人数": clean_people_count,
    "新卒採用人数": clean_long_text,    # データが複雑（年度別内訳等）でテキスト保持
    "初年度月収例": clean_long_text,      # 長文説明テキスト（月給の詳細説明）
    "選考フロー": clean_long_text,
    "給与": clean_long_text,              # 長文説明テキスト（職種別給与詳細）
    "手当": clean_long_text,
    "昇給": clean_long_text,
    "賞与": clean_long_text,
    "勤務時間": clean_short_text,
    "休日休暇": clean_long_text,
    "福利厚生": clean_long_text,
    "試用期間": clean_long_text,
    "研修制度": clean_long_text,
    "自己啓発支援": clean_long_text,
    "メンター制度": clean_long_text,
    "キャリアコンサルティング制度": clean_long_text,
    "社内検定制度": clean_long_text,
    "月平均残業時間": clean_numeric_value,      # 「10.0時間」→「10.0」
    "有給休暇取得日数": clean_numeric_value,    # 「16.1日」→「16.1」
    "海外赴任者数": clean_people_count,  # 「10名」→「10」
    "受動喫煙対策": clean_long_text,
    "平均勤続年数": clean_numeric_value,        # 「10.5年」→「10.5」
    "平均年齢": clean_numeric_value,            # 「35.5歳」→「35.5」
    "提出書類": clean_long_text,
    "採用活動開始時期": clean_short_text,
}

COLUMNS_CARITASU = {
    "企業名": clean_short_text,
    "上場区分": clean_short_text,
    "沿革": clean_long_text,
}

COLUMNS_PRTIMES = {
    "本社所在地": clean_address,
    "電話番号": clean_phone,
    "企業URL": clean_url,
    "プレスリリース": clean_json,
    "SNS_X": clean_url,
    "SNS_Facebook": clean_url,
    "SNS_YouTube": clean_url,
}

COLUMNS_EN_HYOUBAN = {
    "company_name": clean_short_text,
    "total_score": clean_ratio,
    "review_count": clean_short_text,
    "founded_year": clean_short_text,
    "employees": clean_people_count,
    "capital": clean_money,        # 「25699百万円」→ 円単位整数
    "listed_year": clean_short_text,
    "avg_salary": clean_salary,    # 「653万円」→ 円単位整数
    "avg_age": clean_numeric_value,    # 「29.8歳」→「29.8」
    "score_growth": clean_ratio,
    "score_advantage": clean_ratio,
    "score_meritocracy": clean_ratio,
    "score_culture": clean_ratio,
    "score_youth": clean_ratio,
    "score_contribution": clean_ratio,
    "score_innovation": clean_ratio,
    "score_leadership": clean_ratio,
    "reviews_text": clean_long_text,
}

COLUMNS_PHONES = {
    "source_url": clean_url,
    "拠点": clean_short_text,
    "事業部": clean_short_text,
    "ラベル": clean_short_text,
    "電話番号": clean_phone,
    "担当者名リレーションキー": clean_short_text,
}

COLUMNS_PERSONS = {
    "source_url": clean_url,
    "拠点": clean_short_text,
    "事業部": clean_short_text,
    "ラベル": clean_short_text,
    "担当者名": clean_short_text,
    "電話番号リレーションキー": clean_short_text,
}

COLUMNS_EMAILS = {
    "事業部": clean_short_text,
    "メールアドレス": clean_email,
}

COLUMNS_COMPETITORS = {
    "類似企業1": clean_short_text,
    "類似企業2": clean_short_text,
    "類似企業3": clean_short_text,
    "競合企業1": clean_short_text,
    "競合企業2": clean_short_text,
    "競合企業3": clean_short_text,
}

COLUMNS_HR = {
    "企業名_掲載名": clean_short_text,
    "掲載日": clean_date,
}

COLUMNS_CALL_LOGS = {
    "company_name": clean_short_text,
    "sales_rep_name": clean_short_text,
    "phone_number": clean_phone,
    "phone_status": clean_short_text,
    "product_name": clean_short_text,
    "phone_status_memo": clean_long_text,
    "discovered_number": clean_phone,
    "discovered_number_memo": clean_long_text,
    "call_result": clean_short_text,
    "spoke_with": clean_short_text,
    "discovered_person_chuto": clean_short_text,
    "discovered_person_shinsotsu": clean_short_text,
    "notes": clean_long_text,
}


# ---------------------------------------------------------------------------
# 共通ヘルパー
# ---------------------------------------------------------------------------


def _query_to_df(
    session: Session,
    model_class,
    column_config: dict,
    *,
    deduplicate_by_oid: bool = True,
    extra_columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    rawdata テーブルを読み込み → 項目選定 + クレンジング → DataFrame を返す。

    Args:
        session: SQLAlchemy セッション
        model_class: rawdata モデルクラス
        column_config: {カラム名: クレンジング関数} の dict
        deduplicate_by_oid: True の場合、original_id ごとに最新1件のみ残す
        extra_columns: original_id 以外に保持する追加カラム（id 等）
    """
    rows = session.query(model_class).all()
    if not rows:
        cols = ["original_id"] + (extra_columns or []) + list(column_config.keys()) + ["scraped_at"]
        return pd.DataFrame(columns=cols)

    records = []
    for row in rows:
        record = {"original_id": getattr(row, "original_id", None)}
        if extra_columns:
            for col in extra_columns:
                record[col] = str(getattr(row, col, "")) if getattr(row, col, None) else ""
        for col, cleaner in column_config.items():
            raw_val = getattr(row, col, None)
            record[col] = cleaner(raw_val)
        record["scraped_at"] = getattr(row, "scraped_at", None)
        records.append(record)

    df = pd.DataFrame(records)

    if deduplicate_by_oid and "original_id" in df.columns and not df.empty:
        df = df.sort_values("scraped_at", ascending=False, na_position="last")
        df = df.drop_duplicates(subset=["original_id"], keep="first")

    logger.info(f"  intermediate1_{model_class.__tablename__.replace('rawdata_', '')}: {len(df)}行")
    return df


# ---------------------------------------------------------------------------
# 各ソースの load 関数（中間1生成）
# ---------------------------------------------------------------------------


def load_company_master(session: Session) -> pd.DataFrame:
    """企業マスターシート（intermediate1_company_master）"""
    companies = session.query(Company).all()
    if not companies:
        return pd.DataFrame(columns=["original_id", "name", "name_normalized", "stock_code"])

    records = [
        {
            "original_id": str(c.id),
            "name": c.name or "",
            "name_normalized": c.name_normalized or "",
            "stock_code": c.stock_code or "",
        }
        for c in companies
    ]
    df = pd.DataFrame(records)
    logger.info(f"  intermediate1_company_master: {len(df)}行")
    return df


def load_company_info(session: Session) -> pd.DataFrame:
    """① 登録データ（intermediate1_company_info）"""
    return _query_to_df(session, RawdataCompanyInfo, COLUMNS_COMPANY_INFO)


def load_ra_company(session: Session) -> pd.DataFrame:
    """② RA企業詳細（intermediate1_ra_company）"""
    return _query_to_df(session, RawdataRaCompany, COLUMNS_RA_COMPANY)


def load_ra_kyujin(session: Session) -> pd.DataFrame:
    """14 RA求人（intermediate1_ra_kyujin）"""
    return _query_to_df(session, RawdataRaKyujin, COLUMNS_RA_KYUJIN)


def load_mynavi(session: Session) -> pd.DataFrame:
    """③ マイナビ（intermediate1_mynavi）"""
    return _query_to_df(session, RawdataMynavi, COLUMNS_MYNAVI)


def load_rikunabi_company(session: Session) -> pd.DataFrame:
    """④ リクナビ企業情報（intermediate1_rikunabi_company）"""
    return _query_to_df(session, RawdataRikunabiCompany, COLUMNS_RIKUNABI_COMPANY)


def load_rikunabi_employ(session: Session) -> pd.DataFrame:
    """
    15 リクナビ採用情報（intermediate1_rikunabi_employ）

    以下3フィールドは構造化テキストを複数カラムに展開する:
    - 女性管理職比率 → 女性役員比率, 女性管理職比率
    - 育児休業取得者数 → 育休取得者数_男性/女性, 育休対象者数_男性/女性, 育休取得率_男性/女性
    - 過去3年間採用実績 → 直近採用者数, 直近離職者数, 直近定着率
    """
    # 展開対象フィールド名
    EXPAND_FIELDS = ["女性管理職比率", "育児休業取得者数", "過去3年間採用実績"]

    rows = session.query(RawdataRikunabiEmploy).all()
    if not rows:
        expand_cols = [
            "女性役員比率", "女性管理職比率",
            "育休取得者数_男性", "育休取得者数_女性",
            "育休対象者数_男性", "育休対象者数_女性",
            "育休取得率_男性", "育休取得率_女性",
            "直近採用者数", "直近離職者数", "直近定着率",
        ]
        cols = ["original_id"] + list(COLUMNS_RIKUNABI_EMPLOY.keys()) + expand_cols + ["scraped_at"]
        return pd.DataFrame(columns=cols)

    records = []
    for row in rows:
        record = {"original_id": getattr(row, "original_id", None)}
        # 通常カラムのクレンジング
        for col, cleaner in COLUMNS_RIKUNABI_EMPLOY.items():
            record[col] = cleaner(getattr(row, col, None))
        # 構造化テキストの展開
        parsed = parse_female_ratio(getattr(row, "女性管理職比率", None))
        record["女性役員比率"] = parsed.get("女性役員比率", "")
        record["女性管理職比率"] = parsed.get("女性管理職比率", "")

        parsed = parse_childcare_leave(getattr(row, "育児休業取得者数", None))
        for k in ["育休取得者数_男性", "育休取得者数_女性",
                   "育休対象者数_男性", "育休対象者数_女性",
                   "育休取得率_男性", "育休取得率_女性"]:
            record[k] = parsed.get(k, "")

        parsed = parse_retention(getattr(row, "過去3年間採用実績", None))
        for k in ["直近採用者数", "直近離職者数", "直近定着率"]:
            record[k] = parsed.get(k, "")

        record["scraped_at"] = getattr(row, "scraped_at", None)
        records.append(record)

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("scraped_at", ascending=False, na_position="last")
        df = df.drop_duplicates(subset=["original_id"], keep="first")

    logger.info(f"  intermediate1_rikunabi_employ: {len(df)}行")
    return df


def load_caritasu(session: Session) -> pd.DataFrame:
    """⑤ キャリタス（intermediate1_caritasu）"""
    return _query_to_df(session, RawdataCaritasu, COLUMNS_CARITASU)


def load_prtimes(session: Session) -> pd.DataFrame:
    """⑥ PR TIMES（intermediate1_prtimes）"""
    return _query_to_df(session, RawdataPrtimes, COLUMNS_PRTIMES)


def load_minkabu(session: Session) -> pd.DataFrame:
    """16 みんかぶ（intermediate1_minkabu）— JSONB展開"""
    rows = session.query(RawdataMinkabu).all()
    if not rows:
        return pd.DataFrame(columns=["original_id", "scraped_at"])

    records = []
    for row in rows:
        record = {"original_id": row.original_id, "scraped_at": row.scraped_at}
        if row.financial_data and isinstance(row.financial_data, dict):
            for key, val in row.financial_data.items():
                # みんかぶの数値は百万円単位（売上高/営業利益/CF系）or 比率（ROE/自己資本率/成長率）
                if any(key.startswith(p) for p in ("ROE_", "自己資本率_", "売上成長率_")):
                    record[key] = clean_ratio(val)
                else:
                    # 売上高/営業利益/営業CF/フリーCF: 百万円単位の裸数値
                    record[key] = clean_money_million(val)
        records.append(record)

    df = pd.DataFrame(records)
    df = df.sort_values("scraped_at", ascending=False, na_position="last")
    df = df.drop_duplicates(subset=["original_id"], keep="first")
    logger.info(f"  intermediate1_minkabu: {len(df)}行")
    return df


def load_en_hyouban(session: Session) -> pd.DataFrame:
    """⑦ エン評判（intermediate1_en_hyouban）"""
    return _query_to_df(session, RawdataEnHyouban, COLUMNS_EN_HYOUBAN)


def load_phones(session: Session) -> pd.DataFrame:
    """⑧ 電話番号（intermediate1_phones）— 1社複数行OK"""
    return _query_to_df(
        session, RawdataPhones, COLUMNS_PHONES,
        deduplicate_by_oid=False, extra_columns=["id"],
    )


def load_persons(session: Session) -> pd.DataFrame:
    """⑨ 担当者（intermediate1_persons）— 1社複数行OK"""
    return _query_to_df(
        session, RawdataPersons, COLUMNS_PERSONS,
        deduplicate_by_oid=False, extra_columns=["id"],
    )


def load_emails(session: Session) -> pd.DataFrame:
    """⑩ メールアドレス（intermediate1_emails）— 1社複数行OK"""
    return _query_to_df(
        session, RawdataEmails, COLUMNS_EMAILS,
        deduplicate_by_oid=False, extra_columns=["id"],
    )


def load_competitors(session: Session) -> pd.DataFrame:
    """Ⅺ 競合・類似企業（intermediate1_competitors）"""
    return _query_to_df(session, RawdataCompetitors, COLUMNS_COMPETITORS)


def load_hr_services(session: Session) -> pd.DataFrame:
    """
    Ⅻ HRサービス（intermediate1_hr_{service} × 14 → 縦持ち統合）

    original_id が null の行は company_resolver で企業名 → original_id を解決する。
    """
    from db.company_resolver import resolve_company_ids

    HR_TABLES = {
        "Labbase": RawdataHrLabbase,
        "タレントブック": RawdataHrTalentbook,
        "type就活": RawdataHrTypeShinsotsu,
        "ワンキャリア": RawdataHrOnecareer,
        "レバテックルーキー": RawdataHrLevtechRookie,
        "ビズリーチキャンパス": RawdataHrBizreachCampus,
        "オファーボックス": RawdataHrOfferbox,
        "EN転職": RawdataHrEnTenshoku,
        "キミスカ": RawdataHrKimisuka,
        "キャリタス": RawdataHrCaritasu,
        "キャリアチケット": RawdataHrCareerTicket,
        "ビズリーチ": RawdataHrBizreach,
        "アンビ": RawdataHrEnAmbi,
        "type中途": RawdataHrTypeChuto,
        "ヒトトレ": RawdataHrHitotore,
        "アカリク": RawdataHrAcaric,
        "サポーターズ": RawdataHrSupporters,
    }

    from db.company_resolver import normalize_company_name as _norm

    all_records = []
    for service_name, model_class in HR_TABLES.items():
        rows = session.query(model_class).all()
        service_records = []
        for row in rows:
            record = {
                "original_id": row.original_id,
                "service_name": service_name,
                "source_url": clean_url(row.source_url),
            }
            for col, cleaner in COLUMNS_HR.items():
                record[col] = cleaner(getattr(row, col, None))
            record["scraped_at"] = row.scraped_at
            service_records.append(record)

        # サービスごとに企業名dedup（正規化した企業名で重複排除、最新を保持）
        if service_records:
            sdf = pd.DataFrame(service_records)
            sdf["_name_norm"] = sdf["企業名_掲載名"].apply(
                lambda x: _norm(x) if x else ""
            )
            before = len(sdf)
            sdf = sdf.sort_values("scraped_at", ascending=False, na_position="last")
            sdf = sdf.drop_duplicates(subset=["_name_norm"], keep="first")
            sdf = sdf.drop(columns=["_name_norm"])
            after = len(sdf)
            if before != after:
                logger.info(f"  {service_name}: dedup {before} → {after}件")
            all_records.extend(sdf.to_dict("records"))

    if not all_records:
        return pd.DataFrame(
            columns=["original_id", "service_name", "source_url",
                      "企業名_掲載名", "掲載日", "scraped_at"]
        )

    df = pd.DataFrame(all_records)

    # original_id が空の行を company_resolver で解決
    needs_resolve = df["original_id"].isna() | (df["original_id"] == "")
    if needs_resolve.any():
        names_to_resolve = df.loc[needs_resolve, "企業名_掲載名"].dropna().unique().tolist()
        if names_to_resolve:
            name_to_id = resolve_company_ids(names_to_resolve)
            df.loc[needs_resolve, "original_id"] = df.loc[needs_resolve, "企業名_掲載名"].map(name_to_id)

    # original_id が解決できなかった行は除外
    resolved = df["original_id"].notna() & (df["original_id"] != "")
    dropped = (~resolved).sum()
    if dropped > 0:
        logger.warning(f"  HR: {dropped}行の original_id が未解決（除外）")
    df = df[resolved].copy()

    logger.info(f"  intermediate1_hr_services（統合）: {len(df)}行")
    return df


def load_call_logs(session: Session) -> pd.DataFrame:
    """
    13 架電ログ（intermediate1_call_logs）

    original_id が null の行は company_resolver で company_name → original_id を解決する。
    """
    from db.company_resolver import resolve_company_ids

    rows = session.query(RawdataCallLogs).all()
    if not rows:
        return pd.DataFrame(
            columns=["original_id"] + list(COLUMNS_CALL_LOGS.keys()) + ["called_at"]
        )

    records = []
    for row in rows:
        record = {"original_id": row.original_id, "called_at": row.called_at}
        for col, cleaner in COLUMNS_CALL_LOGS.items():
            record[col] = cleaner(getattr(row, col, None))
        records.append(record)

    df = pd.DataFrame(records)

    # original_id が空の行を company_resolver で解決
    needs_resolve = df["original_id"].isna() | (df["original_id"] == "")
    if needs_resolve.any():
        names_to_resolve = df.loc[needs_resolve, "company_name"].dropna().unique().tolist()
        if names_to_resolve:
            name_to_id = resolve_company_ids(names_to_resolve)
            df.loc[needs_resolve, "original_id"] = df.loc[needs_resolve, "company_name"].map(name_to_id)

    logger.info(f"  intermediate1_call_logs: {len(df)}行")
    return df


# ---------------------------------------------------------------------------
# 一括ロード
# ---------------------------------------------------------------------------


def load_all(session: Session) -> dict[str, pd.DataFrame]:
    """全中間データ1を一括ロードして dict で返す。"""
    logger.info("=" * 50)
    logger.info("中間データ1 生成開始")
    logger.info("=" * 50)

    result = {
        "company_master": load_company_master(session),
        "company_info": load_company_info(session),
        "ra_company": load_ra_company(session),
        "ra_kyujin": load_ra_kyujin(session),
        "mynavi": load_mynavi(session),
        "rikunabi_company": load_rikunabi_company(session),
        "rikunabi_employ": load_rikunabi_employ(session),
        "caritasu": load_caritasu(session),
        "prtimes": load_prtimes(session),
        "minkabu": load_minkabu(session),
        "en_hyouban": load_en_hyouban(session),
        "phones": load_phones(session),
        "persons": load_persons(session),
        "emails": load_emails(session),
        "competitors": load_competitors(session),
        "hr_services": load_hr_services(session),
        "call_logs": load_call_logs(session),
    }

    total = sum(len(df) for df in result.values())
    logger.info(f"中間データ1 生成完了: 合計 {total} 行")
    return result
