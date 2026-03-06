"""
中間データ2: 中間1 → 突合・マージ → BQアップロード直前
=====================================================
DATABASE_DESIGN.md Section 4 に準拠。

- intermediate2_company_info: source priority に従い突合
- intermediate2_phones / persons / emails: 中間1をそのまま使用
- intermediate2_phone_person_relation: phones × persons のリレーション生成
- intermediate2_competitors: 中間1 + 企業名→original_id 変換
- intermediate2_hr_services: 中間1（14テーブル縦統合済み）をそのまま使用
- intermediate2_call_logs: 中間1をそのまま使用
"""

import logging
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------


def _coalesce(*series: pd.Series) -> pd.Series:
    """
    複数の Series から最初の非null・非空文字列値を採用する coalesce。
    source priority の実装に使用。
    """
    result = series[0].copy()
    for s in series[1:]:
        mask = result.isna() | (result == "")
        result = result.where(~mask, s)
    return result


def _safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    """DataFrame にカラムが存在しない場合は空の Series を返す。"""
    if col in df.columns:
        return df[col].fillna("")
    return pd.Series("", index=df.index)


def _left_merge(base: pd.DataFrame, other: pd.DataFrame, suffix: str) -> pd.DataFrame:
    """original_id で左結合。空 DataFrame の場合はスキップ。"""
    if other.empty:
        return base
    # scraped_at の重複を避けるため除外
    other_cols = [c for c in other.columns if c != "scraped_at"]
    return base.merge(
        other[other_cols], on="original_id", how="left", suffixes=("", f"_{suffix}")
    )


# ---------------------------------------------------------------------------
# 4-1. 企業情報データベース（intermediate2_company_info）
# ---------------------------------------------------------------------------


def build_company_info(i1: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    intermediate1 の各 DataFrame を original_id で突合し、
    source priority に従って企業情報DBを生成する。

    Section 4-1 のフィールドとsource priorityに厳密に準拠。
    """
    master = i1["company_master"]
    if master.empty:
        logger.warning("企業マスターが空です")
        return pd.DataFrame()

    # ベース: 企業マスターの original_id
    base = master[["original_id"]].copy()

    # 各ソースを左結合
    # ①→②→⑭→③→④→⑮→⑤→⑥→⑦ の順（suffix 衝突を制御）
    base = _left_merge(base, i1["company_info"], "ci")       # ① CSVインポート
    base = _left_merge(base, i1["ra_company"], "ra")         # ② RA企業詳細
    base = _left_merge(base, i1["ra_kyujin"], "rak")         # ⑭ RA求人
    base = _left_merge(base, i1["mynavi"], "my")             # ③ マイナビ
    base = _left_merge(base, i1["rikunabi_company"], "rk")   # ④ リクナビ企業情報
    base = _left_merge(base, i1["rikunabi_employ"], "rke")   # ⑮ リクナビ採用情報
    base = _left_merge(base, i1["caritasu"], "ct")           # ⑤ キャリタス
    base = _left_merge(base, i1["prtimes"], "pt")            # ⑥ PR TIMES
    base = _left_merge(base, i1["en_hyouban"], "eh")         # ⑦ エン評判

    # みんかぶ: 動的カラムのためそのまま結合
    if not i1["minkabu"].empty:
        mk_cols = [c for c in i1["minkabu"].columns if c != "scraped_at"]
        base = base.merge(i1["minkabu"][mk_cols], on="original_id", how="left")

    # ------------------------------------------------------------------
    # source priority に従った coalesce（Section 4-1 厳守）
    # ------------------------------------------------------------------
    result = pd.DataFrame({"original_id": base["original_id"]})

    # === 基本情報 ===
    # 本社所在地: ② RA > ⑥ PR TIMES
    result["本社所在地"] = _coalesce(
        _safe_col(base, "本社所在地"),       # RA (suffix なし or _ra)
        _safe_col(base, "本社所在地_pt"),    # PR TIMES
    )
    # 本社都道府県: ① CSVインポート
    result["本社都道府県"] = _safe_col(base, "本社都道府県")
    # 本社郵便番号: ③ マイナビ
    result["本社郵便番号"] = _safe_col(base, "本社郵便番号")
    # 設立: ② RA > ④ リクナビ
    result["設立"] = _coalesce(
        _safe_col(base, "設立"),             # RA
        _safe_col(base, "設立_rk"),          # リクナビ
    )
    # 代表者: ④ リクナビ > ① CSVインポート（代表者名）
    result["代表者"] = _coalesce(
        _safe_col(base, "代表者"),           # リクナビ
        _safe_col(base, "代表者名"),         # CSVインポート
    )
    # 資本金: ② RA > ④ リクナビ
    result["資本金"] = _coalesce(
        _safe_col(base, "資本金"),           # RA
        _safe_col(base, "資本金_rk"),        # リクナビ
    )
    # 従業員数: ④ リクナビ > ① CSVインポート
    result["従業員数"] = _coalesce(
        _safe_col(base, "従業員数_rk"),      # リクナビ
        _safe_col(base, "従業員数"),         # CSVインポート
    )
    # 企業規模: ① CSVインポート
    result["企業規模"] = _safe_col(base, "企業規模")
    # 業種: ④ リクナビ（リクナビ独自カテゴリ）
    result["業種"] = _safe_col(base, "業種_rk")
    # 業種_csv: ① CSVインポート（日本標準産業分類形式。分類体系が異なるため別カラム）
    result["業種_csv"] = _safe_col(base, "業種")
    # 業種詳細: ① CSVインポート
    result["業種詳細"] = _safe_col(base, "業種詳細")
    # 上場区分: ⑤ キャリタス
    result["上場区分"] = _safe_col(base, "上場区分")
    # 企業URL: ⑥ PR TIMES
    result["企業URL"] = _safe_col(base, "企業URL")
    # 電話番号: ⑥ PR TIMES
    result["電話番号"] = _safe_col(base, "電話番号")
    # 代表電話番号: ① CSVインポート
    result["代表電話番号"] = _safe_col(base, "代表電話番号")
    # 事業所: ② RA > ④ リクナビ
    result["事業所"] = _coalesce(
        _safe_col(base, "事業所"),           # RA
        _safe_col(base, "事業所_rk"),        # リクナビ
    )
    # 関連会社: ② RA > ④ リクナビ
    result["関連会社"] = _coalesce(
        _safe_col(base, "関連会社"),         # RA
        _safe_col(base, "関連会社_rk"),      # リクナビ
    )
    # 沿革: ⑤ キャリタス
    result["沿革"] = _safe_col(base, "沿革")

    # === 財務情報 ===
    result["売上高"] = _safe_col(base, "売上高")
    result["純利益"] = _safe_col(base, "純利益")
    result["地域別売上高"] = _safe_col(base, "地域別売上高")
    result["連結子会社数"] = _safe_col(base, "連結子会社数")
    result["連結研究開発費"] = _safe_col(base, "連結研究開発費")
    result["株主"] = _safe_col(base, "株主")
    result["株主公開"] = _safe_col(base, "株主公開")
    result["決算情報"] = _safe_col(base, "決算情報")

    # === 企業詳細 ===
    result["事業内容"] = _safe_col(base, "事業内容")
    result["企業理念"] = _safe_col(base, "企業理念")
    result["リクナビ限定情報"] = _safe_col(base, "リクナビ限定情報")
    result["プレエントリー候補リスト登録人数"] = _safe_col(base, "プレエントリー候補リスト登録人数")
    result["公開求人数"] = _safe_col(base, "公開求人数")
    result["備考"] = _safe_col(base, "備考")

    # === PR TIMES ===
    result["プレスリリース"] = _safe_col(base, "プレスリリース")
    result["SNS_X"] = _safe_col(base, "SNS_X")
    result["SNS_Facebook"] = _safe_col(base, "SNS_Facebook")
    result["SNS_YouTube"] = _safe_col(base, "SNS_YouTube")

    # === 求人情報（⑭ RA求人 /viewjob/）===
    result["想定年収"] = _safe_col(base, "想定年収")
    result["仕事の特徴"] = _safe_col(base, "仕事の特徴")

    # === 採用情報（15 リクナビ採用 + ③ マイナビ）===
    for col in ["主な募集職種", "主な勤務地", "応募資格", "積極採用対象", "採用予定学科",
                "採用人数", "新卒採用人数"]:
        result[col] = _safe_col(base, col)
    # 採用実績校 / 採用実績学部学科: ③ マイナビ
    result["採用実績校"] = _safe_col(base, "採用実績校")
    result["採用実績学部学科"] = _safe_col(base, "採用実績学部学科")
    for col in ["初年度月収例", "選考フロー", "給与", "手当", "昇給", "賞与",
                "勤務時間", "休日休暇", "福利厚生", "試用期間", "研修制度",
                "自己啓発支援", "メンター制度", "キャリアコンサルティング制度",
                "社内検定制度", "月平均残業時間", "有給休暇取得日数",
                "海外赴任者数",
                "受動喫煙対策", "平均勤続年数", "平均年齢",
                "提出書類", "採用活動開始時期"]:
        result[col] = _safe_col(base, col)
    # 構造化テキスト展開カラム（女性管理職比率・育児休業・採用実績）
    for col in ["女性役員比率", "女性管理職比率",
                "育休取得者数_男性", "育休取得者数_女性",
                "育休対象者数_男性", "育休対象者数_女性",
                "育休取得率_男性", "育休取得率_女性",
                "直近採用者数", "直近離職者数", "直近定着率"]:
        result[col] = _safe_col(base, col)

    # === みんかぶ財務（動的カラム）===
    for col in base.columns:
        if col.startswith(("売上高_", "営業利益_", "自己資本率_", "ROE_",
                           "営業CF_", "フリーCF_", "売上成長率_")):
            result[col] = _safe_col(base, col)

    # === エン評判 ===
    for col in ["total_score", "review_count",
                "founded_year", "employees", "capital", "listed_year",
                "avg_salary", "avg_age",
                "score_growth", "score_advantage", "score_meritocracy",
                "score_culture", "score_youth", "score_contribution",
                "score_innovation", "score_leadership", "reviews_text"]:
        result[col] = _safe_col(base, col)

    # === メールアドレス（intermediate1_emails から集約）===
    if "emails" in i1 and not i1["emails"].empty:
        emails_df = i1["emails"][["original_id", "メールアドレス"]].copy()
        emails_df = emails_df[emails_df["メールアドレス"].notna() & (emails_df["メールアドレス"] != "")]
        if not emails_df.empty:
            emails_agg = (
                emails_df.groupby("original_id")["メールアドレス"]
                .apply(lambda x: ", ".join(sorted(x.unique())))
                .reset_index()
                .rename(columns={"メールアドレス": "企業メールアドレス"})
            )
            result = result.merge(emails_agg, on="original_id", how="left")
        else:
            result["企業メールアドレス"] = pd.NA
    else:
        result["企業メールアドレス"] = pd.NA

    # scraped_at = 実行日時
    result["scraped_at"] = datetime.now(timezone.utc)

    # 空文字を NaN に変換（BQ では null が適切）
    result = result.replace("", pd.NA)

    logger.info(f"  intermediate2_company_info: {len(result)}行 × {len(result.columns)}列")
    return result


# ---------------------------------------------------------------------------
# 4-2〜4-4. 電話番号 / 担当者 / メール（中間1をそのまま使用）
# ---------------------------------------------------------------------------


def build_phones(i1: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """intermediate2_phones: 中間1をそのまま使用"""
    df = i1["phones"].copy()
    # id カラムを除外（BQ には不要）
    if "id" in df.columns:
        df = df.drop(columns=["id"])
    logger.info(f"  intermediate2_phones: {len(df)}行")
    return df


def build_persons(i1: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """intermediate2_persons: 中間1をそのまま使用"""
    df = i1["persons"].copy()
    if "id" in df.columns:
        df = df.drop(columns=["id"])
    logger.info(f"  intermediate2_persons: {len(df)}行")
    return df


def build_emails(i1: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """intermediate2_emails: 中間1をそのまま使用"""
    df = i1["emails"].copy()
    if "id" in df.columns:
        df = df.drop(columns=["id"])
    logger.info(f"  intermediate2_emails: {len(df)}行")
    return df


# ---------------------------------------------------------------------------
# 4-5. 連絡先×担当者リレーションDB
# ---------------------------------------------------------------------------


def build_phone_person_relation(i1: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    intermediate2_phone_person_relation:
    phones の 担当者名リレーションキー → persons の id
    persons の 電話番号リレーションキー → phones の id
    2方向を concat → 重複除去
    """
    phones = i1["phones"]
    persons = i1["persons"]
    relations = []

    # phones → persons 方向
    if not phones.empty and "担当者名リレーションキー" in phones.columns:
        phone_rels = phones[
            phones["担当者名リレーションキー"].notna()
            & (phones["担当者名リレーションキー"] != "")
        ][["id", "担当者名リレーションキー"]].copy()
        phone_rels = phone_rels.rename(
            columns={"id": "phone_id", "担当者名リレーションキー": "person_id"}
        )
        phone_rels["source"] = "scraping"
        relations.append(phone_rels)

    # persons → phones 方向
    if not persons.empty and "電話番号リレーションキー" in persons.columns:
        person_rels = persons[
            persons["電話番号リレーションキー"].notna()
            & (persons["電話番号リレーションキー"] != "")
        ][["電話番号リレーションキー", "id"]].copy()
        person_rels = person_rels.rename(
            columns={"電話番号リレーションキー": "phone_id", "id": "person_id"}
        )
        person_rels["source"] = "scraping"
        relations.append(person_rels)

    if not relations:
        return pd.DataFrame(
            columns=["phone_id", "person_id", "source", "confirmed_at", "call_log_id"]
        )

    df = pd.concat(relations, ignore_index=True)
    df = df.drop_duplicates(subset=["phone_id", "person_id"])
    df["confirmed_at"] = pd.NaT
    df["call_log_id"] = pd.NA

    logger.info(f"  intermediate2_phone_person_relation: {len(df)}行")
    return df


# ---------------------------------------------------------------------------
# 4-6. 類似企業DB（中間1 + 企業名→original_id 変換）
# ---------------------------------------------------------------------------


def build_competitors(i1: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    intermediate2_competitors: 中間1 + 企業名→original_id 変換。
    類似企業1~3 / 競合企業1~3 の各企業名を企業マスターと突合し、
    _id サフィックス付きカラムに original_id を格納する。
    """
    from db.company_resolver import resolve_company_ids

    df = i1["competitors"].copy()
    if df.empty:
        for col in ["類似企業1", "類似企業2", "類似企業3",
                     "競合企業1", "競合企業2", "競合企業3"]:
            df[f"{col}_id"] = pd.Series(dtype="object")
        logger.info("  intermediate2_competitors: 0行")
        return df

    name_cols = ["類似企業1", "類似企業2", "類似企業3",
                 "競合企業1", "競合企業2", "競合企業3"]

    # 全企業名を集めて一括解決
    all_names: set[str] = set()
    for col in name_cols:
        if col in df.columns:
            all_names.update(
                n for n in df[col].dropna().unique() if n
            )

    name_to_id: dict[str, str | None] = {}
    if all_names:
        name_to_id = resolve_company_ids(list(all_names))
        matched = sum(1 for v in name_to_id.values() if v)
        logger.info(f"  competitors 企業名→ID: {matched}/{len(all_names)} マッチ")

    # _id カラムを追加
    for col in name_cols:
        id_col = f"{col}_id"
        if col in df.columns:
            df[id_col] = df[col].map(
                lambda x: name_to_id.get(x) if pd.notna(x) and x else pd.NA
            )
        else:
            df[id_col] = pd.NA

    logger.info(f"  intermediate2_competitors: {len(df)}行")
    return df


# ---------------------------------------------------------------------------
# 4-7. 競合HRサービスDB（14テーブル縦持ち統合済み）
# ---------------------------------------------------------------------------


def build_hr_services(i1: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    intermediate2_hr_services:
    中間1で既に 14テーブル → 縦持ち統合 + original_id 解決済み。

    出力カラム: original_id, service_name, 企業名_掲載名, 掲載日, scraped_at
    """
    df = i1["hr_services"].copy()
    # source_url は中間2では不要（BQテーブル定義に含まれない）
    if "source_url" in df.columns:
        df = df.drop(columns=["source_url"])
    logger.info(f"  intermediate2_hr_services: {len(df)}行")
    return df


# ---------------------------------------------------------------------------
# 4-8. 架電データ（中間1をそのまま使用）
# ---------------------------------------------------------------------------


def build_call_logs(i1: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """intermediate2_call_logs: 中間1をそのまま使用"""
    df = i1["call_logs"].copy()
    logger.info(f"  intermediate2_call_logs: {len(df)}行")
    return df


# ---------------------------------------------------------------------------
# 一括ビルド
# ---------------------------------------------------------------------------


def build_all(i1: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """全中間データ2を一括ビルドして dict で返す。"""
    logger.info("=" * 50)
    logger.info("中間データ2 生成開始")
    logger.info("=" * 50)

    result = {
        "company_master": i1["company_master"],  # そのまま
        "company_info": build_company_info(i1),
        "phones": build_phones(i1),
        "persons": build_persons(i1),
        "emails": build_emails(i1),
        "phone_person_relation": build_phone_person_relation(i1),
        "competitors": build_competitors(i1),
        "hr_services": build_hr_services(i1),
        "call_logs": build_call_logs(i1),
    }

    logger.info("中間データ2 生成完了")
    return result
