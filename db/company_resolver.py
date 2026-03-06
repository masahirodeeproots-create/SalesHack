"""
企業名 → PostgreSQL companies.id (UUID) 解決ユーティリティ
==========================================================
全コレクターが共通で使う企業ID解決関数。

解決ロジック:
  1. name_normalized との完全一致検索
     （company_info / contacts 等、登録名がそのまま使われる場合）
  2. 未マッチ分のみ: 入力名・DB名の両方を正規化した上での突合
     （hr_services のスクレイプ名など表記ゆれがある場合）

DB接続できない場合は全て None を返す（BQ upload はスキップしない）。
"""

import re
import unicodedata

# 除去する法人格パターン（包括的リスト）
_LEGAL_ENTITIES = [
    r"株式会社",
    r"\(株\)",
    r"（株）",
    r"㈱",
    r"有限会社",
    r"\(有\)",
    r"（有）",
    r"合同会社",
    r"\(同\)",
    r"（同）",
    r"合名会社",
    r"合資会社",
    r"一般社団法人",
    r"一般財団法人",
    r"公益社団法人",
    r"公益財団法人",
    r"医療法人",
    r"社会福祉法人",
    r"学校法人",
    r"宗教法人",
    r"NPO法人",
    r"特定非営利活動法人",
    r"独立行政法人",
    r"社団法人",
    r"財団法人",
]
_LEGAL_PATTERN = re.compile("|".join(_LEGAL_ENTITIES))

# 先頭・末尾の括弧注釈パターン（例: (東京), [上場], 【東証プライム上場WDBグループ】）
_LEADING_ANNOTATION = re.compile(r"^[（(\[【][^）)\]】]*[）)\]】]\s*")
_TRAILING_ANNOTATION = re.compile(r"\s*[（(\[【][^）)\]】]*[）)\]】]\s*$")


def normalize_company_name(name: str) -> str:
    """
    企業名を正規化する（企業マスターとの突合用）。

    1. Unicode NFKC正規化（全角英数字→半角、合字分解等）
    2. 法人格除去（株式会社, (株), ㈱, 有限会社 等）
    3. 空白正規化（全角スペース含む）
    4. 引用符・括弧の除去
    5. 末尾注釈の除去（例: (東京), [上場], 【東証プライム上場WDBグループ】）
    """
    if not name:
        return ""

    name = name.strip()

    # 1. NFKC正規化（全角→半角: Ａ→A, ０→0, ＆→& 等）
    name = unicodedata.normalize("NFKC", name)

    # 2. 法人格除去
    name = _LEGAL_PATTERN.sub("", name)

    # 3. 空白正規化
    name = re.sub(r"[\s\u3000]+", " ", name).strip()

    # 4. 先頭・末尾の括弧注釈除去（例: 【東証プライム上場】, (東京), [上場]）
    name = _LEADING_ANNOTATION.sub("", name)
    name = _TRAILING_ANNOTATION.sub("", name).strip()

    # 5. 残った引用符・括弧除去
    name = name.strip("\"'「」『』【】")

    return name


def resolve_company_ids(names: list[str]) -> dict[str, str | None]:
    """
    企業名リスト → {企業名: company_id (UUID文字列) or None} のマップを返す。

    解決手順:
      1. name_normalized と完全一致検索
      2. 未マッチ分: 入力名・DB名の両方を normalize して突合
         （hr_services のスクレイプ名など表記ゆれがある場合）

    DB接続できない場合は全て None を返す（BQ upload はスキップしない）。
    """
    if not names:
        return {}

    try:
        from db.connection import get_session
        from db.models import Company
    except ImportError:
        return {name: None for name in names}

    result: dict[str, str | None] = {name: None for name in names}

    try:
        with get_session() as session:
            all_companies = session.query(Company.id, Company.name_normalized).all()

        # Step 1: name_normalized と完全一致
        exact_map: dict[str, str] = {
            c.name_normalized: str(c.id) for c in all_companies
        }
        unmatched: list[str] = []
        for name in names:
            if name in exact_map:
                result[name] = exact_map[name]
            else:
                unmatched.append(name)

        # Step 2: 未マッチ分を正規化して再検索
        if unmatched:
            normalized_map: dict[str, str] = {}
            for c in all_companies:
                key = normalize_company_name(c.name_normalized)
                if key and key not in normalized_map:  # 重複は先勝ち
                    normalized_map[key] = str(c.id)
            for name in unmatched:
                key = normalize_company_name(name)
                if key and key in normalized_map:
                    result[name] = normalized_map[key]

    except Exception:
        pass  # DB接続失敗時は全て None のまま返す（BQ upload は続行）

    return result
