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

_LEGAL_SUFFIXES = [
    "株式会社", "有限会社", "合同会社", "合資会社", "社団法人", "財団法人",
    "（株）", "(株)", "（有）", "(有)", "（合）", "(合)",
]

_ZEN_TO_HAN = str.maketrans(
    "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
    "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
    "　",
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    " ",
)


def normalize_company_name(name: str) -> str:
    """
    企業名を正規化する（hr_services のスクレイプ名との突合用）。
    - 全角英数・スペースを半角に変換
    - 法人格（株式会社、有限会社等）を除去
    - 前後の空白を除去
    """
    name = name.translate(_ZEN_TO_HAN).strip()
    for suffix in _LEGAL_SUFFIXES:
        name = name.replace(suffix, "")
    return name.strip()


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
